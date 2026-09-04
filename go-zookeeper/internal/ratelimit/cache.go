package ratelimit

import (
	"container/list"
	"sync"
	"time"
)

const defaultCacheMax = 10_000

// entryCache is an in-process LRU cache of per-client leaky buckets.
// It is a placeholder for a shared Redis-backed limiter in a multi-instance deployment.
type entryCache struct {
	mu      sync.Mutex
	entries map[string]*list.Element
	order   *list.List
	max     int
}

type lruNode struct {
	key   string
	entry *limiterEntry
}

func newEntryCache(max int) *entryCache {
	if max <= 0 {
		max = defaultCacheMax
	}
	return &entryCache{
		entries: make(map[string]*list.Element),
		order:   list.New(),
		max:     max,
	}
}

func (c *entryCache) get(key string, limit int, window time.Duration) *limiterEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if el, ok := c.entries[key]; ok {
		node := el.Value.(*lruNode)
		node.entry.configure(limit, window)
		c.order.MoveToFront(el)
		return node.entry
	}

	if len(c.entries) >= c.max {
		back := c.order.Back()
		evict := back.Value.(*lruNode)
		delete(c.entries, evict.key)
		c.order.Remove(back)
	}

	entry := &limiterEntry{}
	entry.configure(limit, window)

	node := &lruNode{key: key, entry: entry}
	el := c.order.PushFront(node)
	c.entries[key] = el
	return entry
}

// limiterEntry uses a single atomic schedule (WithoutSlack leaky-bucket semantics
// matching go.uber.org/ratelimit) so peek and commit share one state machine.
type limiterEntry struct {
	mu         sync.Mutex
	nextIssue  int64
	perRequest int64
	limit      int
	window     time.Duration
}

func (e *limiterEntry) configure(limit int, window time.Duration) {
	perRequest := int64(window / time.Duration(limit))
	if perRequest == e.perRequest && e.limit == limit && e.window == window {
		return
	}
	e.perRequest = perRequest
	e.limit = limit
	e.window = window
	e.nextIssue = 0
}

func (e *limiterEntry) allow(maxWait time.Duration) (bool, time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	now := time.Now().UnixNano()
	wait := leakyWait(e.nextIssue, now, e.perRequest)
	if wait > int64(maxWait) {
		return false, time.Duration(wait)
	}

	if wait > 0 {
		time.Sleep(time.Duration(wait))
		now = time.Now().UnixNano()
	}

	e.nextIssue = scheduleNextIssue(e.nextIssue, now, e.perRequest)
	return true, 0
}

// leakyWait and scheduleNextIssue mirror go.uber.org/ratelimit WithoutSlack scheduling.
func leakyWait(nextIssue, now, perRequest int64) int64 {
	return scheduleNextIssue(nextIssue, now, perRequest) - now
}

func scheduleNextIssue(nextIssue, now, perRequest int64) int64 {
	switch {
	case nextIssue == 0 || now-nextIssue > perRequest:
		return now
	default:
		return nextIssue + perRequest
	}
}
