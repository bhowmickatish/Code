package ratelimit

import (
	"container/list"
	"sync"
	"time"

	uberlimit "go.uber.org/ratelimit"
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

type limiterEntry struct {
	mu     sync.Mutex
	rl     uberlimit.Limiter
	limit  int
	window time.Duration
}

func (e *limiterEntry) configure(limit int, window time.Duration) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if limit == e.limit && window == e.window && e.rl != nil {
		return
	}

	e.limit = limit
	e.window = window
	e.rl = uberlimit.New(limit, uberlimit.Per(window), uberlimit.WithoutSlack)
}

func (e *limiterEntry) take() {
	e.mu.Lock()
	rl := e.rl
	e.mu.Unlock()

	rl.Take()
}
