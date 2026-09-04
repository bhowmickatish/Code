package ratelimit

import (
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	uberlimit "go.uber.org/ratelimit"

	"github.com/atish/go-zookeeper/internal/model"
)

type Limiter struct {
	mu    sync.RWMutex
	rules []compiledRule
}

type compiledRule struct {
	name       string
	pathPrefix string
	limit      int
	window     time.Duration
	key        model.KeyStrategy
}

func NewLimiter(doc model.RulesDocument) (*Limiter, error) {
	compiled := make([]compiledRule, 0, len(doc.Rules))
	for _, rule := range doc.Rules {
		window, err := rule.WindowDuration()
		if err != nil {
			return nil, err
		}

		compiled = append(compiled, compiledRule{
			name:       rule.Name,
			pathPrefix: rule.PathPrefix,
			limit:      rule.Limit,
			window:     window,
			key:        rule.Key,
		})
	}

	sort.Slice(compiled, func(i, j int) bool {
		return len(compiled[i].pathPrefix) > len(compiled[j].pathPrefix)
	})

	return &Limiter{rules: compiled}, nil
}

func (l *Limiter) Update(doc model.RulesDocument) error {
	next, err := NewLimiter(doc)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.rules = next.rules
	l.mu.Unlock()
	return nil
}

// Allow applies the leaky-bucket limiter for the request. It blocks until a slot
// is available (go.uber.org/ratelimit only exposes blocking Take).
func (l *Limiter) Allow(r *http.Request) (allowed bool, ruleName string, retryAfterSec int) {
	l.mu.RLock()
	rules := l.rules
	l.mu.RUnlock()

	rule, ok := matchRule(rules, r.URL.Path)
	if !ok {
		return true, "", 0
	}

	key := limiterKey(rule.key, r)
	limiter := l.limiterFor(rule.name, key, rule.limit, rule.window)
	limiter.Take()
	return true, rule.name, 0
}

func matchRule(rules []compiledRule, path string) (compiledRule, bool) {
	for _, rule := range rules {
		if strings.HasPrefix(path, rule.pathPrefix) {
			return rule, true
		}
	}
	return compiledRule{}, false
}

func limiterKey(strategy model.KeyStrategy, r *http.Request) string {
	switch strategy {
	case model.KeyStrategyGlobal:
		return "global"
	default:
		return clientIP(r)
	}
}

func clientIP(r *http.Request) string {
	if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
		parts := strings.Split(forwarded, ",")
		return strings.TrimSpace(parts[0])
	}
	host := r.RemoteAddr
	if idx := strings.LastIndex(host, ":"); idx != -1 {
		return host[:idx]
	}
	return host
}

var limiterCache sync.Map

type limiterEntry struct {
	mu      sync.Mutex
	limiter uberlimit.Limiter
	limit   int
	window  time.Duration
}

func (l *Limiter) limiterFor(ruleName, key string, limit int, window time.Duration) uberlimit.Limiter {
	cacheKey := ruleName + ":" + key

	entry, _ := limiterCache.LoadOrStore(cacheKey, &limiterEntry{})
	e := entry.(*limiterEntry)

	e.mu.Lock()
	defer e.mu.Unlock()

	if e.limiter == nil || e.limit != limit || e.window != window {
		e.limiter = uberlimit.New(limit, uberlimit.Per(window), uberlimit.WithoutSlack)
		e.limit = limit
		e.window = window
	}
	return e.limiter
}
