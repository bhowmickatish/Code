package ratelimit

import (
	"errors"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	uberlimit "go.uber.org/ratelimit"

	"github.com/atish/go-zookeeper/internal/model"
)

var (
	instance *Limiter
	initMu   sync.Mutex
)

// ErrAlreadyInitialized is returned when Init is called more than once.
var ErrAlreadyInitialized = errors.New("ratelimit: already initialized")

type Limiter struct {
	mu    sync.RWMutex
	rules []compiledRule
	cache sync.Map // ruleName:clientKey -> *limiterEntry
}

type compiledRule struct {
	name       string
	pathPrefix string
	limit      int
	window     time.Duration
	key        model.KeyStrategy
}

// Init creates the application-wide limiter. It must be called exactly once.
func Init(doc model.RulesDocument) (*Limiter, error) {
	initMu.Lock()
	defer initMu.Unlock()

	if instance != nil {
		return nil, ErrAlreadyInitialized
	}

	rules, err := compileRules(doc)
	if err != nil {
		return nil, err
	}

	instance = &Limiter{rules: rules}
	return instance, nil
}

// Instance returns the application-wide limiter created by Init.
// All HTTP handlers must use this instance (typically via handler.RateLimitMiddleware).
func Instance() *Limiter {
	if instance == nil {
		panic("ratelimit: Init must be called before Instance")
	}
	return instance
}

func compileRules(doc model.RulesDocument) ([]compiledRule, error) {
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

	return compiled, nil
}

func (l *Limiter) Update(doc model.RulesDocument) error {
	rules, err := compileRules(doc)
	if err != nil {
		return err
	}

	l.mu.Lock()
	l.rules = rules
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

type limiterEntry struct {
	mu      sync.Mutex
	limiter uberlimit.Limiter
	limit   int
	window  time.Duration
}

func (l *Limiter) limiterFor(ruleName, key string, limit int, window time.Duration) uberlimit.Limiter {
	cacheKey := ruleName + ":" + key

	entry, _ := l.cache.LoadOrStore(cacheKey, &limiterEntry{})
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
