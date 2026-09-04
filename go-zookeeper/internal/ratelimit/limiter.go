package ratelimit

import (
	"errors"
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/atish/go-zookeeper/internal/model"
)

var (
	instance *Limiter
	initMu   sync.Mutex
)

// ErrAlreadyInitialized is returned when Init is called more than once.
var ErrAlreadyInitialized = errors.New("ratelimit: already initialized")

type Limiter struct {
	mu           sync.RWMutex
	rules        []compiledRule
	cache        *entryCache
	maxWait      time.Duration
	trustedProxy bool
}

type compiledRule struct {
	name       string
	pathPrefix string
	limit      int
	window     time.Duration
	key        model.KeyStrategy
}

// Init creates the application-wide limiter. It must be called exactly once.
func Init(doc model.RulesDocument, maxCacheEntries int, maxWait time.Duration, trustedProxy bool) (*Limiter, error) {
	initMu.Lock()
	defer initMu.Unlock()

	if instance != nil {
		return nil, ErrAlreadyInitialized
	}

	rules, err := compileRules(doc)
	if err != nil {
		return nil, err
	}

	instance = &Limiter{
		rules:        rules,
		cache:        newEntryCache(maxCacheEntries),
		maxWait:      maxWait,
		trustedProxy: trustedProxy,
	}
	return instance, nil
}

// Instance returns the application-wide limiter created by Init.
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
		if len(compiled[i].pathPrefix) != len(compiled[j].pathPrefix) {
			return len(compiled[i].pathPrefix) > len(compiled[j].pathPrefix)
		}
		return compiled[i].name < compiled[j].name
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

// Allow applies leaky-bucket rate limiting. Returns 429 when required wait exceeds maxWait.
func (l *Limiter) Allow(r *http.Request) (ruleName string, allowed bool, retryAfter time.Duration) {
	l.mu.RLock()
	rules := l.rules
	maxWait := l.maxWait
	trustedProxy := l.trustedProxy
	l.mu.RUnlock()

	rule, ok := matchRule(rules, r.URL.Path)
	if !ok {
		return "", true, 0
	}

	key := limiterKey(rule.key, r, trustedProxy)
	entry := l.cache.get(rule.name+":"+key, rule.limit, rule.window)

	ok, wait := entry.allow(maxWait)
	if !ok {
		return rule.name, false, wait
	}
	return rule.name, true, 0
}

func matchRule(rules []compiledRule, path string) (compiledRule, bool) {
	for _, rule := range rules {
		if strings.HasPrefix(path, rule.pathPrefix) {
			return rule, true
		}
	}
	return compiledRule{}, false
}

func limiterKey(strategy model.KeyStrategy, r *http.Request, trustedProxy bool) string {
	switch strategy {
	case model.KeyStrategyGlobal:
		return "global"
	default:
		return clientIP(r, trustedProxy)
	}
}

func clientIP(r *http.Request, trustedProxy bool) string {
	if trustedProxy {
		if forwarded := r.Header.Get("X-Forwarded-For"); forwarded != "" {
			parts := strings.Split(forwarded, ",")
			return strings.TrimSpace(parts[0])
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
