package ratelimit

import (
	"net"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/atish/go-zookeeper/internal/config"
	"github.com/atish/go-zookeeper/internal/model"
)

var (
	instance *Limiter
	initOnce sync.Once
)

type Limiter struct {
	mu    sync.RWMutex
	rules []compiledRule
	cache *entryCache
	cfg   config.Config
}

type compiledRule struct {
	name       string
	pathPrefix string
	limit      int
	window     time.Duration
	key        model.KeyStrategy
}

// Instance returns the application-wide limiter singleton.
func Instance() *Limiter {
	initOnce.Do(func() {
		cfg, err := config.Instance()
		if err != nil {
			cfg = config.Config{
				RateLimitCacheMax:   defaultCacheMax,
				RateLimitUserHeader: "X-User-ID",
			}
		}

		instance = &Limiter{
			cache: newEntryCache(cfg.RateLimitCacheMax),
			cfg:   cfg,
		}
	})

	return instance
}

// Update hot-reloads rate limit rules on the limiter.
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

func compileRules(doc model.RulesDocument) ([]compiledRule, error) {
	if err := model.ValidateRulesDocument(doc); err != nil {
		return nil, err
	}

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

// Allow applies leaky-bucket rate limiting via go.uber.org/ratelimit.
// Over-limit requests block until a slot is available (Take semantics).
// Returns the matched rule name and whether the request is allowed.
// Unmatched paths fail closed (allowed == false).
func (l *Limiter) Allow(r *http.Request) (ruleName string, allowed bool) {
	l.mu.RLock()
	rules := l.rules
	l.mu.RUnlock()

	rule, ok := matchRule(rules, r.URL.Path)
	if !ok {
		return "", false
	}

	key := limiterKey(rule.key, r, l.cfg.TrustedProxy, l.cfg.RateLimitUserHeader)
	entry := l.cache.get(rule.name+":"+key, rule.limit, rule.window)
	entry.take()
	return rule.name, true
}

func matchRule(rules []compiledRule, path string) (compiledRule, bool) {
	for _, rule := range rules {
		if strings.HasPrefix(path, rule.pathPrefix) {
			return rule, true
		}
	}
	return compiledRule{}, false
}

func limiterKey(strategy model.KeyStrategy, r *http.Request, trustedProxy bool, userHeader string) string {
	switch strategy {
	case model.KeyStrategyGlobal:
		return "global"
	case model.KeyStrategyUser:
		if userID := clientUser(r, userHeader); userID != "" {
			return "user:" + userID
		}
		return "ip:" + clientIP(r, trustedProxy)
	default:
		return "ip:" + clientIP(r, trustedProxy)
	}
}

func clientUser(r *http.Request, header string) string {
	return strings.TrimSpace(r.Header.Get(header))
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
