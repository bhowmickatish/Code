package ratelimit

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/atish/go-zookeeper/internal/model"
)

func TestLimiterKeyUser(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	req.Header.Set("X-User-ID", "alice")

	key := limiterKey(model.KeyStrategyUser, req, false, "X-User-ID")
	if key != "user:alice" {
		t.Fatalf("expected user:alice, got %q", key)
	}
}

func TestLimiterKeyUserFallsBackToIP(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	req.RemoteAddr = "203.0.113.5:1234"

	key := limiterKey(model.KeyStrategyUser, req, false, "X-User-ID")
	if key != "ip:203.0.113.5" {
		t.Fatalf("expected ip fallback, got %q", key)
	}
}

func TestMatchRuleLongestPrefixWins(t *testing.T) {
	doc := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "api-default", PathPrefix: "/api/", Limit: 100, Window: "1m", Key: model.KeyStrategyIP},
			{Name: "users", PathPrefix: "/api/users", Limit: 10, Window: "1s", Key: model.KeyStrategyIP},
		},
	}
	rules, err := compileRules(doc)
	if err != nil {
		t.Fatalf("compile rules: %v", err)
	}

	rule, ok := matchRule(rules, "/api/users")
	if !ok || rule.name != "users" {
		t.Fatalf("expected users rule, got %q ok=%v", rule.name, ok)
	}

	rule, ok = matchRule(rules, "/api/orders")
	if !ok || rule.name != "api-default" {
		t.Fatalf("expected api-default rule, got %q ok=%v", rule.name, ok)
	}
}

func TestAllowFailsClosedWhenNoRuleMatches(t *testing.T) {
	l := &Limiter{cache: newEntryCache(defaultCacheMax)}
	doc := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "users", PathPrefix: "/api/users", Limit: 10, Window: "1s", Key: model.KeyStrategyIP},
		},
	}
	if err := l.Update(doc); err != nil {
		t.Fatalf("update rules: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/api/orders", nil)
	if _, allowed := l.Allow(req); allowed {
		t.Fatal("expected unmatched path to fail closed")
	}
}

func TestAllowFailsClosedWithNoRulesLoaded(t *testing.T) {
	l := &Limiter{cache: newEntryCache(defaultCacheMax)}

	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	if _, allowed := l.Allow(req); allowed {
		t.Fatal("expected request to fail closed when no rules are loaded")
	}
}

func TestUpdateRejectsInvalidRulesDocument(t *testing.T) {
	l := &Limiter{cache: newEntryCache(defaultCacheMax)}
	valid := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "users", PathPrefix: "/api/users", Limit: 10, Window: "1s", Key: model.KeyStrategyIP},
		},
	}
	if err := l.Update(valid); err != nil {
		t.Fatalf("update valid rules: %v", err)
	}

	invalid := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "bad", PathPrefix: "/api/users", Limit: 0, Window: "1s", Key: model.KeyStrategyIP},
		},
	}
	if err := l.Update(invalid); err == nil {
		t.Fatal("expected invalid limit to fail validation")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	ruleName, allowed := l.Allow(req)
	if !allowed || ruleName != "users" {
		t.Fatalf("expected previous rules to remain active, got rule=%q allowed=%v", ruleName, allowed)
	}
}

func TestUpdateKeepsRulesOnInvalidDocument(t *testing.T) {
	l := &Limiter{cache: newEntryCache(defaultCacheMax)}
	valid := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "users", PathPrefix: "/api/users", Limit: 10, Window: "1s", Key: model.KeyStrategyIP},
		},
	}
	if err := l.Update(valid); err != nil {
		t.Fatalf("update valid rules: %v", err)
	}

	invalid := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "bad", PathPrefix: "/api/users", Limit: 10, Window: "not-a-duration", Key: model.KeyStrategyIP},
		},
	}
	if err := l.Update(invalid); err == nil {
		t.Fatal("expected invalid rules to fail")
	}

	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	ruleName, allowed := l.Allow(req)
	if !allowed || ruleName != "users" {
		t.Fatalf("expected previous rules to remain active, got rule=%q allowed=%v", ruleName, allowed)
	}
}

func TestCompileRulesTieBreaksByName(t *testing.T) {
	doc := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "b-rule", PathPrefix: "/api/", Limit: 1, Window: "1s", Key: model.KeyStrategyIP},
			{Name: "a-rule", PathPrefix: "/api/", Limit: 1, Window: "1s", Key: model.KeyStrategyIP},
		},
	}

	rules, err := compileRules(doc)
	if err != nil {
		t.Fatalf("compile rules: %v", err)
	}
	if len(rules) != 2 {
		t.Fatalf("expected 2 rules, got %d", len(rules))
	}
	if rules[0].name != "a-rule" {
		t.Fatalf("expected a-rule first on tie-break, got %s", rules[0].name)
	}
}
