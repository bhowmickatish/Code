package ratelimit

import (
	"testing"

	"github.com/atish/go-zookeeper/internal/model"
)

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
