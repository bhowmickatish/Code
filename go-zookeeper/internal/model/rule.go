package model

import (
	"encoding/json"
	"fmt"
	"time"
)

type KeyStrategy string

const (
	KeyStrategyIP     KeyStrategy = "ip"
	KeyStrategyGlobal KeyStrategy = "global"
)

type RulesDocument struct {
	Version int         `json:"version"`
	Rules   []RateLimit `json:"rules"`
}

type RateLimit struct {
	Name       string      `json:"name"`
	PathPrefix string      `json:"path_prefix"`
	Limit      int         `json:"limit"`
	Window     string      `json:"window"`
	Key        KeyStrategy `json:"key"`
}

func (r RateLimit) WindowDuration() (time.Duration, error) {
	if r.Window == "" {
		return 0, fmt.Errorf("rule %q: window is required", r.Name)
	}
	d, err := time.ParseDuration(r.Window)
	if err != nil {
		return 0, fmt.Errorf("rule %q: invalid window %q", r.Name, r.Window)
	}
	if d <= 0 {
		return 0, fmt.Errorf("rule %q: window must be positive", r.Name)
	}
	return d, nil
}

func ParseRulesDocument(data []byte) (RulesDocument, error) {
	var doc RulesDocument
	if err := json.Unmarshal(data, &doc); err != nil {
		return RulesDocument{}, fmt.Errorf("parse rules json: %w", err)
	}
	if doc.Version == 0 {
		return RulesDocument{}, fmt.Errorf("rules version is required")
	}
	if len(doc.Rules) == 0 {
		return RulesDocument{}, fmt.Errorf("rules list is empty")
	}
	for i, rule := range doc.Rules {
		if err := validateRule(rule); err != nil {
			return RulesDocument{}, fmt.Errorf("rules[%d]: %w", i, err)
		}
	}
	return doc, nil
}

func validateRule(rule RateLimit) error {
	if rule.Name == "" {
		return fmt.Errorf("name is required")
	}
	if rule.PathPrefix == "" {
		return fmt.Errorf("path_prefix is required")
	}
	if rule.Limit <= 0 {
		return fmt.Errorf("limit must be positive")
	}
	if _, err := rule.WindowDuration(); err != nil {
		return err
	}
	switch rule.Key {
	case KeyStrategyIP, KeyStrategyGlobal:
	default:
		return fmt.Errorf("key must be %q or %q", KeyStrategyIP, KeyStrategyGlobal)
	}
	return nil
}
