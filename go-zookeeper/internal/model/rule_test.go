package model

import (
	"testing"
	"time"
)

func TestParseRulesDocument(t *testing.T) {
	raw := []byte(`{
		"version": 1,
		"rules": [{
			"name": "users",
			"path_prefix": "/api/users",
			"limit": 10,
			"window": "1s",
			"key": "ip"
		}]
	}`)

	doc, err := ParseRulesDocument(raw)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if doc.Version != 1 {
		t.Fatalf("expected version 1, got %d", doc.Version)
	}
	if len(doc.Rules) != 1 {
		t.Fatalf("expected 1 rule, got %d", len(doc.Rules))
	}

	window, err := doc.Rules[0].WindowDuration()
	if err != nil {
		t.Fatalf("window duration: %v", err)
	}
	if window != time.Second {
		t.Fatalf("expected 1s window, got %s", window)
	}
}

func TestParseRulesDocumentRejectsInvalidKey(t *testing.T) {
	raw := []byte(`{
		"version": 1,
		"rules": [{
			"name": "bad",
			"path_prefix": "/api",
			"limit": 1,
			"window": "1s",
			"key": "header"
		}]
	}`)

	_, err := ParseRulesDocument(raw)
	if err == nil {
		t.Fatal("expected validation error")
	}
}
