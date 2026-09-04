package model

import (
	"testing"
)

func TestParseRulesDocumentRejectsDuplicateNames(t *testing.T) {
	raw := []byte(`{
		"version": 1,
		"rules": [
			{"name": "dup", "path_prefix": "/a", "limit": 1, "window": "1s", "key": "ip"},
			{"name": "dup", "path_prefix": "/b", "limit": 1, "window": "1s", "key": "ip"}
		]
	}`)

	_, err := ParseRulesDocument(raw)
	if err == nil {
		t.Fatal("expected duplicate name validation error")
	}
}
