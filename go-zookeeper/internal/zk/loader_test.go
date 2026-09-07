package zk

import (
	"os"
	"path/filepath"
	"testing"

	gzk "github.com/go-zookeeper/zk"

	"github.com/atish/go-zookeeper/internal/model"
)

func TestLoadRulesFromFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.json")
	data := []byte(`{
		"version": 1,
		"rules": [{
			"name": "users",
			"path_prefix": "/api/users",
			"limit": 10,
			"window": "1s",
			"key": "ip"
		}]
	}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write rules file: %v", err)
	}

	doc, err := LoadRulesFromFile(path)
	if err != nil {
		t.Fatalf("load rules: %v", err)
	}
	if len(doc.Rules) != 1 || doc.Rules[0].Name != "users" {
		t.Fatalf("unexpected rules: %+v", doc.Rules)
	}
}

func TestHandleSessionExpiredRequestsRewatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.json")
	data := []byte(`{
		"version": 1,
		"rules": [{
			"name": "users",
			"path_prefix": "/api/users",
			"limit": 10,
			"window": "1s",
			"key": "ip"
		}]
	}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write rules file: %v", err)
	}

	loader := &Loader{seedFile: path, rulesPath: "/ratelimit/rules"}
	applied := false
	rewatch := loader.handleSessionEvent(gzk.Event{
		Type:  gzk.EventSession,
		State: gzk.StateExpired,
	}, func(doc model.RulesDocument) error {
		applied = true
		return nil
	})

	if !applied {
		t.Fatal("expected fallback rules to be applied on session expiry")
	}
	if !rewatch {
		t.Fatal("expected session expiry to request watch re-registration")
	}
}

func TestHandleNodeDeletedAppliesFallback(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "rules.json")
	data := []byte(`{
		"version": 1,
		"rules": [{
			"name": "users",
			"path_prefix": "/api/users",
			"limit": 10,
			"window": "1s",
			"key": "ip"
		}]
	}`)
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatalf("write rules file: %v", err)
	}

	loader := &Loader{seedFile: path, rulesPath: "/ratelimit/rules"}
	applied := false
	loader.handleNodeEvent(gzk.Event{Type: gzk.EventNodeDeleted}, func(doc model.RulesDocument) error {
		applied = true
		if len(doc.Rules) != 1 {
			t.Fatalf("expected 1 rule, got %d", len(doc.Rules))
		}
		return nil
	})
	if !applied {
		t.Fatal("expected fallback rules to be applied on node delete")
	}
}
