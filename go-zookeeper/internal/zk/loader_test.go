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
	loader.handleNodeEvent(gzk.Event{Type: gzk.EventNodeDeleted}, func(doc model.RulesDocument) {
		applied = true
		if len(doc.Rules) != 1 {
			t.Fatalf("expected 1 rule, got %d", len(doc.Rules))
		}
	})
	if !applied {
		t.Fatal("expected fallback rules to be applied on node delete")
	}
}
