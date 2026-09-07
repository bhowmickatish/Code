package zk

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	gzk "github.com/go-zookeeper/zk"

	"github.com/atish/go-zookeeper/internal/model"
	"github.com/atish/go-zookeeper/internal/reloadstatus"
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
	reloadstatus.ResetForTest()

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

	snap := reloadstatus.Snapshot()
	if snap.ZKOK || snap.ZK.Source != "zookeeper-session" {
		t.Fatalf("expected session expiry zookeeper status, got %+v", snap)
	}
}

func TestHandleSessionConnectedReloadsFromZK(t *testing.T) {
	reloadstatus.ResetForTest()

	loader := &Loader{rulesPath: "/ratelimit/rules"}
	withTestLoadHook(t, func(*Loader) (model.RulesDocument, error) {
		return model.RulesDocument{
			Version: 1,
			Rules: []model.RateLimit{{
				Name: "users", PathPrefix: "/api/users", Limit: 10, Window: "1s", Key: model.KeyStrategyIP,
			}},
		}, nil
	})

	applied := false
	rewatch := loader.handleSessionEvent(gzk.Event{
		Type:  gzk.EventSession,
		State: gzk.StateConnected,
	}, func(doc model.RulesDocument) error {
		applied = true
		return nil
	})

	if !applied {
		t.Fatal("expected rules reload on session connect")
	}
	if rewatch {
		t.Fatal("did not expect rewatch on session connect")
	}

	snap := reloadstatus.Snapshot()
	if !snap.RulesOK || snap.Rules.Source != "zookeeper" {
		t.Fatalf("unexpected rules snapshot: %+v", snap)
	}
	if !snap.ZKOK || snap.ZK.Source != "zookeeper" {
		t.Fatalf("unexpected zk snapshot: %+v", snap)
	}
}

func TestReloadFromZKLoadFailureRecordsError(t *testing.T) {
	reloadstatus.ResetForTest()

	loader := &Loader{rulesPath: "/ratelimit/rules"}
	withTestLoadHook(t, func(*Loader) (model.RulesDocument, error) {
		return model.RulesDocument{}, errors.New("parse rules json: invalid")
	})

	loader.reloadFromZK(func(model.RulesDocument) error {
		t.Fatal("should not apply rules when zookeeper load fails")
		return nil
	})

	snap := reloadstatus.Snapshot()
	if !snap.RulesOK {
		t.Fatal("expected rules_ok true when zookeeper load failed before apply")
	}
	if snap.ZKOK || snap.ZK.Source != "zookeeper" {
		t.Fatalf("expected failed zookeeper status, got %+v", snap)
	}
}

func TestApplyReloadRecordsUpdateError(t *testing.T) {
	reloadstatus.ResetForTest()

	applyReload(func(model.RulesDocument) error {
		return errors.New("invalid rules")
	}, model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{{
			Name: "users", PathPrefix: "/api/users", Limit: 10, Window: "1s", Key: model.KeyStrategyIP,
		}},
	}, "zookeeper")

	snap := reloadstatus.Snapshot()
	if snap.RulesOK || snap.Rules.Source != "zookeeper" {
		t.Fatalf("expected failed rules apply status, got %+v", snap)
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
