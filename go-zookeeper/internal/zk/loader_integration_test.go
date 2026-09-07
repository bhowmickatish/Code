package zk

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gzk "github.com/go-zookeeper/zk"
)

func waitForConnected(client *Client, timeout time.Duration) bool {
	timer := time.NewTimer(timeout)
	defer timer.Stop()

	for {
		select {
		case <-timer.C:
			return false
		case event := <-client.Events():
			if event.Type == gzk.EventSession && event.State == gzk.StateConnected {
				return true
			}
		}
	}
}

func TestConnectAndLoadRulesIntegration(t *testing.T) {
	addr := os.Getenv("ZOOKEEPER_ADDRS")
	if addr == "" {
		addr = "localhost:2181"
	}

	client, err := Connect([]string{addr}, 5*time.Second, true, "")
	if err != nil {
		t.Skipf("zookeeper not available at %s: %v", addr, err)
	}
	defer client.Close()

	if !waitForConnected(client, 5*time.Second) {
		t.Skipf("zookeeper session not established at %s", addr)
	}

	dir := t.TempDir()
	seedFile := filepath.Join(dir, "rules.json")
	seed := []byte(`{
		"version": 1,
		"rules": [{
			"name": "integration",
			"path_prefix": "/api/",
			"limit": 10,
			"window": "1s",
			"key": "ip"
		}]
	}`)
	if err := os.WriteFile(seedFile, seed, 0o644); err != nil {
		t.Fatalf("write seed rules: %v", err)
	}

	rulesPath := "/ratelimit/integration-" + strings.ReplaceAll(t.Name(), "/", "-")
	exists, err := client.Exists(rulesPath)
	if err != nil {
		t.Fatalf("exists: %v", err)
	}
	if exists {
		if err := client.Set(rulesPath, seed); err != nil {
			t.Fatalf("set rules: %v", err)
		}
	} else if err := client.CreatePath(rulesPath, seed); err != nil {
		t.Fatalf("create rules path: %v", err)
	}

	loader := NewLoader(client, rulesPath, seedFile)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	doc, err := loader.LoadOnStartup(ctx, false)
	if err != nil {
		t.Fatalf("load on startup: %v", err)
	}
	if len(doc.Rules) != 1 || doc.Rules[0].Name != "integration" {
		t.Fatalf("unexpected rules: %+v", doc.Rules)
	}
}
