package ratelimit

import (
	"testing"

	"github.com/atish/go-zookeeper/internal/model"
)

// ResetForTest re-initializes the singleton limiter for tests.
func ResetForTest(t *testing.T, doc model.RulesDocument, maxCacheEntries int, trustedProxy bool) *Limiter {
	t.Helper()

	initMu.Lock()
	instance = nil
	initMu.Unlock()

	limiter, err := Init(doc, maxCacheEntries, trustedProxy, "")
	if err != nil {
		t.Fatalf("init test limiter: %v", err)
	}
	return limiter
}
