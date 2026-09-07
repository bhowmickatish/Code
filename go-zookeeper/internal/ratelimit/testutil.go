package ratelimit

import (
	"sync"
	"testing"

	"github.com/atish/go-zookeeper/internal/model"
)

// ResetForTest re-initializes the singleton limiter for tests.
func ResetForTest(t *testing.T, doc model.RulesDocument) *Limiter {
	t.Helper()

	instance = nil
	initOnce = sync.Once{}

	if err := Instance().Update(doc); err != nil {
		t.Fatalf("update test rules: %v", err)
	}
	return Instance()
}
