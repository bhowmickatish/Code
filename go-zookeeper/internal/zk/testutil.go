package zk

import (
	"testing"

	"github.com/atish/go-zookeeper/internal/model"
)

var testLoadHook func(*Loader) (model.RulesDocument, error)

func withTestLoadHook(t *testing.T, hook func(*Loader) (model.RulesDocument, error)) {
	t.Helper()

	prev := testLoadHook
	testLoadHook = hook
	t.Cleanup(func() {
		testLoadHook = prev
	})
}
