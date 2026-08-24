package cache

import "testing"

func TestLockKey(t *testing.T) {
	if got := LockKey("product:42"); got != "lock:product:42" {
		t.Fatalf("LockKey() = %q, want lock:product:42", got)
	}
}
