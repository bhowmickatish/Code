package ratelimit

import (
	"testing"
	"time"

	uberlimit "go.uber.org/ratelimit"
)

func TestLimiterEntryUsesUberSpacing(t *testing.T) {
	uberElapsed := measureUberSpacing()
	entryElapsed := measureEntrySpacing()

	if uberElapsed < 400*time.Millisecond {
		t.Fatalf("uber spacing too short: %v", uberElapsed)
	}
	if entryElapsed < 400*time.Millisecond {
		t.Fatalf("entry spacing too short: %v", entryElapsed)
	}
}

func measureUberSpacing() time.Duration {
	rl := uberlimit.New(2, uberlimit.Per(time.Second), uberlimit.WithoutSlack)
	rl.Take()
	start := time.Now()
	rl.Take()
	return time.Since(start)
}

func measureEntrySpacing() time.Duration {
	entry := &limiterEntry{}
	entry.configure(2, time.Second)
	entry.take()
	start := time.Now()
	entry.take()
	return time.Since(start)
}

func TestEntryCacheLRUEviction(t *testing.T) {
	cache := newEntryCache(2)

	a := cache.get("a", 10, time.Second)
	b := cache.get("b", 10, time.Second)
	cache.get("a", 10, time.Second)

	if _, ok := cache.entries["b"]; !ok {
		t.Fatal("b should be cached before inserting c")
	}

	_ = cache.get("c", 10, time.Second)

	if _, ok := cache.entries["b"]; ok {
		t.Fatal("b should have been evicted as LRU")
	}
	if _, ok := cache.entries["a"]; !ok {
		t.Fatal("a should remain cached after LRU eviction of b")
	}

	reloadedA := cache.get("a", 10, time.Second)
	if reloadedA != a {
		t.Fatal("a entry pointer should be stable across LRU eviction of b")
	}

	reloadedB := cache.get("b", 10, time.Second)
	if reloadedB == b {
		t.Fatal("b entry should have been recreated after eviction")
	}
}
