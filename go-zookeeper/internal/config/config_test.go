package config

import "testing"

func TestInstanceReturnsCachedConfig(t *testing.T) {
	ResetForTest()
	t.Setenv("SERVER_ADDR", ":9090")

	cfg1, err := Instance()
	if err != nil {
		t.Fatalf("first instance: %v", err)
	}

	t.Setenv("SERVER_ADDR", ":9999")

	cfg2, err := Instance()
	if err != nil {
		t.Fatalf("second instance: %v", err)
	}

	if cfg1.ServerAddr != ":9090" {
		t.Fatalf("expected :9090, got %q", cfg1.ServerAddr)
	}
	if cfg2.ServerAddr != ":9090" {
		t.Fatalf("expected cached :9090, got %q", cfg2.ServerAddr)
	}
}

func TestInstanceInvalidEnvReturnsError(t *testing.T) {
	ResetForTest()
	t.Setenv("RATELIMIT_CACHE_MAX", "not-a-number")

	if _, err := Instance(); err == nil {
		t.Fatal("expected config load error")
	}
}
