package cache

import "testing"

func TestProductSlotKeys(t *testing.T) {
	keys := ProductSlotKeys(42)
	if keys.Data != "{product:42}:data" {
		t.Fatalf("Data = %q, want {product:42}:data", keys.Data)
	}
	if keys.Notify != "{product:42}:notify" {
		t.Fatalf("Notify = %q, want {product:42}:notify", keys.Notify)
	}
	if keys.Lock != "{product:42}:lock" {
		t.Fatalf("Lock = %q, want {product:42}:lock", keys.Lock)
	}
}

func TestIdempotencySlotKeys(t *testing.T) {
	keys := IdempotencySlotKeys("abc123")
	if keys.Data != "{idempotency:abc123}:data" {
		t.Fatalf("Data = %q, want {idempotency:abc123}:data", keys.Data)
	}
	if keys.Notify != "{idempotency:abc123}:notify" {
		t.Fatalf("Notify = %q, want {idempotency:abc123}:notify", keys.Notify)
	}
	if keys.Lock != "{idempotency:abc123}:lock" {
		t.Fatalf("Lock = %q, want {idempotency:abc123}:lock", keys.Lock)
	}
}
