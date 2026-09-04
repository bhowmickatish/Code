package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/atish/go-zookeeper/internal/model"
	"github.com/atish/go-zookeeper/internal/ratelimit"
)

func TestRateLimitMiddlewareThrottles(t *testing.T) {
	doc := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "test", PathPrefix: "/", Limit: 1, Window: "1s", Key: model.KeyStrategyGlobal},
		},
	}

	instance := ratelimit.ResetForTest(t, doc, 100, false)

	handler := RateLimitMiddleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	req := httptest.NewRequest(http.MethodGet, "/api/users", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("first request expected 200, got %d", rec.Code)
	}

	start := time.Now()
	rec = httptest.NewRecorder()
	handler.ServeHTTP(rec, req)
	elapsed := time.Since(start)

	if rec.Code != http.StatusOK {
		t.Fatalf("second request expected 200 after throttle, got %d", rec.Code)
	}
	if elapsed < 900*time.Millisecond {
		t.Fatalf("expected ~1s throttle, got %v", elapsed)
	}
	if rec.Header().Get("X-RateLimit-Rule") != "test" {
		t.Fatalf("expected X-RateLimit-Rule=test, got %q", rec.Header().Get("X-RateLimit-Rule"))
	}

	_ = instance
}

func TestHealthBypassesRateLimitMiddleware(t *testing.T) {
	doc := model.RulesDocument{
		Version: 1,
		Rules: []model.RateLimit{
			{Name: "test", PathPrefix: "/", Limit: 1, Window: "1s", Key: model.KeyStrategyGlobal},
		},
	}
	ratelimit.ResetForTest(t, doc, 100, false)

	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)

	limited := http.NewServeMux()
	limited.HandleFunc("/api/users", users)
	mux.Handle("/api/", RateLimitMiddleware()(limited))

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodGet, "/health", nil)
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("health request %d expected 200, got %d", i+1, rec.Code)
		}
	}
}
