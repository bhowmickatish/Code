package handler

import (
	"encoding/json"
	"net/http"

	"github.com/atish/go-zookeeper/internal/ratelimit"
)

func health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func users(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"resource": "users",
		"message":  "sample endpoint protected by zookeeper-backed rate limits",
	})
}

func orders(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"resource": "orders",
		"message":  "falls back to api-default rule (longer path prefix wins)",
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// RateLimitMiddleware applies the application-wide limiter to every request.
// All API modules share ratelimit.Instance() via this middleware.
func RateLimitMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, ruleName, _ := ratelimit.Instance().Allow(r)
			if ruleName != "" {
				w.Header().Set("X-RateLimit-Rule", ruleName)
			}
			next.ServeHTTP(w, r)
		})
	}
}
