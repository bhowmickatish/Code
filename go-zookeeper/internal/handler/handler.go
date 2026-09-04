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
		"message":  "protected by orders-global rule (shared bucket for all clients)",
	})
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

// RateLimitMiddleware applies the application-wide limiter to every request.
func RateLimitMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if ruleName := ratelimit.Instance().Allow(r); ruleName != "" {
				w.Header().Set("X-RateLimit-Rule", ruleName)
			}
			next.ServeHTTP(w, r)
		})
	}
}
