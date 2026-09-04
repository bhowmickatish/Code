package handler

import (
	"encoding/json"
	"net/http"
	"strconv"

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
func RateLimitMiddleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			ruleName, allowed, retryAfter := ratelimit.Instance().Allow(r)
			if ruleName != "" {
				w.Header().Set("X-RateLimit-Rule", ruleName)
			}
			if !allowed {
				retrySec := int(retryAfter.Seconds())
				if retrySec < 1 {
					retrySec = 1
				}
				w.Header().Set("Retry-After", strconv.Itoa(retrySec))
				writeJSON(w, http.StatusTooManyRequests, map[string]string{
					"error":   "rate limit exceeded",
					"rule":    ruleName,
					"message": "try again later",
				})
				return
			}
			next.ServeHTTP(w, r)
		})
	}
}
