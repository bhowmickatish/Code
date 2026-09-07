package handler

import (
	"encoding/json"
	"net/http"

	"github.com/atish/go-zookeeper/internal/ratelimit"
	"github.com/atish/go-zookeeper/internal/reloadstatus"
)

func health(w http.ResponseWriter, r *http.Request) {
	snap := reloadstatus.Snapshot()
	payload := map[string]any{
		"status":   "ok",
		"rules_ok": snap.RulesOK,
		"zk_ok":    snap.ZKOK,
	}
	if snap.HasRules {
		payload["rules"] = snap.Rules
	}
	if snap.HasZK {
		payload["zk"] = snap.ZK
	}

	if !snap.RulesOK {
		payload["status"] = "degraded"
		writeJSON(w, http.StatusServiceUnavailable, payload)
		return
	}

	writeJSON(w, http.StatusOK, payload)
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
			ruleName, allowed := ratelimit.Instance().Allow(r)
			if !allowed {
				writeJSON(w, http.StatusForbidden, map[string]string{
					"error": "no rate limit rule covers this path",
				})
				return
			}
			w.Header().Set("X-RateLimit-Rule", ruleName)
			next.ServeHTTP(w, r)
		})
	}
}
