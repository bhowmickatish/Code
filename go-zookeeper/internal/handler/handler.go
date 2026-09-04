package handler

import (
	"encoding/json"
	"net/http"

	"github.com/atish/go-zookeeper/internal/ratelimit"
)

type API struct {
	limiter *ratelimit.Limiter
}

func NewAPI(limiter *ratelimit.Limiter) *API {
	return &API{limiter: limiter}
}

func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/health", a.health)
	mux.HandleFunc("/api/users", a.users)
	mux.HandleFunc("/api/orders", a.orders)
}

func (a *API) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *API) users(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"resource": "users",
		"message":  "sample endpoint protected by zookeeper-backed rate limits",
	})
}

func (a *API) orders(w http.ResponseWriter, r *http.Request) {
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

func RateLimitMiddleware(limiter *ratelimit.Limiter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, ruleName, _ := limiter.Allow(r)
			if ruleName != "" {
				w.Header().Set("X-RateLimit-Rule", ruleName)
			}
			next.ServeHTTP(w, r)
		})
	}
}
