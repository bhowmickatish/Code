package handler

import "net/http"

// RateLimitHandler builds the application HTTP handler.
// /health is registered outside rate-limit middleware; API routes are limited.
func RateLimitHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", health)

	limited := http.NewServeMux()
	registerUserRoutes(limited)
	registerOrderRoutes(limited)
	mux.Handle("/api/", RateLimitMiddleware()(limited))

	return mux
}

func registerUserRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/users", users)
}

func registerOrderRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/orders", orders)
}
