package handler

import "net/http"

// RateLimitHandler builds the application HTTP handler wrapped with RateLimitMiddleware.
// All API modules register on a shared mux so every route uses the singleton limiter.
func RateLimitHandler() http.Handler {
	mux := http.NewServeMux()

	registerHealthRoutes(mux)
	registerUserRoutes(mux)
	registerOrderRoutes(mux)

	return RateLimitMiddleware()(mux)
}

func registerHealthRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/health", health)
}

func registerUserRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/users", users)
}

func registerOrderRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/orders", orders)
}
