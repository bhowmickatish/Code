package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/atish/go-cache-aside/internal/cache"
	"github.com/atish/go-cache-aside/internal/config"
	"github.com/atish/go-cache-aside/internal/db"
	"github.com/atish/go-cache-aside/internal/handler"
	"github.com/atish/go-cache-aside/internal/repository"
)

func main() {
	cfg := config.Load()
	ctx := context.Background()

	pool, err := db.NewPool(ctx, cfg.PostgresURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	if err := db.Migrate(ctx, pool); err != nil {
		log.Fatalf("migrate: %v", err)
	}

	redisClient, err := cache.NewClusterClient(cfg.RedisClusterAddrs)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	repo := repository.NewProductRepository(pool, redisClient, cfg.CacheTTL)
	productHandler := handler.NewProductHandler(repo, cfg.PageDefaultLimit, cfg.PageDefaultOffset, cfg.PageMaxLimit)

	mux := http.NewServeMux()
	productHandler.Register(mux)

	server := &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("listening on %s", cfg.ServerAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)
	<-stop

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
