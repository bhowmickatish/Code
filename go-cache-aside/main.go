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
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}
	ctx := context.Background()

	pool, err := db.NewPool(ctx, cfg.PostgresURL)
	if err != nil {
		log.Fatalf("postgres: %v", err)
	}
	defer pool.Close()

	if cfg.IsDevelopmentMode {
		if err := db.Migrate(ctx, pool); err != nil {
			log.Fatalf("migrate: %v", err)
		}
	}

	redisClient, err := cache.NewClusterClient(cfg.RedisClusterAddrs)
	if err != nil {
		log.Fatalf("redis: %v", err)
	}
	defer redisClient.Close()

	repo := repository.NewProductRepository(
		pool,
		redisClient,
		cfg.CacheTTL,
		cfg.IdempotencyTTL,
		cfg.CacheLockTTL,
		cfg.CacheLockMaxWait,
	)
	productHandler := handler.NewProductHandler(repo, cfg.PageDefaultLimit, cfg.PageDefaultOffset, cfg.PageMaxLimit)

	mux := http.NewServeMux()
	productHandler.Register(mux)

	server := &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      mux,
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	serverErr := make(chan error, 1)
	go func() {
		log.Printf("APP_ENV=%s listening on %s", cfg.AppEnv, cfg.ServerAddr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serverErr <- err
		}
	}()

	select {
	case sig := <-stop:
		log.Printf("shutting down: %v", sig)
	case err := <-serverErr:
		log.Printf("server error: %v", err)
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
