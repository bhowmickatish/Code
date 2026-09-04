package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/atish/go-zookeeper/internal/config"
	"github.com/atish/go-zookeeper/internal/handler"
	"github.com/atish/go-zookeeper/internal/model"
	"github.com/atish/go-zookeeper/internal/ratelimit"
	"github.com/atish/go-zookeeper/internal/zk"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	zkClient, err := zk.Connect(cfg.ZookeeperAddrs, cfg.ZKSessionTimeout)
	if err != nil {
		log.Fatalf("zookeeper: %v", err)
	}
	defer zkClient.Close()

	loader := zk.NewLoader(zkClient, cfg.RulesPath, cfg.SeedRulesFile)
	doc, err := loader.LoadOnStartup(ctx, cfg.IsDevelopmentMode)
	if err != nil {
		log.Fatalf("load rules: %v", err)
	}

	limiter, err := ratelimit.NewLimiter(doc)
	if err != nil {
		log.Fatalf("rate limiter: %v", err)
	}

	go func() {
		if err := loader.Watch(ctx, func(updated model.RulesDocument) {
			if err := limiter.Update(updated); err != nil {
				log.Printf("apply updated rules failed: %v", err)
			}
		}); err != nil && err != context.Canceled {
			log.Printf("zookeeper watch stopped: %v", err)
		}
	}()

	api := handler.NewAPI(limiter)
	mux := http.NewServeMux()
	api.Register(mux)

	server := &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      handler.RateLimitMiddleware(limiter)(mux),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		log.Printf("APP_ENV=%s listening on %s (zookeeper=%v rules_path=%s)",
			cfg.AppEnv, cfg.ServerAddr, cfg.ZookeeperAddrs, cfg.RulesPath)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("server: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
