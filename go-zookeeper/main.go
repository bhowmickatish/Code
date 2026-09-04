package main

import (
	"context"
	"log/slog"
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
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	doc, zkClient, loader := loadRules(ctx, cfg)
	if zkClient != nil {
		defer zkClient.Close()
	}

	if _, err := ratelimit.Init(doc, cfg.RateLimitCacheMax, cfg.RateLimitMaxWait, cfg.TrustedProxy, cfg.RateLimitUserHeader); err != nil {
		slog.Error("rate limiter init failed", "err", err)
		os.Exit(1)
	}

	if loader != nil {
		go func() {
			if err := loader.Watch(ctx, func(updated model.RulesDocument) {
				if err := ratelimit.Instance().Update(updated); err != nil {
					slog.Error("apply updated rules failed", "err", err)
				}
			}); err != nil && err != context.Canceled {
				slog.Warn("zookeeper watch stopped", "err", err)
			}
		}()
	}

	server := &http.Server{
		Addr:         cfg.ServerAddr,
		Handler:      handler.RateLimitHandler(),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 10 * time.Second,
	}

	go func() {
		slog.Info("server listening",
			"app_env", cfg.AppEnv,
			"addr", cfg.ServerAddr,
			"zookeeper", cfg.ZookeeperAddrs,
			"rules_path", cfg.RulesPath,
		)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Warn("shutdown error", "err", err)
	}
}

func loadRules(ctx context.Context, cfg config.Config) (model.RulesDocument, *zk.Client, *zk.Loader) {
	loadCtx, cancel := context.WithTimeout(ctx, cfg.ZKSessionTimeout)
	defer cancel()

	zkClient, err := zk.Connect(cfg.ZookeeperAddrs, cfg.ZKSessionTimeout, cfg.ZKOpenACL, cfg.ZKDigest)
	if err != nil {
		slog.Warn("zookeeper connect failed, using fallback rules file",
			"file", cfg.SeedRulesFile,
			"err", err,
		)
		doc, err := zk.LoadRulesFromFile(cfg.SeedRulesFile)
		if err != nil {
			slog.Error("load rules failed", "err", err)
			os.Exit(1)
		}
		return doc, nil, nil
	}

	loader := zk.NewLoader(zkClient, cfg.RulesPath, cfg.SeedRulesFile)
	doc, err := loader.LoadOnStartup(loadCtx, cfg.IsDevelopmentMode)
	if err != nil {
		zkClient.Close()
		slog.Error("load rules failed", "err", err)
		os.Exit(1)
	}
	return doc, zkClient, loader
}
