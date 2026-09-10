package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/atish/go-otlp-ingest/internal/batcher"
	"github.com/atish/go-otlp-ingest/internal/clickhouse"
	"github.com/atish/go-otlp-ingest/internal/config"
	"github.com/atish/go-otlp-ingest/internal/mapper"
	"github.com/atish/go-otlp-ingest/internal/otlpgrpc"
)

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, nil)))

	cfg, err := config.Instance()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	openCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	store, err := clickhouse.Open(openCtx, cfg.ClickHouseDSN)
	cancel()
	if err != nil {
		slog.Error("clickhouse open failed", "err", err)
		os.Exit(1)
	}
	defer store.Close()

	b := batcher.New(store, cfg.BatchSize, cfg.QueueCapacity, cfg.BatchInterval, slog.Default())

	lis, err := net.Listen("tcp", cfg.GRPCAddr)
	if err != nil {
		slog.Error("listen failed", "err", err)
		os.Exit(1)
	}

	gs := grpc.NewServer()
	colmetricspb.RegisterMetricsServiceServer(gs, otlpgrpc.New(b, cfg.MaxDataPoints, mapper.Limits{
		MaxAttrKeys:  cfg.MaxAttrKeys,
		MaxAttrValue: cfg.MaxAttrValue,
	}))
	hs := health.NewServer()
	healthpb.RegisterHealthServer(gs, hs)
	setHealth(hs, store, b)

	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				setHealth(hs, store, b)
			}
		}
	}()

	httpSrv := &http.Server{
		Addr:         cfg.HealthAddr,
		Handler:      healthHandler(store, b),
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 5 * time.Second,
	}

	go func() {
		slog.Info("http health listening", "addr", cfg.HealthAddr)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("http health failed", "err", err)
		}
	}()

	go func() {
		slog.Info("otlp metrics grpc listening", "addr", cfg.GRPCAddr)
		if err := gs.Serve(lis); err != nil {
			slog.Error("grpc serve failed", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	stopped := make(chan struct{})
	go func() {
		gs.GracefulStop()
		close(stopped)
	}()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		gs.Stop()
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	_ = httpSrv.Shutdown(shutdownCtx)
	if err := b.Close(shutdownCtx); err != nil {
		slog.Warn("batcher flush", "err", err)
	}
}

func setHealth(hs *health.Server, store *clickhouse.Store, b *batcher.Batcher) {
	if !ingestReady(store, b) {
		hs.SetServingStatus("", healthpb.HealthCheckResponse_NOT_SERVING)
		return
	}
	hs.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)
}

func healthHandler(store *clickhouse.Store, b *batcher.Batcher) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			http.NotFound(w, r)
			return
		}
		if !ingestReady(store, b) {
			http.Error(w, "ingest unavailable", http.StatusServiceUnavailable)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
}

func ingestReady(store *clickhouse.Store, b *batcher.Batcher) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := store.Ping(ctx); err != nil {
		return false
	}
	return b.IngestReady()
}
