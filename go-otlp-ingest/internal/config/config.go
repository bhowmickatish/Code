package config

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

const DefaultClickHouseDSN = "clickhouse://default:@127.0.0.1:9000/otel"

type Config struct {
	GRPCAddr      string
	HealthAddr    string
	ClickHouseDSN string
	BatchSize     int
	BatchInterval time.Duration
	QueueCapacity int
	MaxDataPoints int
	MaxAttrKeys   int
	MaxAttrValue  int
}

var (
	instance Config
	loadErr  error
	loadOnce sync.Once
)

func Instance() (Config, error) {
	loadOnce.Do(func() {
		instance, loadErr = LoadFromEnv()
	})
	return instance, loadErr
}

func LoadFromEnv() (Config, error) {
	batchSize, err := envInt("BATCH_SIZE", 5000)
	if err != nil {
		return Config{}, err
	}
	queueCap, err := envInt("QUEUE_CAPACITY", 50_000)
	if err != nil {
		return Config{}, err
	}
	maxPoints, err := envInt("MAX_DATA_POINTS", 100_000)
	if err != nil {
		return Config{}, err
	}
	maxKeys, err := envInt("MAX_ATTR_KEYS", 64)
	if err != nil {
		return Config{}, err
	}
	maxVal, err := envInt("MAX_ATTR_VALUE", 1024)
	if err != nil {
		return Config{}, err
	}
	interval, err := envDuration("BATCH_INTERVAL", time.Second)
	if err != nil {
		return Config{}, err
	}

	return Config{
		GRPCAddr:      envOrDefault("GRPC_ADDR", ":4317"),
		HealthAddr:    envOrDefault("HEALTH_ADDR", ":8080"),
		ClickHouseDSN: envOrDefault("CLICKHOUSE_DSN", DefaultClickHouseDSN),
		BatchSize:     batchSize,
		BatchInterval: interval,
		QueueCapacity: queueCap,
		MaxDataPoints: maxPoints,
		MaxAttrKeys:   maxKeys,
		MaxAttrValue:  maxVal,
	}, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be an integer", key)
	}
	return n, nil
}

func envDuration(key string, fallback time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s must be a duration (e.g. 1s, 50ms)", key)
	}
	return d, nil
}
