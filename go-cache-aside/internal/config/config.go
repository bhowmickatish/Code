package config

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

const DefaultAppEnv = "development"

type Config struct {
	AppEnv                string
	IsDevelopmentMode     bool
	PostgresURL           string
	RedisClusterAddrs     []string
	CacheTTL              time.Duration
	CacheLockTTL          time.Duration
	CacheLockMaxWait      time.Duration
	CacheLockPollInterval time.Duration
	ServerAddr            string
	PageDefaultLimit      int
	PageDefaultOffset     int
	PageMaxLimit          int
}

func Load() (Config, error) {
	pageDefaultLimit, err := envInt("PAGE_DEFAULT_LIMIT", 20)
	if err != nil {
		return Config{}, err
	}
	pageDefaultOffset, err := envInt("PAGE_DEFAULT_OFFSET", 0)
	if err != nil {
		return Config{}, err
	}
	pageMaxLimit, err := envInt("PAGE_MAX_LIMIT", 100)
	if err != nil {
		return Config{}, err
	}

	cacheLockTTL, err := envDuration("CACHE_LOCK_TTL", 10*time.Second)
	if err != nil {
		return Config{}, err
	}
	cacheLockMaxWait, err := envDuration("CACHE_LOCK_MAX_WAIT", 3*time.Second)
	if err != nil {
		return Config{}, err
	}
	cacheLockPollInterval, err := envDuration("CACHE_LOCK_POLL_INTERVAL", 50*time.Millisecond)
	if err != nil {
		return Config{}, err
	}

	appEnv := envOrDefault("APP_ENV", DefaultAppEnv)

	return Config{
		AppEnv:                appEnv,
		IsDevelopmentMode:     appEnv == DefaultAppEnv,
		PostgresURL:           envOrDefault("POSTGRES_URL", "postgres://app:app@localhost:5432/appdb?sslmode=disable"),
		RedisClusterAddrs:     envCSV("REDIS_CLUSTER_ADDRS", "localhost:6379,localhost:6380,localhost:6381"),
		CacheTTL:              5 * time.Minute,
		CacheLockTTL:          cacheLockTTL,
		CacheLockMaxWait:      cacheLockMaxWait,
		CacheLockPollInterval: cacheLockPollInterval,
		ServerAddr:            envOrDefault("SERVER_ADDR", ":8080"),
		PageDefaultLimit:      pageDefaultLimit,
		PageDefaultOffset:     pageDefaultOffset,
		PageMaxLimit:          pageMaxLimit,
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
		return 0, fmt.Errorf("%s must be a duration (e.g. 10s, 50ms)", key)
	}
	return d, nil
}

func envCSV(key, fallback string) []string {
	raw := envOrDefault(key, fallback)
	parts := strings.Split(raw, ",")
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			addrs = append(addrs, part)
		}
	}
	return addrs
}
