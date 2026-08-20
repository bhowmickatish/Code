package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	PostgresURL       string
	RedisClusterAddrs []string
	CacheTTL          time.Duration
	ServerAddr        string
	PageDefaultLimit  int
	PageDefaultOffset int
	PageMaxLimit      int
}

func Load() Config {
	return Config{
		PostgresURL:       envOrDefault("POSTGRES_URL", "postgres://app:app@localhost:5432/appdb?sslmode=disable"),
		RedisClusterAddrs: envCSV("REDIS_CLUSTER_ADDRS", "localhost:6379,localhost:6380,localhost:6381"),
		CacheTTL:          5 * time.Minute,
		ServerAddr:        envOrDefault("SERVER_ADDR", ":8080"),
		PageDefaultLimit:  envInt("PAGE_DEFAULT_LIMIT", 20),
		PageDefaultOffset: envInt("PAGE_DEFAULT_OFFSET", 0),
		PageMaxLimit:      envInt("PAGE_MAX_LIMIT", 100),
	}
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
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
