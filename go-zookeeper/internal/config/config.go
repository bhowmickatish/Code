package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const DefaultAppEnv = "development"

type Config struct {
	AppEnv              string
	IsDevelopmentMode   bool
	ZookeeperAddrs      []string
	RulesPath           string
	ServerAddr          string
	ZKSessionTimeout    time.Duration
	SeedRulesFile       string
	RateLimitCacheMax   int
	RateLimitMaxWait    time.Duration
	RateLimitUserHeader string
	TrustedProxy        bool
	ZKOpenACL           bool
	ZKDigest            string
}

func Load() (Config, error) {
	sessionTimeout, err := envDuration("ZK_SESSION_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	appEnv := envOrDefault("APP_ENV", DefaultAppEnv)

	cacheMax, err := envInt("RATELIMIT_CACHE_MAX", 10_000)
	if err != nil {
		return Config{}, err
	}

	maxWait, err := envDuration("RATELIMIT_MAX_WAIT", 0)
	if err != nil {
		return Config{}, err
	}

	trustedProxy, err := envBool("TRUSTED_PROXY", false)
	if err != nil {
		return Config{}, err
	}

	openACL := appEnv == DefaultAppEnv
	if v := os.Getenv("ZK_OPEN_ACL"); v != "" {
		openACL, err = envBool("ZK_OPEN_ACL", openACL)
		if err != nil {
			return Config{}, err
		}
	}

	return Config{
		AppEnv:              appEnv,
		IsDevelopmentMode:   appEnv == DefaultAppEnv,
		ZookeeperAddrs:      envCSV("ZOOKEEPER_ADDRS", "localhost:2181"),
		RulesPath:           envOrDefault("ZK_RULES_PATH", "/ratelimit/rules"),
		ServerAddr:          envOrDefault("SERVER_ADDR", ":8080"),
		ZKSessionTimeout:    sessionTimeout,
		SeedRulesFile:       envOrDefault("SEED_RULES_FILE", "config/rules.json"),
		RateLimitCacheMax:   cacheMax,
		RateLimitMaxWait:    maxWait,
		RateLimitUserHeader: envOrDefault("RATELIMIT_USER_HEADER", "X-User-ID"),
		TrustedProxy:        trustedProxy,
		ZKOpenACL:           openACL,
		ZKDigest:            os.Getenv("ZK_DIGEST"),
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

func envBool(key string, fallback bool) (bool, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean", key)
	}
	return b, nil
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
	parts := splitCSV(raw)
	addrs := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			addrs = append(addrs, part)
		}
	}
	return addrs
}

func splitCSV(raw string) []string {
	var parts []string
	start := 0
	for i := 0; i <= len(raw); i++ {
		if i == len(raw) || raw[i] == ',' {
			part := trimSpace(raw[start:i])
			if part != "" {
				parts = append(parts, part)
			}
			start = i + 1
		}
	}
	return parts
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t') {
		s = s[:len(s)-1]
	}
	return s
}
