package config

import (
	"fmt"
	"os"
	"time"
)

const DefaultAppEnv = "development"

type Config struct {
	AppEnv            string
	IsDevelopmentMode bool
	ZookeeperAddrs    []string
	RulesPath         string
	ServerAddr        string
	ZKSessionTimeout  time.Duration
	SeedRulesFile     string
}

func Load() (Config, error) {
	sessionTimeout, err := envDuration("ZK_SESSION_TIMEOUT", 10*time.Second)
	if err != nil {
		return Config{}, err
	}

	appEnv := envOrDefault("APP_ENV", DefaultAppEnv)

	return Config{
		AppEnv:            appEnv,
		IsDevelopmentMode: appEnv == DefaultAppEnv,
		ZookeeperAddrs:    envCSV("ZOOKEEPER_ADDRS", "localhost:2181"),
		RulesPath:         envOrDefault("ZK_RULES_PATH", "/ratelimit/rules"),
		ServerAddr:        envOrDefault("SERVER_ADDR", ":8080"),
		ZKSessionTimeout:  sessionTimeout,
		SeedRulesFile:     envOrDefault("SEED_RULES_FILE", "config/rules.json"),
	}, nil
}

func envOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
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
