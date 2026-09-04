# Go ZooKeeper Rate Limit Sample

A minimal Go application that loads **rate limiting rules from Apache ZooKeeper** on startup, applies them to HTTP routes, and hot-reloads when the ZK node changes.

See [DESIGN.md](./DESIGN.md) for architecture and production notes.

## Architecture

```
Startup
  1. Connect to ZooKeeper (with timeout via context)
  2. If ZK unavailable → fall back to config/rules.json
  3. If /ratelimit/rules is missing (development only), seed from config/rules.json
  4. Init singleton limiter (one instance per application)
  5. Watch /ratelimit/rules for changes → reload; fallback on delete/session loss

HTTP request
  1. /health → no rate limit
  2. /api/* → RateLimitMiddleware → ratelimit.Instance()
  3. Match rule by path prefix (longest wins)
  4. Leaky bucket: wait up to RATELIMIT_MAX_WAIT, else 429
```

## Prerequisites

- Go 1.26+
- Docker (ZooKeeper)
- `config/rules.json` present for ZK fallback (required in production deploys)

## Quick Start

```bash
cd go-zookeeper
docker compose up -d
go run .
```

```bash
curl http://localhost:8080/health          # never rate-limited
curl http://localhost:8080/api/users
curl -H "X-User-ID: alice" http://localhost:8080/api/users   # per-user bucket (users rule)
curl http://localhost:8080/api/orders                          # global bucket (orders-global rule)
curl http://localhost:8080/api/other                           # per-ip bucket (api-default rule)
```

## Configuration

| Variable              | Default              | Description                                      |
| --------------------- | -------------------- | ------------------------------------------------ |
| `APP_ENV`             | `development`        | Seeds ZK from file when node missing             |
| `ZOOKEEPER_ADDRS`     | `localhost:2181`     | Comma-separated ZK ensemble                      |
| `ZK_RULES_PATH`       | `/ratelimit/rules`   | Znode path for rules JSON                        |
| `ZK_SESSION_TIMEOUT`  | `10s`                | ZK session + startup load timeout                |
| `ZK_OPEN_ACL`         | `true` in development| `WorldACL(PermAll)` when true; else digest ACL   |
| `ZK_DIGEST`           | _(empty)_            | `user:password` for digest ACL when ACL not open |
| `SEED_RULES_FILE`     | `config/rules.json`  | Bootstrap + fallback rules file (**required**)   |
| `RATELIMIT_CACHE_MAX` | `10000`              | In-process LRU cache (placeholder for Redis)     |
| `RATELIMIT_MAX_WAIT`  | `0`                  | Max wait before 429 (`0` = reject immediately)   |
| `RATELIMIT_USER_HEADER` | `X-User-ID`        | Header for `key: "user"` rules                   |
| `TRUSTED_PROXY`       | `false`              | Trust `X-Forwarded-For` for IP keys              |
| `SERVER_ADDR`         | `:8080`              | HTTP listen address                              |

## Update rules live

```bash
docker compose exec zookeeper zkCli.sh set /ratelimit/rules '{"version":2,"rules":[{"name":"users","path_prefix":"/api/users","limit":5,"window":"1s","key":"ip"}]}'
```
