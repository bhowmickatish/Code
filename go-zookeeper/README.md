# Go ZooKeeper Rate Limit Sample

A minimal Go application that loads **rate limiting rules from Apache ZooKeeper** on startup, applies them to HTTP routes, and hot-reloads when the ZK node changes.

## Architecture

```
Startup
  1. Connect to ZooKeeper
  2. If /ratelimit/rules is missing (development only), seed from config/rules.json
  3. Load and validate rules JSON
  4. Init singleton limiter (one instance per application)
  5. Watch /ratelimit/rules for changes → reload without restart

HTTP request
  1. All routes go through handler.RateLimitHandler() → RateLimitMiddleware → ratelimit.Instance()
  2. Match rule by path prefix
  3. Resolve limiter key (ip or global) on the shared instance
  4. Block on limiter.Take() until a slot is available (leaky bucket, no burst)
```

ZooKeeper stores a single JSON document at `ZK_RULES_PATH` (default `/ratelimit/rules`):

```json
{
  "version": 1,
  "rules": [
    {
      "name": "users",
      "path_prefix": "/api/users",
      "limit": 10,
      "window": "1s",
      "key": "ip"
    }
  ]
}
```

| Field         | Description                                      |
| ------------- | ------------------------------------------------ |
| `name`        | Rule identifier (logged and returned in headers) |
| `path_prefix` | Longest matching prefix selects the rule         |
| `limit`       | Max requests allowed in the window               |
| `window`      | Go duration string (`1s`, `1m`, `500ms`)          |
| `key`         | `ip` (per client) or `global` (shared bucket)    |

## Prerequisites

- Go 1.26+
- Docker (ZooKeeper)

## Quick Start

```bash
cd go-zookeeper
docker compose up -d
go run .
```

Sample endpoints:

```bash
curl http://localhost:8080/health
curl http://localhost:8080/api/users
curl http://localhost:8080/api/orders
```

`/api/users` is limited to **10 req/s per IP**. `/api/orders` uses the broader `api-default` rule (**100 req/min**).

## Update rules live

Use the ZooKeeper CLI (inside the container) or any ZK client:

```bash
docker compose exec zookeeper zkCli.sh set /ratelimit/rules '{"version":2,"rules":[{"name":"users","path_prefix":"/api/users","limit":5,"window":"1s","key":"ip"}]}'
```

The app logs the reload and applies new limits without restarting.

## Configuration

| Variable            | Default              | Description                          |
| ------------------- | -------------------- | ------------------------------------ |
| `APP_ENV`           | `development`        | Seeds ZK from file when node missing |
| `ZOOKEEPER_ADDRS`   | `localhost:2181`     | Comma-separated ZK ensemble          |
| `ZK_RULES_PATH`     | `/ratelimit/rules`   | Znode path for rules JSON            |
| `ZK_SESSION_TIMEOUT`| `10s`                | ZK session timeout                   |
| `SEED_RULES_FILE`   | `config/rules.json`  | Bootstrap file in development        |
| `SERVER_ADDR`       | `:8080`              | HTTP listen address                  |
