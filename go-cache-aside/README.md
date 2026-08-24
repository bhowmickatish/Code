# Go Cache-Aside Sample

A minimal Go REST API demonstrating the **cache-aside** pattern with Redis Cluster and PostgreSQL.

See [DESIGN.md](./DESIGN.md) for full architecture and design decisions.

## Architecture

```
GET /products/{id}
  1. Check Redis for key `product:{id}`
  2. On hit  → return cached JSON
  3. On miss → two-gate stampede mitigation, then load and cache:
       Gate 1 (singleflight): one in-flight load per product ID per instance
       Gate 2 (Redis lock):   SET lock:product:{id} NX — one loader globally;
                              waiters poll cache until populated (see DESIGN.md §3.2)
     → read from PostgreSQL, write to Redis (TTL 5 min), return

GET /products / GET /products?q=...
  → Always PostgreSQL (paginated; never cached)

PUT/DELETE /products/{id}
  1. Write to PostgreSQL
  2. Invalidate Redis key `product:{id}`
```

Redis nodes in `docker-compose.yml` use `maxmemory 256mb` and `volatile-lru` for capacity eviction (keys with TTL only).

## Prerequisites

- Go 1.26+
- Docker (Postgres + Redis Cluster)

## Quick Start

```bash
# Start Postgres and Redis Cluster (3 masters on 6379, 6380, 6381)
docker compose up -d

# Run the app (APP_ENV=development by default — runs schema migrate)
go run .

# Create a product (price_cents: 999 = $9.99)
curl -X POST http://localhost:8080/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Widget","price_cents":999}'

# List (Postgres, paginated)
curl "http://localhost:8080/products?limit=10&offset=0"

# Search (Postgres, paginated)
curl "http://localhost:8080/products?q=widget&limit=10"

# First GET by ID hits Postgres and caches the result
curl http://localhost:8080/products/1

# Subsequent GETs are served from Redis
curl http://localhost:8080/products/1

# Update invalidates the cache
curl -X PUT http://localhost:8080/products/1 \
  -H "Content-Type: application/json" \
  -d '{"name":"Super Widget","price_cents":1499}'

# Delete also invalidates the cache
curl -X DELETE http://localhost:8080/products/1
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `APP_ENV` | `development` | When `development`, runs `db.Migrate()` on startup; use `production` to skip |
| `POSTGRES_URL` | `postgres://app:app@localhost:5432/appdb?sslmode=disable` | Postgres connection string |
| `REDIS_CLUSTER_ADDRS` | `localhost:6379,localhost:6380,localhost:6381` | Comma-separated cluster node addresses |
| `SERVER_ADDR` | `:8080` | HTTP listen address |
| `PAGE_DEFAULT_LIMIT` | `20` | Default page size for list/search |
| `PAGE_DEFAULT_OFFSET` | `0` | Default offset for list/search |
| `PAGE_MAX_LIMIT` | `100` | Max allowed `limit` query param |
| `CACHE_LOCK_TTL` | `10s` | Redis lock auto-expire if loader crashes |
| `CACHE_LOCK_MAX_WAIT` | `3s` | Max time lock waiters poll cache before fallback DB load |
| `CACHE_LOCK_POLL_INTERVAL` | `50ms` | Poll interval while waiting for cache populate |

Cache TTL is 5 minutes (set in `internal/config/config.go`). TTL controls freshness; Redis LRU evicts under memory pressure independently of TTL.

Production example:

```bash
APP_ENV=production go run .
```
