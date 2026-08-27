# Go Cache-Aside Sample

A minimal Go application demonstrating the **cache-aside** pattern with Redis Cluster and PostgreSQL.

See [DESIGN.md](./DESIGN.md) for full architecture and design decisions.

## Architecture

```
GET /products/{id}
  1. Check Redis for key `{product:id}:data`
  2. On hit  → return cached JSON
  3. On miss → two-gate stampede mitigation, then load and cache:
       Gate 1 (singleflight): one in-flight load per product ID per instance
       Gate 2 (Redis lock):   SET `{product:id}:lock` NX — one loader globally;
                              waiters BRPOP `{product:id}:notify` until populated (see DESIGN.md §3.2)
     → read from PostgreSQL, atomic SET data + LPUSH notify (Lua), return

GET /products / GET /products?q=...
  → Always PostgreSQL (paginated; never cached)

POST /products
  1. Fingerprint request body: SHA-256(name + price_cents) → Redis key idempotency:{hash}
  2. On hit  → replay stored product (same server-assigned id) — safe to retry
  3. On miss → Redis lock → Postgres INSERT → store idempotency record (TTL 24h) → 201
  → Product `id` is server-generated (BIGSERIAL); not used as idempotency key (see DESIGN.md §3.6)

PUT /products/{id}
  → Postgres UPDATE → invalidate Redis `{product:id}:data` (+ notify list)

DELETE /products/{id}
  → Postgres DELETE → invalidate Redis `{product:id}:data` (+ notify list)
```

Redis in `docker-compose.yml` runs a **3-master cluster with one replica per master** (6 containers). Masters are on host ports **6379–6381**; replicas on **6382–6384**. All nodes use AOF persistence, `maxmemory 256mb`, and `volatile-lru` eviction (TTL keys only). The app connects via the three **master** addresses; `go-redis` discovers replicas and handles failover topology automatically.

| Host port | Role    |
| --------- | ------- |
| 6379      | master  |
| 6380      | master  |
| 6381      | master  |
| 6382      | replica |
| 6383      | replica |
| 6384      | replica |

If you previously ran the old 3-node (no-replica) stack, reset volumes before re-forming the cluster:

```bash
docker compose down -v
docker compose up -d
```

Verify topology:

```bash
redis-cli -p 6379 cluster nodes
```

### POST idempotency (summary)


| Topic        | Behavior                                                       |
| ------------ | -------------------------------------------------------------- |
| Client input | `{ "name", "price_cents" }` only — no `Idempotency-Key` header |
| Dedup key    | `name` + `price_cents` fingerprint (not product `id`)          |
| Retry        | Same JSON within `IDEMPOTENCY_TTL` → same `201` and same `id`  |
| Trade-off    | Identical name + price within 24h collapses to one product     |


Full detail: [DESIGN.md §3.6](./DESIGN.md#36-post-idempotency-create).

## Prerequisites

- Go 1.26+
- Docker (Postgres + Redis Cluster)



## Quick Start

```bash
# Start Postgres and Redis Cluster (see port table above)
docker compose up -d

# Run the app (APP_ENV=development by default — runs schema migrate)
go run .

# Create a product (price_cents: 999 = $9.99); safe to retry the same request
curl -X POST http://localhost:8080/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Widget","price_cents":999}'

# Retry with the same body returns the same product id (no duplicate row)
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


| Variable                   | Default                                                   | Description                                                                  |
| -------------------------- | --------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `APP_ENV`                  | `development`                                             | When `development`, runs `db.Migrate()` on startup; use `production` to skip |
| `POSTGRES_URL`             | `postgres://app:app@localhost:5432/appdb?sslmode=disable` | Postgres connection string                                                   |
| `REDIS_CLUSTER_ADDRS`      | `localhost:6379,localhost:6380,localhost:6381`            | Comma-separated cluster master addresses (client bootstraps full topology)   |
| `SERVER_ADDR`              | `:8080`                                                   | HTTP listen address                                                          |
| `PAGE_DEFAULT_LIMIT`       | `20`                                                      | Default page size for list/search                                            |
| `PAGE_DEFAULT_OFFSET`      | `0`                                                       | Default offset for list/search                                               |
| `PAGE_MAX_LIMIT`           | `100`                                                     | Max allowed `limit` query param                                              |
| `CACHE_LOCK_TTL`           | `10s`                                                     | Redis lock auto-expire if loader crashes                                     |
| `CACHE_LOCK_MAX_WAIT`      | `3s`                                                      | Max time lock waiters block on BRPOP before fallback DB load                   |
| `IDEMPOTENCY_TTL`          | `24h`                                                     | Dedup window for identical POST `name` + `price_cents` (§3.6 in DESIGN.md)   |


Cache TTL is 5 minutes (set in `internal/config/config.go`). TTL controls freshness; Redis LRU evicts under memory pressure independently of TTL.

Production example:

```bash
APP_ENV=production go run .
```

