# Go Cache-Aside Sample

A minimal Go REST API demonstrating the **cache-aside** pattern with Redis and PostgreSQL.

## Architecture

```
GET /products/{id}
  1. Check Redis for key `product:{id}`
  2. On hit  → return cached JSON
  3. On miss → read from PostgreSQL, write to Redis, return

PUT/DELETE /products/{id}
  1. Write to PostgreSQL
  2. Invalidate Redis key `product:{id}`
```

## Prerequisites

- Go 1.22+
- Docker (for Postgres and Redis Cluster)

## Quick Start

```bash
# Start Postgres and Redis Cluster (3 masters on 6379, 6380, 6381)
docker compose up -d

# Run the app
go run .

# Create a product
curl -X POST http://localhost:8080/products \
  -H "Content-Type: application/json" \
  -d '{"name":"Widget","price":9.99}'

# First GET hits Postgres and caches the result
curl http://localhost:8080/products/1

# Subsequent GETs are served from Redis
curl http://localhost:8080/products/1

# Update invalidates the cache
curl -X PUT http://localhost:8080/products/1 \
  -H "Content-Type: application/json" \
  -d '{"name":"Super Widget","price":14.99}'

# Delete also invalidates the cache
curl -X DELETE http://localhost:8080/products/1
```

## Configuration

| Variable       | Default                                              |
|----------------|------------------------------------------------------|
| `POSTGRES_URL` | `postgres://app:app@localhost:5432/appdb?sslmode=disable` |
| `REDIS_CLUSTER_ADDRS` | `localhost:6379,localhost:6380,localhost:6381` |
| `SERVER_ADDR`  | `:8080`                                              |

Cache TTL is 5 minutes (set in `internal/config/config.go`).
