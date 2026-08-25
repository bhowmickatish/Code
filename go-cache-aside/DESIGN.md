# Design Document: Go Cache-Aside Sample

## 1. Overview

This application is a minimal go application that demonstrates the **cache-aside** caching pattern. PostgreSQL is the system of record; Redis is an optional, lazy read cache for single-entity lookups by ID.

The design optimizes **repeat reads of the same product** while keeping list and search operations authoritative and complete by always querying Postgres.

### Goals

- Demonstrate cache-aside read path (cache first, DB on miss, populate cache).
- Keep Postgres as the single source of truth.
- Invalidate cache on write/delete so stale single-entity reads are bounded.
- Avoid cache dependency for list and search (partial cache cannot represent full datasets).
- Paginate list and search responses to limit payload size.



### Non-goals

- Full cache mirroring of all products.
- Caching list or search results.
- Database index DDL beyond minimal app startup schema (search indexes are ops/infrastructure).
- Authentication, rate limiting, or production hardening.

---



## 2. Architecture



### 2.1 High-level diagram

```
                    ┌─────────────────────────────────────┐
                    │           HTTP Client               │
                    └──────────────────┬──────────────────┘
                                       │ REST
                    ┌──────────────────▼──────────────────┐
                    │         ProductHandler                │
                    │  (routes, JSON, status codes)         │
                    └──────────────────┬──────────────────┘
                                       │
                    ┌──────────────────▼──────────────────┐
                    │       ProductRepository               │
                    │  (cache-aside logic, SQL, Redis)      │
                    └───┬──────────────────────────┬────────┘
                        │                          │
           list/search  │                          │ get-by-id (cache-aside)
           create       │                          │
                        ▼                          ▼
              ┌─────────────────┐        ┌─────────────────┐
              │   PostgreSQL    │        │      Redis      │
              │ (source of truth) │        │  (read cache)   │
              └─────────────────┘        └─────────────────┘
```



### 2.2 Layer responsibilities


| Layer      | Package               | Responsibility                                                     |
| ---------- | --------------------- | ------------------------------------------------------------------ |
| Entry      | `main`                | Wire config, DB pool, Redis client, HTTP server, graceful shutdown |
| Config     | `internal/config`     | Environment-backed settings (URLs, TTL, pagination defaults)       |
| Model      | `internal/model`      | Domain structs (`Product`, `ProductPage`)                          |
| DB         | `internal/db`         | Postgres connection pool; dev-only schema migration                |
| Cache      | `internal/cache`      | Redis Cluster client (`NewClusterClient`)                          |
| Repository | `internal/repository` | Data access + cache-aside orchestration (bridge layer)             |
| Handler    | `internal/handler`    | REST API surface                                                   |


The **repository** is the bridge between REST handlers and storage. Handlers never call Postgres or Redis directly. It owns:

- When to read Redis vs Postgres.
- SQL for list, search, and CRUD.
- Cache key format, TTL on populate, and invalidation on write/delete.

---



## 3. Cache-Aside Pattern



### 3.1 Definition

In cache-aside, the application (not the database) manages the cache:

1. **Read:** Check cache → on miss, read from DB → write to cache → return.
2. **Write:** Update DB → invalidate (delete) the affected cache entry.
3. **Create:** Write to DB only; store idempotency record in Redis; cache is populated lazily on first read by ID.

The cache is a **partial, lazy copy** of Postgres. Only products that have been fetched via `GET /products/{id}` may exist in Redis.

### 3.2 Read by ID (`GetByID`)

Cache misses use a two-gate stampede mitigation path before hitting Postgres:

1. **Gate 1 —** `singleflight` **(per process):** one in-flight load per product ID per instance.
2. **Gate 2 — Redis lock (cross-instance):** `SET lock:product:{id} NX EX`; one loader globally; waiters poll cache.

```
GET /products/{id}
        │
        ▼
   Redis GET product:{id}
        │
   ┌────┴────┐
   │ hit     │ miss (or corrupt JSON)
   ▼         ▼
 return    Gate 1: singleflight.Do (per instance)
            │
            ▼
       Gate 2: SET lock:product:{id} NX
            │
     ┌──────┴──────┐
     │ acquired    │ not acquired
     ▼             ▼
  double-check   poll GET product:{id}
  cache          until populated or timeout
     │             │
     ▼             ▼
  Postgres       return cached (or fallback load)
  SELECT
     │
     ▼
  Redis SET product:{id} (TTL = 5 min)
  DEL lock:product:{id}
     │
     ▼
  return
```

Lock/wait failures fail open: load from Postgres without blocking the request.

### 3.3 Write paths


| Operation | Postgres                        | Redis                                      |
| --------- | ------------------------------- | ------------------------------------------ |
| `Create`  | `INSERT` (server-assigned `id`) | `SET idempotency:{fingerprint}` — see §3.6 |
| `Update`  | `UPDATE`                        | `DEL product:{id}`                         |
| `Delete`  | `DELETE`                        | `DEL product:{id}`                         |




### 3.4 Cache failure policy (fail-open)

Postgres is the source of truth. Redis failures must not fail successful DB operations:


| Operation                                        | Redis failure                  | HTTP result   |
| ------------------------------------------------ | ------------------------------ | ------------- |
| `GetByID` — cache `SET` after DB read            | Logged; product still returned | `200`         |
| `Update` / `Delete` — cache `DEL` after DB write | Logged; DB change stands       | `200` / `204` |
| `GetByID` — cache `GET` error (not `Nil`)        | Propagated                     | `500`         |
| `Create` — idempotency `SET` after DB insert     | Logged; product still returned | `201`         |


Stale cache entries are bounded by TTL if invalidation fails.

### 3.5 Why list/search skip the cache


| Concern          | Reason to use Postgres only                                                      |
| ---------------- | -------------------------------------------------------------------------------- |
| Completeness     | Cache only holds previously queried IDs                                          |
| New products     | Visible in list immediately after `POST`, but not cached until first `GET` by ID |
| Search (`ILIKE`) | Redis has no equivalent indexed query for name substring search                  |
| Eviction         | TTL or `maxmemory` LRU can remove keys; list must not depend on cache presence   |
| Pagination       | `total` count must reflect full dataset or full match set in Postgres            |




### 3.6 POST idempotency (Create)

`POST /products` is idempotent without any client header. The application handles deduplication internally.

#### Assumptions


| Input                  | Role                                                                                                         |
| ---------------------- | ------------------------------------------------------------------------------------------------------------ |
| `name` + `price_cents` | Logical create identity — idempotency fingerprint input                                                      |
| `id`                   | **Server-generated** (`BIGSERIAL`) on first insert; returned in `201`; not known to the client before create |


Because `id` does not exist until after the first successful insert, it cannot be the idempotency **key** for retries. The key is derived from the request body instead.

#### Fingerprint and Redis record

```
fingerprint = SHA-256(name + "\x00" + price_cents)
Redis key   = idempotency:{hex(fingerprint)}
Redis value = JSON { name, price_cents, product }   // product includes server-assigned id
TTL         = IDEMPOTENCY_TTL (default 24h)
```

Implementation: `internal/repository/idempotency.go` (`idempotencyFingerprint`).

#### Request flow

```
POST /products  {"name", "price_cents"}
        │
        ▼
   Redis GET idempotency:{fingerprint}
        │
   ┌────┴────┐
   │ hit     │ miss
   ▼         ▼
 return    Redis lock on idempotency:{fingerprint}
 stored       │
 product      ├─ lock acquired → Postgres INSERT → SET idempotency record → 201
 (201)        └─ lock not acquired → poll Redis until record appears (or fallback insert)
```

Concurrent identical POSTs across instances use the same **Redis lock + poll** pattern as cache stampede mitigation (§3.2), so only one Postgres `INSERT` runs per fingerprint.

#### Client contract

- **No** `Idempotency-Key` **header** — client API unchanged.
- **Retry safety:** repeat the same JSON body → same `201` response with the same server-assigned `id`.
- **Different body:** different `name` or `price_cents` → different fingerprint → new product.



#### Trade-offs


| Benefit                                         | Cost                                                                                                                   |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------- |
| Safe retries on network timeout / client replay | Two **intentional** creates with identical `name` + `price_cents` within `IDEMPOTENCY_TTL` return the **same** product |
| Works with server-generated IDs                 | After TTL expires, an identical body creates a **new** row (new `id`)                                                  |
| No client idempotency plumbing                  | Natural-key dedup — not suitable if duplicate name/price products are valid business entities                          |


`PUT` remains idempotent by HTTP semantics (same resource state on repeat). `POST` idempotency here is **application-level dedup** keyed on create inputs, not on product `id`.

---



## 4. Data Model



### 4.1 Domain entities

```go
type Product struct {
    ID         int64
    Name       string
    PriceCents int64     // stored as integer cents to avoid floating-point money errors
    CreatedAt  time.Time
}

type ProductPage struct {
    Items  []Product
    Total  int       // full row count (list) or match count (search)
    Limit  int       // page size applied
    Offset int       // rows skipped
}
```



### 4.2 Postgres schema (development migration)

The app runs `db.Migrate()` on startup **only when** `IsDevelopmentMode` **is true** (`APP_ENV=development`, the default). Production sets `APP_ENV=production` and applies DDL via a dedicated migration job.

Migrate also upgrades legacy `price DOUBLE PRECISION` columns to `price_cents` when present.

```sql
CREATE TABLE products (
    id         BIGSERIAL PRIMARY KEY,
    name       TEXT NOT NULL,
    price_cents BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```



### 4.3 Redis key format


| Key            | Value                     | TTL                    |
| -------------- | ------------------------- | ---------------------- |
| `product:{id}` | JSON-serialized `Product` | 5 minutes (`CacheTTL`) |


Example: `product:42` → `{"id":42,"name":"Widget","price_cents":999,"created_at":"..."}`

---



## 5. API Design

Base URL: `http://localhost:8080` (configurable via `SERVER_ADDR`).

### 5.1 Endpoints


| Method   | Path                 | Data source                | Cache behavior                         |
| -------- | -------------------- | -------------------------- | -------------------------------------- |
| `GET`    | `/products`          | Postgres                   | None                                   |
| `GET`    | `/products?q={term}` | Postgres (`ILIKE` on name) | None                                   |
| `POST`   | `/products`          | Postgres                   | `SET idempotency:{fingerprint}` (§3.6) |
| `GET`    | `/products/{id}`     | Cache-aside                | Read-through on miss                   |
| `PUT`    | `/products/{id}`     | Postgres                   | Invalidate key                         |
| `DELETE` | `/products/{id}`     | Postgres                   | Invalidate key                         |


List and search share `GET /products`. When `q` is present and non-empty, the handler routes to `Search`; otherwise `List`.

### 5.2 Pagination

Query parameters (both list and search):


| Param    | Default                   | Max / constraint                 | Source                  |
| -------- | ------------------------- | -------------------------------- | ----------------------- |
| `limit`  | `PAGE_DEFAULT_LIMIT` (20) | Capped at `PAGE_MAX_LIMIT` (100) | Config + query override |
| `offset` | `PAGE_DEFAULT_OFFSET` (0) | Must be ≥ 0                      | Config + query override |


If `limit` or `offset` query params are invalid, the API returns `400 Bad Request`.

**Important:** Pagination limits **response size**, not necessarily **query cost**. Postgres still finds matching rows before applying `LIMIT`/`OFFSET`. For search, index optimization is a database concern (§7).

Response wrapper (`ProductPage`):

```json
{
  "items": [
    {"id": 1, "name": "Widget", "price_cents": 999, "created_at": "..."}
  ],
  "total": 42,
  "limit": 20,
  "offset": 0
}
```

- `total` for list: `COUNT(*)` on `products`.
- `total` for search: `COUNT(*)` where `name ILIKE '%term%'`.
- Empty pages return `"items": []` (never `null`).
- Clients can compute pages: `next_offset = offset + limit`, `has_more = offset + len(items) < total`.



### 5.3 Request / response examples

**Create**

See §3.6 for full idempotency design. Summary: identical `name` + `price_cents` within `IDEMPOTENCY_TTL` replays the same server-generated product; no client header required.

```http
POST /products
Content-Type: application/json

{"name": "Widget", "price_cents": 999}
```

```http
201 Created

{"id": 1, "name": "Widget", "price_cents": 999, "created_at": "..."}
```

**List (Postgres, paginated)**

```http
GET /products
GET /products?limit=10&offset=20
```

```http
200 OK

{
  "items": [...],
  "total": 42,
  "limit": 10,
  "offset": 20
}
```

**Search (Postgres, paginated)**

```http
GET /products?q=wid
GET /products?q=widget&limit=5&offset=0
```

**Get by ID (cache-aside)**

```http
GET /products/1
```

First request: Postgres + cache populate. Subsequent requests within TTL: Redis.

**Update (invalidate cache)**

```http
PUT /products/1
Content-Type: application/json

{"name": "Super Widget", "price_cents": 1499}
```

**Delete (invalidate cache)**

```http
DELETE /products/1
```

```http
204 No Content
```



### 5.4 Error responses


| Condition                                     | Status                      |
| --------------------------------------------- | --------------------------- |
| Invalid JSON, unknown fields, or body > 1 MiB | `400 Bad Request`           |
| Invalid `name` / `price_cents`                | `400 Bad Request`           |
| Invalid product ID in path                    | `400 Bad Request`           |
| Invalid `limit` or `offset`                   | `400 Bad Request`           |
| Product not found (`repository.ErrNotFound`)  | `404 Not Found`             |
| Unsupported HTTP method                       | `405 Method Not Allowed`    |
| DB read/write errors                          | `500 Internal Server Error` |
| Redis read errors (not cache miss)            | `500 Internal Server Error` |


Not-found detection uses `errors.Is(err, repository.ErrNotFound)` — not string matching.

### 5.5 Request validation

- POST/PUT bodies: max **1 MiB** (`http.MaxBytesReader`), `DisallowUnknownFields()`.
- `price_cents` must be a positive integer (no floats — avoids money rounding issues).

---



## 6. Search Design



### 6.1 Current implementation (application)

Repository SQL (`internal/repository/product.go`):

```sql
-- count
SELECT COUNT(*) FROM products WHERE name ILIKE '%' || $1 || '%'

-- rows
SELECT id, name, price_cents, created_at FROM products
WHERE name ILIKE '%' || $1 || '%'
ORDER BY id
LIMIT $2 OFFSET $3
```

Properties:

- Substring, case-insensitive match on `name`.
- Two round-trips per request (count + select).
- Always Postgres; never Redis.
- Paginated via shared `limit`/`offset` handling in the handler.



### 6.2 Cost characteristics


| Factor                          | Impact                                                |
| ------------------------------- | ----------------------------------------------------- |
| Leading `%` in `ILIKE '%term%'` | B-tree index on `name` cannot be used                 |
| `ILIKE`                         | Case-insensitive comparison on candidate rows         |
| `COUNT(*)` + `SELECT`           | Two queries per search request                        |
| Large match sets                | Postgres must identify matches before `LIMIT` applies |


Pagination reduces bytes over the wire but does **not** eliminate full scan cost without a supporting database index.

### 6.3 Search index strategy (database side — out of app scope)

**Chosen approach: Option A — trigram GIN index (**`pg_trgm`**)**

Fits the current `ILIKE '%…%'` query shape. No application code changes required once the index exists.

Reference DDL (applied by ops / separate migration tooling, **not** this app):

```sql
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE INDEX IF NOT EXISTS products_name_trgm_idx
  ON products USING GIN (name gin_trgm_ops);
```


| Approach                       | Index                     | App SQL change             | Fits current API                |
| ------------------------------ | ------------------------- | -------------------------- | ------------------------------- |
| **A: Trigram GIN** (`pg_trgm`) | `GIN (name gin_trgm_ops)` | None                       | Yes — keeps `ILIKE '%term%'`    |
| B: Full-text (`tsvector`)      | `GIN (to_tsvector(...))`  | Yes — `@@ plainto_tsquery` | No — word search, not substring |
| C: Prefix only                 | B-tree on `name`          | Yes — `ILIKE term%`        | No — prefix only                |


**Scope decision:** DDL and search indexes are beyond this application's scope. The app issues queries; Postgres uses indexes when present. Until `pg_trgm` is applied externally, search remains functionally correct but may scan sequentially at scale.

### 6.4 Alternatives not used


| Approach                        | Why not                                                    |
| ------------------------------- | ---------------------------------------------------------- |
| Cache search results in Redis   | Stale/partial results; list/search must stay authoritative |
| Full-text search in app         | Indexing belongs on the database                           |
| External search (Elasticsearch) | Out of scope for this sample                               |


---



## 7. DDL & Migration Scope


| DDL                         | Owned by                     | Notes                                  |
| --------------------------- | ---------------------------- | -------------------------------------- |
| `CREATE TABLE products`     | App when `IsDevelopmentMode` | Minimal demo schema via `db.Migrate()` |
| `pg_trgm` extension         | Ops / external migrations    | Search performance                     |
| GIN trigram index on `name` | Ops / external migrations    | Pairs with current `ILIKE` search      |
| Redis `maxmemory` policy    | Docker / ops                 | Optional capacity eviction             |


The application does **not** manage production index lifecycle, backfills, or extension installs beyond the base table.

---



## 8. Cache Expiration & Eviction

Two independent mechanisms affect Redis entries:

### 8.1 Per-key TTL (application)

- Set via `cache.Set(key, data, CacheTTL)` on cache miss in `GetByID`.
- Default: **5 minutes** (`internal/config/config.go`).
- Purpose: bound staleness if a write path fails to invalidate (defense in depth).



### 8.2 Redis memory eviction (server, optional)

- Not configured in default `docker-compose.yml`.
- To enable LRU under memory pressure, set Redis `maxmemory` and `maxmemory-policy` (e.g. `volatile-lru` for keys with TTL).
- Evicted keys cause a cache miss on the next `GET /products/{id}`; Postgres repopulates the cache.

TTL expiration and LRU eviction are complementary: TTL controls time-based freshness; LRU controls capacity.

---



## 9. Configuration


| Variable                   | Default                                                   | Description                                                                    |
| -------------------------- | --------------------------------------------------------- | ------------------------------------------------------------------------------ |
| `APP_ENV`                  | `development`                                             | Drives `IsDevelopmentMode`; when `development`, runs `db.Migrate()` on startup |
| `POSTGRES_URL`             | `postgres://app:app@localhost:5432/appdb?sslmode=disable` | Postgres connection string                                                     |
| `REDIS_CLUSTER_ADDRS`      | `localhost:6379,localhost:6380,localhost:6381`            | Comma-separated Redis Cluster node addresses                                   |
| `SERVER_ADDR`              | `:8080`                                                   | HTTP listen address                                                            |
| `CacheTTL`                 | `5m` (code default)                                       | Redis key TTL on cache populate                                                |
| `CACHE_LOCK_TTL`           | `10s`                                                     | Redis lock auto-expire if loader crashes                                       |
| `CACHE_LOCK_MAX_WAIT`      | `3s`                                                      | Max time lock waiters poll cache before fallback load                          |
| `CACHE_LOCK_POLL_INTERVAL` | `50ms`                                                    | Poll interval while waiting for cache populate                                 |
| `IDEMPOTENCY_TTL`          | `24h`                                                     | Redis deduplication window for identical POST `name` + `price_cents` (§3.6)    |
| `PAGE_DEFAULT_LIMIT`       | `20`                                                      | Default page size for list/search                                              |
| `PAGE_DEFAULT_OFFSET`      | `0`                                                       | Default offset for list/search                                                 |
| `PAGE_MAX_LIMIT`           | `100`                                                     | Maximum allowed `limit` query param                                            |


Pagination defaults are loaded in config and passed into `ProductHandler`. Query params override defaults when valid.

---



## 10. Infrastructure



### 10.1 Runtime components


| Service       | Image                        | Ports            |
| ------------- | ---------------------------- | ---------------- |
| Postgres 16   | `postgres:16-alpine`         | 5432             |
| Redis Cluster | `redis:7-alpine` × 3 masters | 6379, 6380, 6381 |
| Go app        | local build (Go 1.26+)       | 8080             |


Local Docker Compose runs a **3-master Redis Cluster** (no replicas). Nodes use `cluster-announce-ip` / `cluster-announce-port` so the app on the host can route via slot-aware client. A one-shot `redis-cluster-init` container forms the cluster on first start.

The application uses `redis.NewClusterClient` (`go-redis`). The client computes key slots and routes `GET` / `SET` / `DEL` to the correct node; repository code only passes key names (e.g. `product:42`).

Start dependencies:

```bash
docker compose up -d
go run .
```



### 10.2 Dependencies (Go)


| Package                        | Role                                |
| ------------------------------ | ----------------------------------- |
| `github.com/jackc/pgx/v5`      | Postgres driver and connection pool |
| `github.com/redis/go-redis/v9` | Redis client                        |


---



## 11. Request Flow Examples



### 11.1 Repeated read by ID

1. `GET /products/1` — Redis miss → Postgres → Redis SET (TTL 5m) → `200`.
2. `GET /products/1` — Redis hit → `200` (no Postgres query).



### 11.2 Create then list vs get

1. `POST /products` — see §3.6: fingerprint `name` + `price_cents` → Redis idempotency check → Postgres INSERT on first use → `201`. Product read cache unchanged.
2. `GET /products` — Postgres SELECT (paginated) → product appears in list.
3. `GET /products/1` — Redis miss → Postgres → cache populate.



### 11.3 Update invalidates cache

1. `GET /products/1` — cached in Redis.
2. `PUT /products/1` — Postgres UPDATE → Redis DEL `product:1`.
3. `GET /products/1` — Redis miss → fresh read from Postgres → re-cache.



### 11.4 Paginated search

1. `GET /products?q=widget&limit=10&offset=0` — Postgres COUNT + SELECT (no Redis).
2. `GET /products?q=widget&limit=10&offset=10` — next page from Postgres.
3. `total` in response reflects all name matches, not just the current page.

---



## 12. Horizontal Scaling (Multiple App Instances)

The application supports running **multiple instances** behind a load balancer. No code changes are required to scale horizontally.

### 12.1 Architecture

```
                    Load balancer
                         │
        ┌────────────────┼────────────────┐
        ▼                ▼                ▼
    App instance 1   App instance 2   App instance 3
    (ClusterClient)  (ClusterClient)  (ClusterClient)
        │                │                │
        └────────────────┼────────────────┘
                         │
              ┌──────────┴──────────┐
              ▼                     ▼
         PostgreSQL          Redis Cluster
         (source of truth)   (shared cache)
```

Each instance is independent: own HTTP server, Postgres connection pool, and `ClusterClient` (with its own slot→node topology cache).

### 12.2 Why multi-instance works


| Layer         | Behavior                                                      |
| ------------- | ------------------------------------------------------------- |
| HTTP handlers | Stateless — no in-memory session or per-request state         |
| Postgres      | Shared system of record; each instance uses its own pool      |
| Redis Cluster | Shared external cache; all instances read/write the same keys |
| Cache-aside   | No in-process cache — coherence comes from shared Redis       |



| Scenario                             | Cross-instance behavior                                        |
| ------------------------------------ | -------------------------------------------------------------- |
| Instance A caches `product:1`        | Instance B `GET /products/1` → Redis hit                       |
| Instance B `PUT /products/1` → `DEL` | Instance A next read → miss → Postgres → re-cache              |
| Instance C `POST /products`          | Visible in list/search on all instances immediately (Postgres) |


Sticky sessions are **not** required.

### 12.3 Per-instance requirements

All instances must share:


| Setting               | Notes                                                          |
| --------------------- | -------------------------------------------------------------- |
| `POSTGRES_URL`        | Same database                                                  |
| `REDIS_CLUSTER_ADDRS` | Same Redis Cluster                                             |
| Config defaults       | Consistent `PAGE_*` and `CacheTTL` unless intentionally varied |


Each instance may use its own `SERVER_ADDR` (e.g. `:8080` per container/pod).

### 12.4 Redis Cluster client per instance

Each process creates one `ClusterClient` via `cache.NewClusterClient`. Each client maintains its own slot→node topology map in memory. That is expected: topology is fetched from the cluster, not shared across processes.

Repository code is unchanged — keys only (`product:{id}`); routing is internal to each client.

### 12.5 Operational considerations


| Topic              | Note                                                                                                                                            |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| Startup migrations | `db.Migrate()` runs only when `IsDevelopmentMode` (`APP_ENV=development`). Production uses external migration tooling and `APP_ENV=production`. |
| Connection limits  | N instances × pool size = total Postgres and Redis connections — size pools accordingly.                                                        |
| Cache stampede     | Mitigated via `singleflight` (per instance) + Redis lock on miss (cross-instance); see §3.2.                                                    |
| Load balancer      | Any HTTP LB (round-robin, least-conn) works; no session affinity needed.                                                                        |


---



## 13. Design Decisions & Trade-offs


| Decision                                    | Rationale                                                                                       |
| ------------------------------------------- | ----------------------------------------------------------------------------------------------- |
| Repository layer                            | Centralizes cache-aside rules; handlers stay HTTP-only                                          |
| Cache only `GetByID`                        | Hot-key optimization without mirroring full table                                               |
| List/search from Postgres                   | Correctness; cache is incomplete and unindexed for text queries                                 |
| Paginated list and search                   | Limits payload size; `total` enables client-side paging                                         |
| Pagination defaults in config               | Consistent defaults across environments; overridable per request                                |
| Invalidate on write, not write-through      | Simpler; next read repopulates cache                                                            |
| JSON in Redis                               | Human-readable, matches API response shape                                                      |
| No cache on create                          | Lazy population avoids caching entities never read                                              |
| POST idempotency via body fingerprint       | Server-generated `id`; dedup keyed on `name` + `price_cents` in Redis (§3.6) — no client header |
| `ILIKE` search in Postgres                  | Simple substring search for demo API                                                            |
| Search indexes outside app                  | DDL/index lifecycle is infrastructure, not application code                                     |
| Integer `price_cents`                       | Exact money storage; no float drift in DB, cache, or API                                        |
| Cache fail-open on Redis write errors       | Postgres result returned; TTL bounds staleness if invalidation fails                            |
| `repository.ErrNotFound` sentinel           | Reliable 404 mapping in handlers                                                                |
| Dev-only auto-migrate (`IsDevelopmentMode`) | Local demo convenience; production DDL stays external                                           |
| Shared Redis for all app instances          | Enables horizontal scale without per-process cache coherence issues                             |




### Known limitations

- Search can be expensive at scale without external `pg_trgm` index.
- Pagination does not reduce search scan cost without database indexes.
- Lock wait timeout can cause a rare duplicate Postgres read (fail-open fallback).
- POST idempotency treats identical `name` + `price_cents` as one logical create within `IDEMPOTENCY_TTL` (§3.6).
- No structured metrics/tracing (cache/Redis failures are log-only).
- `OFFSET` pagination degrades for very large offsets (keyset pagination not implemented).
- Invalid `PAGE_*` env vars fail at startup; other misconfig may still go unnoticed.

---



## 14. Future Extensions

- Interface-based repository for unit tests with mocks.
- Proactive TTL refresh for a predefined hot-key set (optional second-layer optimization).
- Health endpoints (`/health`, `/ready`) checking Postgres and Redis.
- OpenTelemetry traces across handler → repository → stores.
- Keyset (cursor) pagination instead of `OFFSET` for large tables.
- Minimum query length for search to reject trivial `q` values.
- Full-text search (`tsvector`) if API moves to word-based search (requires SQL + index changes).

