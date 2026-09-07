# Go ZooKeeper Rate Limit — Design

See [README.md](./README.md) for quick start. This document covers architecture, per-component behavior, trade-offs, and production notes.

## Overview

A sample service that loads **rate-limit rules from ZooKeeper**, applies **leaky-bucket** limiting per route and client, and hot-reloads when the ZK node changes. Rules are validated at load time; invalid documents are rejected without partially applying them.

## Architecture

```
main
  ├── config.Instance()          env-based settings (singleton)
  ├── loadRules()                ZK connect + Loader.LoadOnStartup (or file fallback)
  ├── ratelimit.Instance().Update(doc)   sync before HTTP listens
  ├── reloadstatus.RecordRulesApply("startup", nil)
  ├── loader.Watch(onReload)     background hot-reload goroutine
  └── handler.RateLimitHandler()
        ├── /health              no rate limit
        └── /api/* + middleware  → ratelimit.Instance().Allow
```

### Startup sequence

1. Load config from environment (`config.Instance()` — singleton, parsed once).
2. Connect to ZooKeeper with `ZK_SESSION_TIMEOUT` as the startup load deadline.
3. If ZK connect fails → read `SEED_RULES_FILE` directly; no watch goroutine.
4. If ZK connect succeeds → `Loader.LoadOnStartup` (optional bootstrap in development).
5. `ratelimit.Instance().Update(doc)` runs **synchronously** before `ListenAndServe` (no startup race).
6. Start `loader.Watch` in a goroutine; each reload calls `Instance().Update`.
7. Serve HTTP until SIGINT/SIGTERM; graceful shutdown with 5s timeout.

### Request path

1. `/health` → handler only; middleware not applied.
2. `/api/*` → `RateLimitMiddleware` → `Instance().Allow(r)`.
3. Longest matching `path_prefix` wins; same-length prefixes tie-break by rule `name`.
4. Resolve client key (`ip`, `user`, or `global`) → LRU cache lookup → `Take()` (blocks if over limit).
5. Unmatched paths return **403 Forbidden** (fail closed).
6. Matched requests include `X-RateLimit-Rule`.

---

## Components

### `main`

**Role:** Process lifecycle — config, rules bootstrap, limiter init, HTTP server, graceful shutdown.

| Function | Responsibility |
|----------|----------------|
| `main()` | Wire components; exit on config/rules errors; defer ZK close |
| `loadRules()` | Connect ZK or fall back to seed file; return `(doc, client, loader)` |

**Key behaviors:**

- Uses `signal.NotifyContext` for SIGINT/SIGTERM.
- ZK client is closed on exit when connect succeeded.
- Watch goroutine logs a warning if it stops for reasons other than context cancel.
- HTTP server: `ReadTimeout` 5s, `WriteTimeout` 10s, shutdown timeout 5s.
- When ZK is unavailable at startup, returns `(doc, nil, nil)` — service still starts with file rules.

---

### `internal/config`

**Role:** Centralize environment parsing into a typed `Config` struct. Loaded once via `Instance()` (`sync.Once`); subsequent calls return the same values.

**File:** `config.go`

| Field | Env var(s) | Default | Notes |
|-------|------------|---------|-------|
| `AppEnv` | `APP_ENV` | `development` | Drives bootstrap + ACL defaults |
| `IsDevelopmentMode` | _(derived)_ | `true` when `APP_ENV=development` | Enables ZK node seeding |
| `ZookeeperAddrs` | `ZOOKEEPER_ADDRS` | `localhost:2181` | Comma-separated |
| `RulesPath` | `ZK_RULES_PATH` | `/ratelimit/rules` | Znode for rules JSON |
| `ServerAddr` | `SERVER_ADDR` | `:8080` | HTTP listen address |
| `ZKSessionTimeout` | `ZK_SESSION_TIMEOUT` | `10s` | ZK session + startup load timeout |
| `SeedRulesFile` | `SEED_RULES_FILE` | `config/rules.json` | Bootstrap + fallback |
| `RateLimitCacheMax` | `RATELIMIT_CACHE_MAX` | `10000` | LRU capacity |
| `RateLimitUserHeader` | `RATELIMIT_USER_HEADER` | `X-User-ID` | Header for `key: "user"` |
| `TrustedProxy` | `TRUSTED_PROXY` | `false` | Trust `X-Forwarded-For` |
| `ZKOpenACL` | `ZK_OPEN_ACL` | `true` in development | `WorldACL(PermAll)` when true |
| `ZKDigest` | `ZK_DIGEST` | _(empty)_ | `user:password` for digest ACL |

**Design notes:**

- Boolean and duration env vars fail fast with descriptive errors on first load.
- `ZK_OPEN_ACL` defaults to open in development; production should set `ZK_OPEN_ACL=false` with `ZK_DIGEST`.

---

### `internal/model`

**Role:** Rules JSON schema, parsing, and validation.

**File:** `rule.go`

**Types:**

| Type | Purpose |
|------|---------|
| `RulesDocument` | Top-level JSON: `version`, `rules[]` |
| `RateLimit` | Single rule: `name`, `path_prefix`, `limit`, `window`, `key` |
| `KeyStrategy` | `ip`, `user`, or `global` |

**Validation (`ParseRulesDocument`):**

- `version` must be non-zero.
- At least one rule required.
- Each rule: non-empty `name` and `path_prefix`, positive `limit`, valid Go duration `window`, valid `key`.
- Rule names must be unique within the document.

**`WindowDuration()`:** Parses `window` (e.g. `"1s"`, `"100ms"`) via `time.ParseDuration`; must be positive.

Invalid JSON or validation errors propagate up; `Loader` and `Limiter.Update` never apply partial rule sets.

---

### `internal/zk` — Client

**Role:** Thin wrapper around `github.com/go-zookeeper/zk` for connect, CRUD, watch, and ACL.

**File:** `client.go`

| Method | Behavior |
|--------|----------|
| `Connect` | Dial ensemble; optional `AddAuth("digest", …)` when `ZK_DIGEST` set |
| `Get` / `Set` | Read/write znode data |
| `Exists` | Check path existence |
| `CreatePath` | Create intermediate paths; leaf gets payload; uses `createACL()` |
| `Watch` | `GetW` on rules path; returns node event channel |
| `Events` | Session lifecycle channel (`StateConnected`, `StateExpired`, etc.) |
| `Close` | Close connection |

**ACL policy (`createACL`):**

| Condition | ACL |
|-----------|-----|
| `openACL == true` | `WorldACL(PermAll)` |
| `digest` set, `openACL == false` | `DigestACL(PermAll, user, password)` |
| Otherwise | `WorldACL(PermRead \| PermWrite \| PermCreate \| PermDelete)` |

---

### `internal/zk` — Loader

**Role:** Load rules at startup, optional bootstrap, watch for changes, apply fallback on failure.

**File:** `loader.go`

**Types:**

| Type | Purpose |
|------|---------|
| `Loader` | Holds `client`, `rulesPath`, `seedFile` |
| `ReloadFunc` | `func(model.RulesDocument) error` — called on every successful reload |

**Methods:**

| Method | When | Behavior |
|--------|------|----------|
| `LoadOnStartup` | Startup | Optional bootstrap; load from ZK or fall back to seed file |
| `Watch` | Background | Outer loop registers watch; inner loop handles session + node events |
| `LoadRulesFromFile` | Fallback | Read + validate local JSON (also used when ZK down at startup) |

**Rules source of truth:**

| Priority | Source | When |
|----------|--------|------|
| 1 | ZooKeeper `ZK_RULES_PATH` | ZK reachable |
| 2 | `SEED_RULES_FILE` | ZK connect/load failure, node deleted, session expired, watch registration failure |

**Watch loop:**

```
for {
  register Watch(rulesPath)
  select {
    session event → handleSessionEvent → rewatch if true
    node event    → handleNodeEvent    → rewatch if true
    ctx.Done      → exit
  }
}
```

| Event | Action | Rewatch? |
|-------|--------|----------|
| `NodeDataChanged`, `NodeCreated` | Reload from ZK | No |
| `NodeDeleted` | Apply seed file fallback | Yes |
| `NotWatching` | Log + re-register | Yes |
| `StateExpired` | Apply seed file fallback | Yes |
| `StateDisconnected` | Log, wait for reconnect | No |
| `StateConnected` | Reload from ZK | No |

**Bootstrap (`bootstrapIfMissing`):** When `IsDevelopmentMode`, if rules znode is missing, validate seed file and `CreatePath` with its contents. Skipped in production (`APP_ENV != development`).

**Deploy requirement:** ship `config/rules.json` (or set `SEED_RULES_FILE`) so the process can start and recover when ZK is down.

---

### `internal/ratelimit` — Limiter

**Role:** Singleton rate limiter — compile rules, match requests, delegate to per-key buckets.

**File:** `limiter.go`

**Lifecycle:**

| API | Behavior |
|-----|----------|
| `Instance()` | `sync.Once` creates singleton; reads cache/proxy settings from config |
| `(l *Limiter) Update(doc)` | Validate, compile + sort rules; swap under `l.mu` |
| `(l *Limiter) Allow(r)` | Match rule, resolve key, `Take()`; returns `(ruleName, allowed)` |

**Concurrency:**

- `l.mu` (RWMutex) guards compiled rules snapshot during `Allow`.
- `entryCache` and each `limiterEntry` have their own mutexes.

**Rule matching (`matchRule`):**

- Rules sorted by descending `path_prefix` length, then ascending `name`.
- First prefix match via `strings.HasPrefix` wins.
- No match → `Allow` returns `allowed=false`; middleware responds **403 Forbidden**.
- No rules loaded → same fail-closed behavior.

**Client key resolution (`limiterKey`):**

| `key` | Cache key suffix |
|-------|------------------|
| `global` | `global` |
| `user` | `user:{header}` if header present; else `ip:{addr}` |
| `ip` | `ip:{addr}` |

Full cache key: `{ruleName}:{suffix}`.

**IP resolution:**

- Default: `RemoteAddr` host (strip port).
- `TRUSTED_PROXY=true`: first value in `X-Forwarded-For`.

**Algorithm:** `go.uber.org/ratelimit` leaky bucket with `WithoutSlack`. Over-limit requests **block** via `Take()` (no 429 response).

**Sample rules** (see `config/rules.json`):

| Rule | Path | Key | Effect |
|------|------|-----|--------|
| `api-default` | `/api/` | `ip` | Per-IP on all API routes |
| `users` | `/api/users` | `user` | Per-user (longer prefix wins over `api-default`) |
| `orders-global` | `/api/orders` | `global` | Shared bucket for all clients |

**Not stacked:** one rule applies per request (longest prefix). Global + per-user stacking is out of scope for this sample.

---

### `internal/ratelimit` — entryCache

**Role:** In-process LRU of per-client leaky buckets.

**File:** `cache.go`

| Concern | Implementation |
|---------|----------------|
| Structure | `map[key] → list.Element` + doubly-linked list for LRU order |
| Capacity | `RATELIMIT_CACHE_MAX` (default 10,000); evict LRU on insert |
| Reconfigure | `configure()` recreates uber limiter when `limit` or `window` changes |
| Thread safety | `entryCache.mu` for map/list; `limiterEntry.mu` for `Take()` |

**Placeholder for production:** replace with **Redis** (or similar) for cluster-wide limits. Each replica currently maintains independent counters; N instances ≈ N× effective rate.

---

### `internal/handler`

**Role:** HTTP routing and rate-limit middleware.

**Files:** `router.go`, `handler.go`

**Route table:**

| Route | Rate limited | Handler |
|-------|--------------|---------|
| `/health` | No | JSON with `rules_ok`, `zk_ok`; **503** only when rules apply failed |
| `/api`, `/api/users`, `/api/orders` | Yes | Sample API routes (`/api` and `/api/` both mounted) |

**Middleware (`RateLimitMiddleware`):**

- Wraps `/api` and `/api/` routes.
- Calls `ratelimit.Instance().Allow(r)`.
- Returns **403** with JSON error when no rule matches (fail closed).
- Sets `X-RateLimit-Rule` and calls `next` when a rule matched.
- Over-limit requests block inside `Take()`; they are not rejected with 429.

**Design choice:** `/health` registered on outer mux, outside middleware, so load balancers and orchestrators are never throttled. Returns **503** only when `rules_ok` is false (last apply failed). When ZooKeeper is flaky but fallback rules applied successfully, returns **200** with `zk_ok: false`.

---

### `internal/reloadstatus`

**Role:** Track rules apply and ZooKeeper health separately for observability and `/health`.

| API | Behavior |
|-----|----------|
| `RecordRulesApply(source, err)` | Last limiter apply outcome (`startup`, `zookeeper`, `fallback`, …) |
| `RecordZK(source, err)` | Last ZK rules read or session/watch **failure**; successful rules read marks healthy |
| `Snapshot()` | `{ rules_ok, zk_ok, rules, zk }` |

**Health semantics:**

| Signal | Meaning | 503 when false? |
|--------|---------|-----------------|
| `rules_ok` | Last `Update` succeeded | **Yes** |
| `zk_ok` | Last successful **rules read** from ZooKeeper | No — alert separately; watch-only success does not flip this true |

Used by `Loader`, startup in `main`, and `/health`. A failed ZK reload that keeps prior rules sets `zk_ok: false` but leaves `rules_ok: true`. Watch registration success alone does not set `zk_ok: true`.

### Health probes (Kubernetes / load balancers)

| Probe | Endpoint | Pass when |
|-------|----------|-----------|
| **Readiness** | `/health` | HTTP 200 (`rules_ok: true`) — process can enforce rate limits |
| **Liveness** | `/health` or a separate `/health/live` | Process responds (do **not** fail liveness on `zk_ok: false`) |
| **Alerting** | `/health` JSON | Page when `zk_ok: false` persists — ZK rules source is unhealthy even if fallback rules are active |

Using `/health` as a liveness probe will restart pods unnecessarily when ZooKeeper is down but fallback rules are working.

---

## Rate limiting (summary)

- **Algorithm:** leaky bucket (`go.uber.org/ratelimit`, `WithoutSlack`); blocks until allowed
- **Rule matching:** longest `path_prefix` wins; tie-break by rule `name`
- **Keys:** `ip`, `user` (via configurable header), `global`
- **Hot reload:** `Update` swaps compiled rules; existing cache entries reconfigure on next access

---

## ZooKeeper (summary)

- **Watch:** reload on data change; fallback on delete/session expiry; re-register on watch loss; reload on reconnect
- **ACLs:** open in development; digest in production via `ZK_DIGEST`
- **Session expiry:** records `zk_ok: false`, applies fallback, re-registers watch

---

## Health checks

`GET /health` returns:

- **200** when `rules_ok: true` (rate limits are applied), even if `zk_ok: false`
- **503** only when `rules_ok: false` (last rules apply failed)

Use as a **readiness** probe. Monitor `zk_ok` separately for ZooKeeper dependency alerts; do not use it as a liveness failure condition when fallback rules are acceptable.

---

## Logging

Uses `log/slog` with structured fields (`source`, `count`, `version`, `path`, `err`, rule details on load). Extend with JSON handler + trace IDs in production.

---

## Known limits (sample scope)

- No distributed rate limiting (see Redis note on `entryCache`)
- No metrics/tracing hooks
- No 429 / max-wait timeout — requests block indefinitely under sustained overload
- Rules not stacked (single winner per request)
- Digest ACL requires ZK user setup matching `ZK_DIGEST`