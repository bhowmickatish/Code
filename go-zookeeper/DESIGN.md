# Go ZooKeeper Rate Limit — Design

See [README.md](./README.md) for quick start. This document covers architecture, trade-offs, and production notes.

## Overview

A sample service that loads **rate-limit rules from ZooKeeper**, applies **leaky-bucket** limiting per route and client, and hot-reloads when the ZK node changes.

## Components

```
main
  ├── config          env-based settings
  ├── zk.Loader       rules from ZK + fallback file + watch
  ├── ratelimit.Init  singleton limiter (one per process)
  └── handler.RateLimitHandler
        ├── /health           (no rate limit)
        └── /api/* + middleware → ratelimit.Instance()
```

## Rules source of truth

| Priority | Source | When |
|----------|--------|------|
| 1 | ZooKeeper `/ratelimit/rules` | ZK reachable |
| 2 | `SEED_RULES_FILE` (`config/rules.json`) | ZK connect/load failure, node deleted, session expired |

**Deploy requirement:** ship `config/rules.json` (or set `SEED_RULES_FILE`) so the process can start when ZK is down.

## Rate limiting

- **Algorithm:** leaky bucket (WithoutSlack), equivalent to `go.uber.org/ratelimit`
- **Single state machine** per bucket (`nextIssue`) — peek and commit share state (no mirror drift)
- **Bounded wait:** `RATELIMIT_MAX_WAIT` — wait up to N, else **429** + `Retry-After`
- **Rule matching:** longest `path_prefix` wins; same-length prefixes tie-break by rule `name` (stable)
- **Keys:** `ip` (per client) or `global`
- **Client IP:** `RemoteAddr` by default; `X-Forwarded-For` only when `TRUSTED_PROXY=true`

### In-process cache (placeholder)

`entryCache` is an **LRU of per-client buckets in memory**. It is a **placeholder** for a **shared Redis-backed limiter** in multi-instance deployments — each replica currently enforces its own counters.

Replace with Redis (or similar) when you need cluster-wide limits.

## ZooKeeper

- **Watch:** reload on data change; fallback file on node delete, watch loss, session expiry
- **ACLs:** `ZK_OPEN_ACL=true` (default in development) uses `WorldACL(PermAll)` for bootstrap
- **Production:** set `ZK_OPEN_ACL=false` and `ZK_DIGEST=user:password` for digest ACL + `AddAuth`

## Health checks

`/health` is registered **outside** rate-limit middleware and is never throttled.

## Logging

Uses `log/slog` for structured fields (`source`, `count`, `version`, `err`). Extend with JSON handler + trace IDs in production.

## Known limits (sample scope)

- No distributed rate limiting (see Redis note above)
- No metrics/tracing hooks
- Digest ACL requires ZK user setup matching `ZK_DIGEST`
