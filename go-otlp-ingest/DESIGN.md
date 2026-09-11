# Design Document: OTLP Metrics Ingest → ClickHouse

## 1. Overview

This project is a Go **metrics ingestion service**. It exposes the official OpenTelemetry Protocol (OTLP) **metrics gRPC** API. When a client exports metric data, the service flattens OpenTelemetry resource / scope / metric / data-point trees into rows and batch-inserts them into **ClickHouse**.

Any OTel SDK, Collector, or agent that already speaks OTLP/gRPC can send to this service without a custom client.

**Implementation status:** v1 implemented under `cmd/ingest`. Layout below matches the code.

### Goals

- Speak the official OTLP metrics gRPC API on port **4317** by convention.
- Support gauge, sum (counter), histogram, exponential histogram, and summary.
- Flatten Resource / Scope / Metric / DataPoint into queryable ClickHouse tables.
- Batch inserts so ClickHouse is not hit once per RPC.
- Stay compatible with the OTel Collector ClickHouse exporter mental model (Grafana-friendly maps and time columns).

### Non-goals (v1)

- Traces or logs (same OTLP family, different RPCs and tables).
- Kafka or a local WAL buffer (direct ClickHouse write is enough for a sample).
- Auth, TLS, or multi-tenant quotas.
- Exactly-once ingest (OTLP is at-least-once; duplicates are accepted).
- PromQL or a query API (ClickHouse SQL / Grafana is the read path).
- HTTP/JSON OTLP on port **4318**.
- Exemplars (skipped to keep the mapper small).

---

## 2. Architecture

### 2.1 High-level diagram

```
┌─────────────────┐     ┌─────────────────┐
│   OTel SDK      │     │ OTel Collector  │
└────────┬────────┘     └────────┬────────┘
         │  OTLP gRPC :4317      │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  cmd/ingest           │
         │  MetricsService.Export│
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  internal/mapper      │
         │  OTLP → typed rows    │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  internal/batcher     │
         │  size / time flush    │
         └───────────┬───────────┘
                     ▼
         ┌───────────────────────┐
         │  internal/clickhouse  │
         │  native protocol      │
         └───────────┬───────────┘
                     ▼
              ┌─────────────┐
              │ ClickHouse  │
              │ :9000 native│
              └─────────────┘
```

### 2.2 Layer responsibilities

| Layer        | Package / path       | Responsibility                                              |
| ------------ | -------------------- | ----------------------------------------------------------- |
| Process      | `cmd/ingest`         | Config, gRPC server, health, graceful shutdown flush        |
| OTLP gRPC    | `internal/otlpgrpc`  | `MetricsServiceServer`, validation, partial success         |
| Mapping      | `internal/mapper`    | Walk OTLP tree; emit typed rows; stringify attributes       |
| Batching     | `internal/batcher`   | Bounded queue; flush on size or interval; backpressure      |
| Storage      | `internal/clickhouse`| `CREATE TABLE IF NOT EXISTS`, batched native inserts        |
| Config       | `internal/config`    | Env-based settings (gRPC addr, CH DSN, batch knobs)         |
| Broker       | `docker-compose.yml` | ClickHouse HTTP `:8123`, native `:9000`                     |

Application code uses generated OTLP types from `go.opentelemetry.io/proto/otlp`. It does not fork the proto.

### 2.3 Project layout

```
go-otlp-ingest/
├── cmd/
│   ├── ingest/              # Process lifecycle
│   └── client/              # One-shot OTLP Export client
├── internal/
│   ├── config/              # Env: GRPC_ADDR, CH DSN, batch size/interval
│   ├── otlpgrpc/            # MetricsServiceServer
│   ├── mapper/              # OTLP → typed rows
│   ├── batcher/             # Size/time flush, backpressure
│   └── clickhouse/          # Schema ensure + insert
├── docker-compose.yml       # ClickHouse
├── DESIGN.md
└── README.md
```

---

## 3. Request path

1. Client calls `Export(ExportMetricsServiceRequest)`.
2. Server rejects empty requests and requests that exceed a max data-point budget.
3. Mapper walks `ResourceMetrics → ScopeMetrics → Metric → *DataPoints` and emits typed rows.
4. Rows go to an in-memory batcher (size and time flush).
5. Writer inserts via the native ClickHouse protocol (`clickhouse-go` v2).
6. RPC returns `ExportMetricsServiceResponse`. Mapping or insert failures use OTLP `partial_success` (rejected point count + error message), not a custom error schema.

**Ack timing:** gRPC success is returned **after a successful handoff to the batcher**, not after ClickHouse fsync. Export latency stays low. If the process dies with an unflushed batch, producers retry (at-least-once). Duplicate rows are possible and accepted.

A later production shape can put Kafka between mapper and ClickHouse so ingest ACK and ClickHouse durability are decoupled.

---

## 4. Protocol: OTLP metrics gRPC

Implement `opentelemetry.proto.collector.metrics.v1.MetricsService`.

| Item     | Value |
| -------- | ----- |
| RPC      | `Export` |
| Request  | `ExportMetricsServiceRequest` (`repeated ResourceMetrics`) |
| Response | `ExportMetricsServiceResponse` (`optional ExportMetricsPartialSuccess`) |
| Port     | `4317` (OTLP gRPC convention) |
| Content  | `application/grpc` |

HTTP/JSON OTLP (`4318`) is out of v1.

Resource attributes such as `service.name` are promoted to a first-class ClickHouse column for ordering and filtering.

### 4.1 Partial success

OTLP defines `ExportMetricsPartialSuccess`:

- `rejected_data_points` — count that could not be accepted
- `error_message` — human-readable reason

Use this when some points map but others fail (unknown metric type, oversize attributes after cap, etc.). Full request rejection uses gRPC status codes (see backpressure).

### 4.2 gRPC status codes

| Condition                         | Status                    |
| --------------------------------- | ------------------------- |
| Empty request                     | `INVALID_ARGUMENT`        |
| Over max data-point budget        | `INVALID_ARGUMENT`        |
| Batcher queue full                | `RESOURCE_EXHAUSTED`      |
| ClickHouse unavailable at handoff | `UNAVAILABLE`             |
| Shutdown in progress              | `UNAVAILABLE`             |

`RESOURCE_EXHAUSTED` is the backpressure signal so SDKs and Collectors retry with backoff instead of unbounded RAM on the ingest process.

---

## 5. Domain model (rows)

Internal rows are the mapper output and the batcher unit of work. They are not the OTLP protobuf types.

Shared fields on every row:

| Field                | Source                                      |
| -------------------- | ------------------------------------------- |
| `TimeUnix`           | Data point `time_unix_nano` (or receive time) |
| `StartTimeUnix`      | Data point start time when present          |
| `ServiceName`        | Resource attribute `service.name`           |
| `MetricName`         | `Metric.name`                               |
| `MetricDescription`  | `Metric.description`                        |
| `MetricUnit`         | `Metric.unit`                               |
| `ResourceAttributes` | Resource attributes as string map           |
| `ScopeName`          | Instrumentation scope name                  |
| `ScopeVersion`       | Instrumentation scope version               |
| `Attributes`         | Data-point attributes as string map         |

Typed extras:

| Metric kind            | Extra fields |
| ---------------------- | ------------ |
| Gauge                  | `Value` |
| Sum                    | `Value`, `AggregationTemporality`, `IsMonotonic` |
| Histogram              | `Count`, `Sum`, `Min`, `Max`, `BucketCounts`, `ExplicitBounds` |
| Exponential histogram  | `Count`, `Sum`, `Scale`, `ZeroCount`, positive/negative offset + bucket counts |
| Summary                | `Count`, `Sum`, `QuantileValues` (quantile + value arrays) |

---

## 6. ClickHouse schema

One table per metric type. Histogram buckets are not a single float; mixing types in one table forces nullable columns and awkward queries. This matches the Collector `clickhouseexporter` shape so Grafana OpenTelemetry dashboards stay familiar.

### 6.1 Shared columns

Every table includes:

```sql
ResourceAttributes Map(LowCardinality(String), String),
ScopeName          LowCardinality(String),
ScopeVersion       LowCardinality(String),
ServiceName        LowCardinality(String),
MetricName         LowCardinality(String),
MetricDescription  String,
MetricUnit         LowCardinality(String),
Attributes         Map(LowCardinality(String), String),
StartTimeUnix      DateTime64(9),
TimeUnix           DateTime64(9)
```

### 6.2 Tables

**`otel_metrics_gauge`**

- `Value Float64`

**`otel_metrics_sum`**

- `Value Float64`
- `AggregationTemporality Int32` — OTLP enum (delta / cumulative)
- `IsMonotonic Bool`

**`otel_metrics_histogram`**

- `Count UInt64`
- `Sum Nullable(Float64)`
- `Min Nullable(Float64)`
- `Max Nullable(Float64)`
- `BucketCounts Array(UInt64)`
- `ExplicitBounds Array(Float64)`

**`otel_metrics_exp_histogram`**

- `Count UInt64`
- `Sum Nullable(Float64)`
- `Scale Int32`
- `ZeroCount UInt64`
- `PositiveOffset Int32`
- `PositiveBucketCounts Array(UInt64)`
- `NegativeOffset Int32`
- `NegativeBucketCounts Array(UInt64)`

**`otel_metrics_summary`**

- `Count UInt64`
- `Sum Float64`
- `ValueAtQuantiles.Quantile Array(Float64)`
- `ValueAtQuantiles.Value Array(Float64)`

### 6.3 Engine and keys

```sql
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, toUnixTimestamp(TimeUnix), cityHash64(Attributes))
```

- Partition by day so TTL and drops are cheap.
- Order by service + metric + time so typical Grafana filters scan less.
- `cityHash64(Attributes)` keeps the sort key bounded when attribute maps are large.

### 6.4 Schema lifecycle

On startup: `CREATE TABLE IF NOT EXISTS` for all five tables. No separate migration tool in v1.

### 6.5 Inserts

- Batched `INSERT` via native protocol (`clickhouse-go` v2).
- Flush when the batch hits **N rows** or **T milliseconds** (env-configurable).
- Enable ClickHouse `async_insert` in compose so small leftover batches still land efficiently.

Exemplars are not stored in v1.

---

## 7. Mapping rules

Walk order: each `ResourceMetrics` → each `ScopeMetrics` → each `Metric` → data points of that metric’s type.

| Rule | Behavior |
| ---- | -------- |
| Empty resource / scope / metric | Skip |
| Metric with no data points / nil inner payload | Reject via `partial_success` |
| Nil data point in a metric | Reject that point via `partial_success` |
| Number value | `AsDouble` if set, else `AsInt` → `float64` |
| Missing `time_unix_nano` | Use server receive time (last resort; documented here) |
| `service.name` missing | Empty string `ServiceName`; still store full resource map |
| Attribute values | Stringify non-string `AnyValue` (bool, int, double, bytes, array, kvlist) so ClickHouse `Map(String, String)` stays uniform |
| Oversize maps | Cap max keys (sorted alphabetically) and max value length; drop excess keys |
| Unknown metric type | Reject those points via `partial_success` |
| Histogram sum/min/max | Stored as `Nullable(Float64)`; unset OTLP fields remain SQL `NULL` |

Bytes attributes are **hex-encoded**. Nested array/kvlist values are JSON-encoded strings.

---

## 8. Batcher and backpressure

```
Export RPC
  → map to rows
  → batcher.Enqueue(rows)
        ├─ queue has room → ack RPC (empty ExportMetricsServiceResponse)
        └─ queue full     → RESOURCE_EXHAUSTED (no enqueue)
  → background flush
        ├─ len(batch) >= BatchSize
        └─ or ticker BatchInterval
        → clickhouse.Insert(typed batches)
```

| Setting          | Role |
| ---------------- | ---- |
| `BatchSize`      | Flush when this many rows are pending |
| `BatchInterval`  | Flush leftover rows on a timer |
| `QueueCapacity`  | Max rows waiting (mapped but not yet inserted) |

One flush worker is enough for v1. Inserts are grouped **by table** (five insert paths) so a gauge batch does not mix with histogram rows.

On shutdown: stop accepting RPCs → flush remaining queue → close ClickHouse.

If ClickHouse is down during flush, the worker retries with backoff **or** drops and increments a log counter. v1 has **no local WAL**; durability relies on producer retry of `Export`. Prefer failing new `Export` calls with `UNAVAILABLE` when the queue is stuck because CH is down, rather than growing RAM.

---

## 9. Configuration

Env-based singleton, same idea as `go-zookeeper/internal/config`.

| Variable           | Default              | Purpose |
| ------------------ | -------------------- | ------- |
| `GRPC_ADDR`        | `:4317`              | OTLP gRPC listen |
| `HEALTH_ADDR`      | `:8080`              | HTTP `/health` |
| `CLICKHOUSE_DSN`   | native localhost DSN | `clickhouse-go` DSN |
| `BATCH_SIZE`       | `5000`               | Rows per flush |
| `BATCH_INTERVAL`   | `1s`                 | Time flush |
| `QUEUE_CAPACITY`   | `50000`              | Backpressure bound |
| `MAX_DATA_POINTS`  | `100000`             | Per-RPC budget |
| `MAX_ATTR_KEYS`    | `64`                 | Attribute map cap |
| `MAX_ATTR_VALUE`   | `1024`               | Attribute value length cap |

---

## 10. Health and lifecycle

**Startup**

1. Load config.
2. Connect to ClickHouse; `CREATE TABLE IF NOT EXISTS`.
3. Start batcher flush loop.
4. Register gRPC `MetricsService` + gRPC health.
5. Optional HTTP `:8080/health` that pings ClickHouse.
6. Listen until SIGINT/SIGTERM.

**Shutdown**

1. `signal.NotifyContext` cancel.
2. `grpc.GracefulStop` (or `Stop` after a timeout).
3. Flush batcher.
4. Close ClickHouse.

**Health**

- gRPC health and HTTP `/health`: `SERVING` / 200 when ClickHouse ping succeeds **and** the batcher is accepting writes (`IngestReady`, not blocked by insert failures).
- `NOT_SERVING` / 503 when ClickHouse is down or recent inserts are failing.

**Shutdown flush**

- Pending rows are retried for up to 10s during shutdown.
- If rows still cannot be inserted, they are logged and `Close` returns an error (data was already ACKed to clients).

---

## 11. Local stack

`docker-compose.yml` runs ClickHouse:

- HTTP `:8123` (adhoc SQL)
- Native `:9000` (ingest writer)

Local send path:

1. `docker compose up -d`
2. Run `cmd/ingest`
3. Run `cmd/client`, or point an OTel Collector / SDK at `localhost:4317`
4. Query `otel_metrics_gauge` (or the matching type table) in ClickHouse

---

## 12. Reliability and ops

| Concern            | v1 stance |
| ------------------ | --------- |
| Delivery           | At-least-once; duplicates possible |
| Partial failure    | OTLP `partial_success` |
| ClickHouse down    | Fail new Export / fill batcher; no local WAL |
| Schema             | `CREATE TABLE IF NOT EXISTS` on startup |
| Observability      | Structured logs; self-metrics later |
| Auth / TLS         | None (local PLAINTEXT) |
| Multi-tenancy      | None |

### 12.1 Duplicate data points

Retries after a successful batcher handoff but failed (or unflushed) insert produce duplicates. MergeTree does not dedupe. Partial multi-table failures within one flush retry only the tables not yet inserted, avoiding duplicate gauge rows when a later table fails.

Downstream queries should tolerate duplicates (e.g. `argMax` / aggregations), or a later version can use `ReplacingMergeTree` with a stable identity.

### 12.2 Future: Kafka buffer

Insert Kafka between mapper and ClickHouse:

- `Export` ACK = durable append to a topic
- A consumer writes to ClickHouse with its own retry
- Ingest process restart does not lose accepted batches

Out of v1.

---

## 13. Dependencies (planned)

| Library | Use |
| ------- | --- |
| `google.golang.org/grpc` | Server |
| `go.opentelemetry.io/proto/otlp` | Generated OTLP metrics / collector types |
| `github.com/ClickHouse/clickhouse-go/v2` | Native inserts |

No custom `.proto` in this repo.
