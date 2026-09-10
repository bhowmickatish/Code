# go-otlp-ingest

A Go sample that accepts **OTLP metrics over gRPC** and batch-inserts flattened data points into **ClickHouse**.

See [DESIGN.md](./DESIGN.md) for architecture, schema, mapping rules, and non-goals.

## Behavior

- Listen on **:4317** for `MetricsService.Export`
- Map gauge, sum, histogram, exponential histogram, and summary points to typed tables
- Batch inserts over the native protocol (`:9000`)
- Bounded in-memory queue (`RESOURCE_EXHAUSTED` when full)

The telemetry **producer** is `ServiceName` / `ResourceAttributes`. The **subject entity** (device, tenant, host, …) is in data-point `Attributes` (for example `Attributes['device.id']`).

## Prerequisites

- Go 1.26+
- Docker (for local ClickHouse)

## Quick start

### 1. Start ClickHouse

```powershell
cd d:\Code\go-otlp-ingest
docker compose up -d
```

### 2. Run the ingest service

```powershell
go run .\cmd\ingest
```

Health: `http://localhost:8080/health`

### 3. Send a sample metric

```powershell
go run .\cmd\client
```

### 4. Query ClickHouse

```powershell
docker compose exec clickhouse clickhouse-client --query "SELECT ServiceName, MetricName, Attributes, Value FROM otel.otel_metrics_gauge LIMIT 10"
```

If the database is `default` instead of `otel`, omit the `otel.` prefix or set `CLICKHOUSE_DSN`.

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `GRPC_ADDR` | `:4317` | OTLP gRPC listen |
| `HEALTH_ADDR` | `:8080` | HTTP `/health` |
| `CLICKHOUSE_DSN` | `clickhouse://default:@127.0.0.1:9000/otel` | Native DSN |
| `BATCH_SIZE` | `5000` | Rows per flush |
| `BATCH_INTERVAL` | `1s` | Time flush |
| `QUEUE_CAPACITY` | `50000` | Max queued rows |
| `MAX_DATA_POINTS` | `100000` | Per-RPC budget |
| `MAX_ATTR_KEYS` | `64` | Attribute map cap |
| `MAX_ATTR_VALUE` | `1024` | Attribute value length cap |
