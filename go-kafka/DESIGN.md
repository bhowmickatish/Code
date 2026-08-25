# Design Document: Go Kafka Sample

## 1. Overview

This project is a minimal Go application demonstrating **Kafka producer and consumer patterns** with topic partitions, consumer group parallelism, keyed routing, and application-level backpressure.

It uses `[segmentio/kafka-go](https://github.com/segmentio/kafka-go)` behind a thin `internal/kafka` wrapper. The `cmd/producer` and `cmd/consumer` binaries contain the application logic.

### Goals

- Publish and consume JSON `OrderEvent` messages on a partitioned topic.
- Route messages by **key** (`product_id`) so ordering is preserved per product.
- Scale consumption horizontally via **consumer groups** (one partition assigned per member, up to partition count).
- Demonstrate **backpressure**: bounded producer queue, consumer in-flight limits, optional consumer-lag gate on the producer.
- Encapsulate all broker interaction in `internal/kafka` for reuse and testing.



### Non-goals

- Exactly-once semantics, transactions, or idempotent producer settings.
- Schema registry, Avro/Protobuf, or dead-letter topic infrastructure.
- Production observability (metrics, tracing, structured logging).
- Authentication/TLS (local PLAINTEXT broker only).
- Stream processing frameworks (Flink, Kafka Streams).

For a comparison with NATS JetStream, see [docs/KAFKA_VS_JETSTREAM.md](./docs/KAFKA_VS_JETSTREAM.md).

---



## 2. Architecture



### 2.1 High-level diagram

```
┌─────────────────────┐         ┌──────────────────────────────────────┐
│   cmd/producer      │         │         Apache Kafka (KRaft)           │
│   OrderEvent loop   │         │  topic: orders                         │
└──────────┬──────────┘         │    ├── partition 0                     │
           │                    │    ├── partition 1                     │
           ▼                    │    └── partition N (default 6)         │
┌─────────────────────┐         └───────────────┬────────────────────────┘
│ internal/kafka      │                         │
│ .Producer           │   keyed writes          │ consumer group:
│ (queue, lag gate)   │ ───────────────────────►│ order-consumer-group
└─────────────────────┘                         │
                                                ▼
                               ┌────────────────────────────────┐
                               │   cmd/consumer (× N processes) │
                               │   internal/kafka.Consumer      │
                               │   manual offset commit         │
                               └────────────────────────────────┘
```



### 2.2 Layer responsibilities


| Layer         | Package / path       | Responsibility                                                    |
| ------------- | -------------------- | ----------------------------------------------------------------- |
| Producer app  | `cmd/producer`       | Generate sample `OrderEvent`s, call `Producer.Write` / `TryWrite` |
| Consumer app  | `cmd/consumer`       | Run `Consumer.Consume` with simulated processing delay            |
| Kafka wrapper | `internal/kafka`     | Topic admin, producer, consumer, lag, partition helpers           |
| Broker        | `docker-compose.yml` | Single-node KRaft broker on `localhost:9092`                      |


All `kafka-go` usage stays in `internal/kafka`. Application code never imports `segmentio/kafka-go` directly.

### 2.3 Project layout

```
go-kafka/
├── cmd/
│   ├── producer/          # Sample producer
│   └── consumer/          # Sample consumer
├── internal/kafka/
│   ├── config.go          # Brokers, topic, group ID, env helpers
│   ├── topic.go           # EnsureTopic, ScalePartitions
│   ├── partition.go       # Murmur2 partition preview
│   ├── message.go         # Record, Header types
│   ├── producer.go        # Producer + bounded queue + lag gate
│   ├── consumer.go        # Consumer + in-flight Consume loop
│   ├── lag.go             # GroupLag, lagMonitor
│   └── errors.go          # ErrBackpressure, ErrConsumerLag
├── docs/
│   └── KAFKA_VS_JETSTREAM.md
├── scripts/
│   └── run-consumers.ps1  # Launch N consumer processes
└── docker-compose.yml
```

---



## 3. Domain Model



### 3.1 OrderEvent (application)

```go
type OrderEvent struct {
    OrderID   string    `json:"order_id"`
    ProductID string    `json:"product_id"`
    Quantity  int       `json:"quantity"`
    CreatedAt time.Time `json:"created_at"`
}
```

The demo producer cycles `product_id` as `prod-1` … `prod-{partitions}` so traffic spreads across partitions while preserving **per-product ordering** (same key → same partition).

### 3.2 Record (wrapper)

```go
type Record struct {
    Key       []byte
    Value     []byte
    Partition int
    Offset    int64
}
```

`Record` is the consumer-facing view after `FetchMessage`. Offsets are committed manually via `Commit`.

### 3.3 Kafka constants


| Constant  | Value                  | Purpose                                                |
| --------- | ---------------------- | ------------------------------------------------------ |
| `Topic`   | `orders`               | Single demo topic                                      |
| `GroupID` | `order-consumer-group` | Shared consumer group for all `cmd/consumer` instances |


---



## 4. Topic & Partitions



### 4.1 Topic creation

`EnsureTopic(ctx, partitions)` connects to the cluster controller and calls `CreateTopics`:

- Topic: `orders`
- `NumPartitions`: caller-supplied (producer flag or env default)
- `ReplicationFactor`: `1` (single-broker local dev)

If the topic already exists, creation is a no-op (Kafka returns success).

### 4.2 Partition scaling

`ScalePartitions(ctx, target)` adds partitions via `CreatePartitions`. Kafka **only allows increasing** partition count. Decreasing returns an error.

Not used by the demo binaries today; exposed for ops/testing.

### 4.3 Partition count sources


| Source                                      | Default | Notes                                                                     |
| ------------------------------------------- | ------- | ------------------------------------------------------------------------- |
| Producer `-partitions` flag                 | `6`     | Passed to `EnsureTopic` at producer startup                               |
| `KAFKA_PARTITIONS` env                      | `6`     | Used by `PartitionCount()` for consumer startup                           |
| `docker-compose.yml` `KAFKA_NUM_PARTITIONS` | `3`     | Broker auto-create default only; **app creates topic with its own count** |


**Important:** Producer and consumer should agree on partition count. Mismatch does not break consumption but affects routing expectations and max parallelism.

---



## 5. Partition Routing (Keyed Messages)

Messages are routed by `msg.Key`, not by JSON fields inside the value.

```
product_id string → []byte key → Murmur2 hash → partition index
```

Producer configuration:

```go
Balancer: &kafka.Murmur2Balancer{}  // Java / librdkafka compatible
```

`PartitionForKey(key, partitionCount)` in `partition.go` mirrors the same hash for **local partition preview** in logs (returned from `Write` before broker ack).


| Property   | Behavior                                     |
| ---------- | -------------------------------------------- |
| Same key   | Always same partition → **ordering per key** |
| Empty key  | Partition `0`                                |
| Key change | Routing changes; ordering scope changes      |


Demo producer sets `key = []byte(event.ProductID)`.

---



## 6. Producer Design



### 6.1 Write path

```
Write(ctx, key, value, headers...)
  │
  ├─ lag gate (if MaxLag > 0): wait until group lag ≤ threshold
  │
  └─ writeWithQueue
        ├─ queue nil → synchronous writer.WriteMessages (blocks on ack)
        └─ queue N   → enqueue job → background worker → WriteMessages
```

Writer settings:


| Setting        | Value             | Rationale                                      |
| -------------- | ----------------- | ---------------------------------------------- |
| `RequiredAcks` | `RequireAll`      | Wait for ISR ack (stricter durability in demo) |
| `Async`        | `false`           | Synchronous ack per message                    |
| `Balancer`     | `Murmur2Balancer` | Key-compatible partitioning                    |




### 6.2 Bounded producer queue

When `QueueSize > 0`:

- `Write` blocks until a queue slot is free, then until the worker reports broker result.
- `TryWrite` returns `ErrBackpressure` immediately if the queue is full.
- One background goroutine drains the queue (`runWorker`).

When `QueueSize == 0` (default): caller blocks directly on `WriteMessages`.

### 6.3 Consumer lag gate

When `MaxLag > 0`, a background `lagMonitor` polls `GroupLag` every `LagPollInterval` (default `10s`).


| Method     | Lag exceeded                              |
| ---------- | ----------------------------------------- |
| `Write`    | Blocks (poll every 100ms) until lag drops |
| `TryWrite` | Returns `ErrConsumerLag` immediately      |


Lag calculation (`lag.go`):

```
per partition: lag = log_end_offset - committed_group_offset
total lag     = sum(partition lags)
```

Uses `ListOffsets` (end) + `OffsetFetch` (committed). If the consumer group does not exist yet, committed offsets are treated as zero.

**Purpose:** Slow consumers can throttle the demo producer — a simple **end-to-end backpressure** pattern without Kafka quotas.

### 6.4 Message headers

Demo producer attaches:


| Header           | Value              |
| ---------------- | ------------------ |
| `content-type`   | `application/json` |
| `schema-version` | `1`                |


Headers are passed through `internal/kafka.Header` → `kafka.Header`.

---



## 7. Consumer Design



### 7.1 Reader configuration

```go
kafka.NewReader(kafka.ReaderConfig{
    Brokers:        BrokerList(),
    Topic:          Topic,
    GroupID:        GroupID,
    MinBytes:       1,
    MaxBytes:       10e6,
    CommitInterval: 0,           // manual commits only
    StartOffset:    kafka.FirstOffset,
    QueueCapacity:  queueCapacity, // default 2
})
```


| Setting                    | Effect                                         |
| -------------------------- | ---------------------------------------------- |
| `CommitInterval: 0`        | Offsets committed only via `CommitMessages`    |
| `StartOffset: FirstOffset` | New consumer group reads from beginning of log |
| `QueueCapacity`            | Max prefetched messages in reader buffer       |




### 7.2 Consume loop (in-flight backpressure)

`Consume(ctx, cfg, handler)` runs a perpetual loop:

```
acquire slot (maxInFlight semaphore)
  → FetchMessage
  → spawn goroutine:
        handler(rec)
        on success → Commit(rec)
        release slot
```


| `MaxInFlight` | Behavior                                                                   |
| ------------- | -------------------------------------------------------------------------- |
| `1` (default) | Strict sequential processing per consumer loop                             |
| `> 1`         | Parallel handlers; **commits may complete out of order** across partitions |


When all slots are busy, `Fetch` is not called — **processing speed governs consumption rate**.

### 7.3 Offset commit semantics

- **At-least-once delivery:** commit after handler success.
- Handler error → **no commit** → message redelivered on restart/rebalance.
- Demo consumer: JSON unmarshal failure logs and returns `nil` → **commits anyway** (poison message skipped). Production code would use DLQ or `Nack`-style retry policy.



### 7.4 Consumer instance identity

The `-id` flag (or `CONSUMER_ID` env) is **logging only**. Kafka assigns partitions via the consumer group protocol; instance labels are not sent to the broker.

---



## 8. Consumer Groups & Parallelism

Run **N separate processes** of `cmd/consumer`, all with `GroupID = order-consumer-group`:


| Consumers (N) | Partitions (P) | Assignment                        |
| ------------- | -------------- | --------------------------------- |
| 3             | 6              | ~2 partitions each                |
| 6             | 6              | 1 partition each (max throughput) |
| 8             | 6              | 6 active, 2 idle                  |


Use `scripts/run-consumers.ps1 -Count N` to launch N processes on Windows.

Rebalance occurs when members join or leave. `KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0` in compose speeds local startup.

---



## 9. Backpressure Summary


| Mechanism          | Location           | Trigger                    | Blocking behavior                            |
| ------------------ | ------------------ | -------------------------- | -------------------------------------------- |
| Producer queue     | `Producer.Write`   | Queue full                 | Block (or `ErrBackpressure` with `TryWrite`) |
| Consumer lag gate  | `Producer.Write`   | `GroupLag > MaxLag`        | Block (or `ErrConsumerLag`)                  |
| Consumer in-flight | `Consumer.Consume` | `MaxInFlight` workers busy | Delay next `Fetch`                           |
| Reader prefetch    | `kafka.Reader`     | `QueueCapacity`            | Limits broker prefetch                       |


Recommended demo pairings:

```powershell
# Slow consumer + lag-aware producer
go run ./cmd/consumer -delay 5s
go run ./cmd/producer -max-lag 10 -try-write

# Parallel consumer processing
go run ./cmd/consumer -max-inflight 5 -queue-capacity 2
```

---



## 10. Configuration



### 10.1 Environment variables


| Variable           | Default          | Description                                            |
| ------------------ | ---------------- | ------------------------------------------------------ |
| `KAFKA_BROKERS`    | `localhost:9092` | Comma-separated broker addresses                       |
| `KAFKA_PARTITIONS` | `6`              | Partition count hint for consumer (`PartitionCount()`) |
| `CONSUMER_ID`      | `consumer-1`     | Default `-id` flag value (logs only)                   |




### 10.2 Producer flags (`cmd/producer`)


| Flag          | Default | Description                                |
| ------------- | ------- | ------------------------------------------ |
| `-partitions` | `6`     | Partitions when creating topic             |
| `-queue`      | `0`     | Bounded producer queue (`0` = sync)        |
| `-try-write`  | `false` | Non-blocking; return on backpressure       |
| `-max-lag`    | `0`     | Max consumer group lag before backpressure |
| `-lag-poll`   | `10s`   | Lag refresh interval                       |




### 10.3 Consumer flags (`cmd/consumer`)


| Flag              | Default      | Description                      |
| ----------------- | ------------ | -------------------------------- |
| `-id`             | `consumer-1` | Instance label for logs          |
| `-delay`          | `500ms`      | Simulated processing time        |
| `-max-inflight`   | `1`          | Concurrent messages per consumer |
| `-queue-capacity` | `2`          | Reader prefetch limit            |


---



## 11. Infrastructure



### 11.1 Local broker

`docker-compose.yml` runs a single **Apache Kafka 3.7** node in KRaft mode (combined broker + controller):


| Setting     | Value                        |
| ----------- | ---------------------------- |
| Image       | `apache/kafka:3.7.0`         |
| Client port | `9092`                       |
| Listener    | `PLAINTEXT://localhost:9092` |
| Replication | Factor `1` (single node)     |


```powershell
docker compose up -d
docker compose down   # stop
```



### 11.2 Dependencies


| Package                         | Version | Role           |
| ------------------------------- | ------- | -------------- |
| `github.com/segmentio/kafka-go` | v0.4.49 | Client library |
| Go                              | 1.26+   | Language       |


---



## 12. End-to-End Flows



### 12.1 Producer publish loop

1. Build `OrderEvent` with cycling `product_id`.
2. Marshal JSON → `value`; `key = product_id`.
3. `Producer.Write` (or `TryWrite`) → lag check → queue/direct write.
4. `kafka.Writer` sends to partition chosen by `Murmur2Balancer`.
5. Log `order`, `product`, `partition`; sleep `2s`; repeat.

On `ErrBackpressure` / `ErrConsumerLag`, demo retries after `100ms` (decrements counter to resend same order).

### 12.2 Consumer processing loop

1. `EnsureTopic` on startup (uses `PartitionCount()` from env).
2. `Consume` acquires slot → `FetchMessage`.
3. Unmarshal JSON; log partition, offset, key, order fields.
4. Sleep `-delay`; commit offset on success.
5. Repeat until SIGINT/SIGTERM.



### 12.3 Startup order

Either order works. Typical demo:

1. `docker compose up -d`
2. Start consumers (`run-consumers.ps1` or manual)
3. Start producer

Consumers joining an empty group with `StartOffset: FirstOffset` consume from the earliest retained messages.

---



## 13. Design Decisions & Trade-offs


| Decision                          | Rationale                                                   |
| --------------------------------- | ----------------------------------------------------------- |
| `internal/kafka` wrapper          | Isolate `kafka-go`; keep cmd apps thin                      |
| Key = `product_id`                | Per-product ordering without parsing JSON in broker         |
| `Murmur2Balancer`                 | Match Java ecosystem default partitioner                    |
| Manual commits                    | Explicit at-least-once; demo control over ack timing        |
| `RequireAll` acks                 | Stronger durability signal for sample                       |
| Lag-based producer gate           | Illustrate consumer-driven throttling without broker quotas |
| Bounded channels for backpressure | Simple Go-native flow control vs complex rate limiters      |
| Single topic / single group       | Minimize moving parts for learning                          |
| Bad JSON commits in demo          | Avoid stuck consumer on poison message; not production-safe |




### Known limitations

- Single broker, replication factor 1 — no fault tolerance.
- No dead-letter queue; handler failures skip commit but bad JSON is committed away.
- `MaxInFlight > 1` can commit offsets out of order across partitions.
- Partition count must be coordinated between producer and consumer env/flags.
- Lag monitor uses first broker in list only; not cluster-aware HA.
- No graceful consumer rebalance hooks (static group membership demo).

---



## 14. Future Extensions

- Idempotent producer and transactional writes for exactly-once demo.
- Dead-letter topic + retry policy for failed handlers.
- Schema registry integration (Avro/JSON Schema).
- Metrics: lag, produce/consume rate, rebalance events (Prometheus/OpenTelemetry).
- Use `ScalePartitions` in a CLI or migration tool with consumer rebalance documentation.
- TLS/SASL configuration for non-local brokers.
- Replace busy-loop lag wait with channel notification from `lagMonitor`.

---



## 15. Related Documents


| Document                                                   | Contents                                  |
| ---------------------------------------------------------- | ----------------------------------------- |
| [README.md](./README.md)                                   | Quick start, flags, backpressure examples |
| [docs/KAFKA_VS_JETSTREAM.md](./docs/KAFKA_VS_JETSTREAM.md) | Kafka vs NATS JetStream comparison        |


