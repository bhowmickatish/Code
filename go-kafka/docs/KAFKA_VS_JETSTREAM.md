# Kafka vs NATS JetStream

A comparison grounded in the `go-kafka` sample app and the design trade-offs discussed when evaluating both systems.

---

## 1. Core model

| | **Apache Kafka** | **NATS JetStream** |
|---|---|---|
| **Mental model** | Distributed append-only log | Durable streaming on top of lightweight pub/sub |
| **Unit of storage** | Topic → partitions | Stream → subjects (hierarchical) |
| **Routing** | Topic + partition (key hash) | Subject hierarchy (`orders.us.created`) |
| **Consumer model** | Consumer groups with partition assignment | Durable consumers, pull or push, queue groups |
| **Position tracking** | Offsets per partition | Sequence numbers / consumer state |
| **Replay** | Read from any offset | Replay from start, time, or sequence |

### Architecture sketch

```
Kafka                              JetStream
─────                              ─────────

Producer                           Publisher
   │                                  │
   ▼                                  ▼
Topic                              Stream
 ├── partition 0                    └── subjects (orders.>)
 ├── partition 1                           │
 └── partition N                          ▼
        │                          Durable consumer
        ▼                          + queue group
Consumer group                          │
 ├── consumer-1                         ├── instance 1
 ├── consumer-2                         ├── instance 2
 └── consumer-N                         └── instance N
```

### Mapping from `go-kafka`

| Kafka (this repo) | JetStream equivalent |
|---|---|
| Topic `orders` | Stream bound to `orders.>` |
| 6 partitions | Optional subject shards (not built-in) |
| `product_id` as message key | Publish to `orders.{product_id}` |
| `order-consumer-group` | Durable consumer + queue group |
| `CommitMessages` | `msg.Ack()` |
| `EnsureTopic` / `ScalePartitions` | `js.AddStream` / stream config updates |

---

## 2. Messaging semantics

| | **Kafka** | **NATS JetStream** |
|---|---|---|
| **Delivery** | At-least-once (offset commit) | At-least-once (explicit ack) |
| **Exactly-once** | Transactions + idempotent producer | Dedup window (stream-level) |
| **Ordering** | Per partition | Per subject (if designed that way) |
| **Ack model** | Offset commit after processing | `Ack`, `Nak`, `Term`, ack wait / redelivery |
| **Poison messages** | Manual DLQ pattern | Built-in max deliveries + dead letter |

---

## 3. Performance and operations

| | **Kafka** | **NATS JetStream** |
|---|---|---|
| **Throughput** | Very high (batching, sequential disk I/O) | High; often lower latency |
| **Latency** | ms–tens of ms | sub-ms to low ms typical |
| **Retention** | Time/size per topic; long-term store | Limits, interest, or work-queue policies |
| **Cluster complexity** | Higher (KRaft, brokers, ISR) | Lower (single `nats-server -js`) |
| **Resource footprint** | Heavier | Lightweight single binary |
| **Local dev** | `docker compose` (see `docker-compose.yml`) | `nats-server -js` |
| **Stream processing** | Kafka Streams, Flink, etc. | External (process in app) |
| **KV / object store** | External (Redis, S3) | Built-in JetStream KV and Object Store |
| **Request/reply** | Awkward | Native NATS pattern |

---

## 4. Parallelism: 6 consumers

### Kafka (this repo)

Running 6 consumers means **6 processes** of `cmd/consumer`, all with the same `GroupID`. Kafka assigns **one partition per consumer** (with 6 partitions and 6 consumers).

```
partition 0 → consumer-1
partition 1 → consumer-2
...
partition 5 → consumer-6
```

- Max useful consumers = **partition count**
- Assignment is **fixed** per rebalance (consumer-N owns specific partitions)
- One partition **cannot** be assigned to multiple consumers in the same group

### JetStream equivalent

**Closest pattern: queue group**

Run 6 instances with the same durable consumer name and queue group. NATS distributes messages — each message goes to one instance.

```go
js.QueueSubscribe("orders.>", "order-workers", handler,
    nats.BindStream("ORDERS"),
    nats.Durable("order-consumer"),
    nats.ManualAck(),
)
```

| | **Kafka** | **JetStream** |
|---|---|---|
| Load balancing | Consumer group (internal) | Queue group (internal) |
| Max useful workers | = partition count | Not hard-capped at partition count |
| Assignment | Fixed partition → consumer | Dynamic; next message to any idle member |
| Ordering scope | Per partition | Per subject |

**For Kafka-like per-key ordering in JetStream**, publish to a subject per key:

```go
subject := fmt.Sprintf("orders.%s", productID)
js.Publish(subject, payload)
```

**For true 1:1 pinning** (like partition assignment), shard explicitly:

```
orders.shard-0 … orders.shard-5
```

Each consumer instance subscribes to one subject. **The application owns this logic** — JetStream does not hash keys to shards for you.

**Do not confuse with fan-out:** 6 separate durable consumers (no queue group) each receive **every** message — like different Kafka consumer groups, not load sharing.

---

## 5. Who handles sharding?

This was a key distinction from our discussion.

### Kafka — internal

| Concern | Handled by |
|---|---|
| Producer → shard | Client (`Murmur2Balancer`) hashes `key` → partition |
| Consumer → shard | Group coordinator assigns partitions to members |
| Load balancing | Consumer group |
| Per-key ordering | Guaranteed within a partition (while partition count is unchanged) |

The app sets `msg.Key = product_id` and joins a group. Kafka does the rest.

In this repo:

```go
// internal/kafka/producer.go
Balancer: &kafka.Murmur2Balancer{}

// cmd/producer/main.go
key := []byte(event.ProductID)
producer.Write(ctx, key, payload, ...)
```

### JetStream — mixed

| Concern | Handled by |
|---|---|
| Load balancing across N instances | **NATS** via queue group |
| Durability, acks, redelivery | **JetStream** |
| Sharding / ordering by key | **Your application** via subject design |
| Fixed consumer ↔ shard pinning | **Your application** via per-shard subscriptions |

```
Kafka:
  app sets key → [Kafka partitions + consumer group] → routing + balancing

JetStream:
  app sets subject → [JetStream stream] → storage
  app joins queue group → [NATS] → load balancing
  app designs subjects → [your app] → key-based sharding / ordering
```

### Summary

- **Load balancing across consumers:** both systems handle it internally.
- **Key-based sharding and per-key ordering:** Kafka handles it via partitions; JetStream requires explicit subject design (unless you only need “any worker processes any message”).

---

## 6. Partitions and scaling (Kafka-specific)

Concepts covered in the context of `internal/kafka/topic.go` and consumer groups.

### Partition → consumer mapping

- **Not manual.** The group coordinator assigns partitions on rebalance.
- **One partition → one consumer** within the same group (never shared).
- **One consumer → many partitions** when consumers < partitions.
- **Consumers do not auto-scale** to match partitions; you deploy instances, then Kafka rebalances.

### Increasing partitions

- `CreateTopics` ( `EnsureTopic` ) only creates the topic; it does **not** change partition count on an existing topic.
- `ScalePartitions` uses Kafka `CreatePartitions` to increase the **total** count.
- Partitions can only be **added**, never removed.

### Ordering risk when scaling partitions

Routing uses `murmur2(key) % num_partitions`. When `num_partitions` changes, the **same key may map to a different partition**:

```
6 partitions:  prod-3 → partition 2
12 partitions: prod-3 → partition 8
```

Per-key ordering can break across old and new messages. **Best practice:** over-provision partitions at topic creation; scale **consumers**, not partitions, when key ordering matters.

### Replication factor

`ReplicationFactor` in `EnsureTopic` is the **total copies** of each partition across brokers (1 leader + followers).

| RF | Meaning |
|---|---|
| 1 | No redundancy (fine for local single-broker dev) |
| 3 | 1 leader + 2 followers (typical production) |

Requires `RF ≤ number of brokers`.

---

## 7. When to choose which

### Choose **Kafka** when you need

- Very high sustained throughput
- Long retention as a system of record / event log
- Event sourcing with replay from arbitrary offsets
- Complex stream processing (joins, windows, aggregations)
- Mature ecosystem and team familiarity
- Internal key-based sharding without designing subject hierarchies

**Fits:** event sourcing, CDC pipelines, analytics ingestion, large microservice backbones.

### Choose **NATS JetStream** when you need

- Low-latency messaging with optional durability
- Simple ops and small footprint
- Subject-based routing (`orders.*.created`)
- Request/reply + pub/sub + durable streams in one system
- Built-in KV / object store without extra infra
- Work-queue patterns with less partition management

**Fits:** microservices mesh, command/event buses, IoT, control planes, job queues at moderate scale.

---

## 8. Quick reference

| Dimension | **Kafka** | **JetStream** |
|---|---|---|
| Raw throughput & retention at scale | ✅ | |
| Latency & simplicity | | ✅ |
| Event log / replay / analytics | ✅ | |
| Unified messaging (pub/sub + RPC + durable) | | ✅ |
| Operational weight | Heavier | Lighter |
| Key-based sharding | Internal (partitions) | Explicit (subjects) |
| Max parallel consumers | = partition count | Queue group (flexible) |
| Per-key ordering | Per partition | Per subject (if designed) |

---

## 9. One-line summary

**Kafka** is a **log you process** — partitions and consumer groups handle sharding and balancing for you.

**JetStream** is **messaging with optional durability** — queue groups handle balancing; **you design subjects** when you need key-based ordering or pinned shard ownership.
