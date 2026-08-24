# go-kafka

A minimal Go sample demonstrating Kafka producer and consumer patterns with topic partitions and consumer group parallelism.

## Features

- **Producer** — publishes JSON `OrderEvent` messages keyed by `product_id`
- **Consumer** — reads from a shared consumer group with manual offset commits
- **Partitions** — topic created with configurable partition count (default 6)
- **Keyed routing** — `Murmur2Balancer` maps keys to partitions (Java/librdkafka compatible)
- **Parallelism** — run multiple consumer instances in the same group; Kafka assigns one partition per consumer (up to partition count)
- **Backpressure** — bounded producer queue and consumer in-flight limits

## Prerequisites

- Go 1.26+
- Docker (for local Kafka)

## Quick start

### 1. Start Kafka

```powershell
cd d:\Code\go-kafka
docker compose up -d
```

### 2. Start consumers

```powershell
# 3 parallel consumers (default)
.\scripts\run-consumers.ps1 -Count 3

# or max parallelism with 6 partitions
.\scripts\run-consumers.ps1 -Count 6
```

Or run individually in separate terminals:

```powershell
go run ./cmd/consumer -id consumer-1
go run ./cmd/consumer -id consumer-2
```

### 3. Start the producer

```powershell
go run ./cmd/producer
```

You should see producer logs with partition numbers and consumer logs showing which instance handled each message.

## Build

Compile all packages:

```powershell
go build ./...
```

Build binaries:

```powershell
go build -o bin/producer.exe ./cmd/producer
go build -o bin/consumer.exe ./cmd/consumer
```

Run:

```powershell
.\bin\producer.exe
.\bin\consumer.exe -id consumer-1
```

## Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKERS` | `localhost:9092` | Comma-separated broker addresses |
| `KAFKA_PARTITIONS` | `6` | Number of topic partitions |
| `CONSUMER_ID` | `consumer-1` | Consumer instance label (logs only) |

### Flags

**Producer**

| Flag | Default | Description |
|------|---------|-------------|
| `-partitions` | `6` | Partitions when creating the topic |
| `-queue` | `0` | Bounded producer queue size (`0` = sync direct write) |
| `-try-write` | `false` | Return immediately with backpressure when queue is full or lag is high |
| `-max-lag` | `0` | Max consumer group lag before backpressure (`0` = disabled) |
| `-lag-poll` | `10s` | How often to refresh consumer lag from Kafka |

**Consumer**

| Flag | Default | Description |
|------|---------|-------------|
| `-id` | `consumer-1` | Unique instance ID for logging |
| `-delay` | `500ms` | Simulated processing time per message |
| `-max-inflight` | `1` | Max concurrent messages before fetch blocks |
| `-queue-capacity` | `2` | Max messages prefetched by the kafka reader |

## Backpressure

### Producer

`Producer.Write` with `-queue N` enqueues to a bounded buffer. When the buffer is full, the caller blocks until the background worker drains it.

```powershell
go run ./cmd/producer -queue 50
```

With `-try-write`, a full queue returns `ErrBackpressure` instead of blocking:

```powershell
go run ./cmd/producer -queue 10 -try-write
```

### Consumer lag gate

When `-max-lag` is set, the producer polls `ListOffsets` + `OffsetFetch` for the consumer group and blocks (or rejects with `-try-write`) when lag is too high:

```powershell
# Terminal 1: slow consumer
go run ./cmd/consumer -delay 5s

# Terminal 2: producer rejects when lag > 10
go run ./cmd/producer -max-lag 10 -try-write
```

### Consumer

`Consumer.Consume` acquires a slot before each `Fetch`. When `max-inflight` workers are busy, fetching stops until one completes — processing speed governs consumption.

```powershell
go run ./cmd/consumer -max-inflight 5 -queue-capacity 2
```

Lower `-queue-capacity` reduces prefetch and applies tighter backpressure from the broker.

## Architecture

```
Producer (cmd/producer)
    │
    ▼
internal/kafka.Producer  ──►  topic: orders (6 partitions)
    │                              │
    │ keyed by product_id          │ consumer group: order-consumer-group
    ▼                              ▼
partition 0..5  ◄────────  Consumer instances (cmd/consumer)
                           via internal/kafka.Consumer
```

### Consumer groups

Running N consumers means N separate processes of the same app, all bound to `order-consumer-group`. Kafka assigns partitions across group members:

| Consumers | Partitions | Result |
|-----------|------------|--------|
| 3 | 6 | ~2 partitions each |
| 6 | 6 | 1 partition each (max parallelism) |
| 8 | 6 | 6 active, 2 idle |

The `-id` flag is for logging only. Kafka tracks members internally.

### Partition routing

Messages are routed by `msg.Key` (set to `product_id`), not by fields inside the JSON body:

```
product_id → msg.Key → Murmur2 hash → partition
```

Same `product_id` always lands on the same partition, preserving per-product ordering.

## Project structure

```
go-kafka/
├── cmd/
│   ├── producer/       # Sample producer app
│   └── consumer/       # Sample consumer app
├── internal/kafka/
│   ├── config.go       # Brokers, topic, group ID
│   ├── topic.go        # Topic creation
│   ├── partition.go    # Murmur2 partition helper
│   ├── message.go      # Record and Header types
│   ├── producer.go     # Producer wrapper (bounded queue)
│   ├── consumer.go     # Consumer wrapper (in-flight limits)
│   ├── lag.go          # GroupLag + lag monitor
│   └── errors.go       # ErrBackpressure, ErrConsumerLag
├── scripts/
│   └── run-consumers.ps1
├── docker-compose.yml
└── go.mod
```

All `kafka-go` interactions are encapsulated in `internal/kafka`. The `cmd` packages contain only application logic.

## Stop

```powershell
docker compose down
```

Press Ctrl+C to stop producer/consumer processes.
