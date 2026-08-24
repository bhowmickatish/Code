package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	kafkapkg "github.com/atish/go-kafka/internal/kafka"
)

type OrderEvent struct {
	OrderID   string `json:"order_id"`
	ProductID string `json:"product_id"`
	Quantity  int    `json:"quantity"`
	CreatedAt string `json:"created_at"`
}

func main() {
	consumerID := flag.String("id", kafkapkg.ConsumerID(), "unique consumer instance id within the group")
	processingDelay := flag.Duration("delay", 500*time.Millisecond, "simulated processing time per message")
	maxInFlight := flag.Int("max-inflight", 1, "max messages processed concurrently before fetch blocks")
	queueCapacity := flag.Int("queue-capacity", 2, "max messages prefetched by the kafka reader")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	cfg := kafkapkg.ConsumerConfig{
		MaxInFlight:   *maxInFlight,
		QueueCapacity: *queueCapacity,
	}

	consumer, err := kafkapkg.NewConsumerWithConfig(ctx, cfg)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf(
		"consumer %q started (group=%s, topic=%s, partitions=%d, max_inflight=%d, queue_capacity=%d)",
		*consumerID, kafkapkg.GroupID, kafkapkg.Topic, kafkapkg.PartitionCount(), *maxInFlight, *queueCapacity,
	)

	err = consumer.Consume(ctx, cfg, func(ctx context.Context, rec kafkapkg.Record) error {
		var event OrderEvent
		if err := json.Unmarshal(rec.Value, &event); err != nil {
			log.Printf(
				"consumer=%s bad payload partition=%d offset=%d: %v",
				*consumerID, rec.Partition, rec.Offset, err,
			)
			return nil
		}

		log.Printf(
			"consumer=%s partition=%d offset=%d key=%s order=%s product=%s qty=%d",
			*consumerID, rec.Partition, rec.Offset, string(rec.Key), event.OrderID, event.ProductID, event.Quantity,
		)
		time.Sleep(*processingDelay)
		return nil
	})
	if err != nil && ctx.Err() == nil {
		log.Fatalf("consume: %v", err)
	}
	log.Printf("consumer %q shutting down", *consumerID)
}
