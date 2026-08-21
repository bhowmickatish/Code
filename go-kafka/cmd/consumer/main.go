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
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	consumer, err := kafkapkg.NewConsumer(ctx)
	if err != nil {
		log.Fatalf("create consumer: %v", err)
	}
	defer consumer.Close()

	log.Printf(
		"consumer %q started (group=%s, topic=%s, partitions=%d)",
		*consumerID, kafkapkg.GroupID, kafkapkg.Topic, kafkapkg.PartitionCount(),
	)

	for {
		rec, err := consumer.Fetch(ctx)
		if err != nil {
			if ctx.Err() != nil {
				log.Printf("consumer %q shutting down", *consumerID)
				return
			}
			log.Fatalf("fetch: %v", err)
		}

		var event OrderEvent
		if err := json.Unmarshal(rec.Value, &event); err != nil {
			log.Printf(
				"consumer=%s bad payload partition=%d offset=%d: %v",
				*consumerID, rec.Partition, rec.Offset, err,
			)
		} else {
			log.Printf(
				"consumer=%s partition=%d offset=%d key=%s order=%s product=%s qty=%d",
				*consumerID, rec.Partition, rec.Offset, string(rec.Key), event.OrderID, event.ProductID, event.Quantity,
			)
			time.Sleep(*processingDelay)
		}

		if err := consumer.Commit(ctx, rec); err != nil {
			if ctx.Err() != nil {
				log.Printf("consumer %q shutting down", *consumerID)
				return
			}
			log.Fatalf("commit: %v", err)
		}
	}
}
