package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	kafkapkg "github.com/atish/go-kafka/internal/kafka"
)

type OrderEvent struct {
	OrderID   string    `json:"order_id"`
	ProductID string    `json:"product_id"`
	Quantity  int       `json:"quantity"`
	CreatedAt time.Time `json:"created_at"`
}

func main() {
	partitions := flag.Int("partitions", kafkapkg.PartitionCount(), "number of topic partitions")
	queueSize := flag.Int("queue", 0, "bounded producer queue size (0 = sync direct write)")
	nonBlocking := flag.Bool("try-write", false, "reject immediately when producer queue is full or lag is high")
	maxLag := flag.Int64("max-lag", 0, "max consumer group lag before backpressure (0 = disabled)")
	lagPoll := flag.Duration("lag-poll", 10*time.Second, "how often to refresh consumer lag from Kafka")
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	producer, err := kafkapkg.NewProducerWithConfig(ctx, kafkapkg.ProducerConfig{
		Partitions:      *partitions,
		QueueSize:       *queueSize,
		MaxLag:          *maxLag,
		LagPollInterval: *lagPoll,
		LagGroupID:      kafkapkg.GroupID,
	})
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	log.Printf(
		"topic %q ready with %d partitions (queue=%d, max_lag=%d, lag_poll=%s)",
		kafkapkg.Topic, *partitions, *queueSize, *maxLag, *lagPoll,
	)

	for i := 1; ; i++ {
		select {
		case <-ctx.Done():
			log.Println("producer shutting down")
			return
		default:
		}

		productID := fmt.Sprintf("prod-%d", (i%*partitions)+1)
		event := OrderEvent{
			OrderID:   fmt.Sprintf("order-%d", i),
			ProductID: productID,
			Quantity:  i % 5,
			CreatedAt: time.Now().UTC(),
		}

		payload, err := json.Marshal(event)
		if err != nil {
			log.Fatalf("marshal: %v", err)
		}

		key := []byte(event.ProductID)
		headers := []kafkapkg.Header{
			{Key: "content-type", Value: []byte("application/json")},
			{Key: "schema-version", Value: []byte(strconv.Itoa(1))},
		}

		var partition int
		if *nonBlocking {
			partition, err = producer.TryWrite(ctx, key, payload, headers...)
		} else {
			partition, err = producer.Write(ctx, key, payload, headers...)
		}
		if err != nil {
			switch err {
			case kafkapkg.ErrBackpressure:
				log.Printf("backpressure: queue full, retrying order=%s", event.OrderID)
			case kafkapkg.ErrConsumerLag:
				log.Printf("backpressure: consumer lag high, retrying order=%s", event.OrderID)
			default:
				log.Fatalf("publish: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			i--
			continue
		}

		log.Printf("produced order=%s product=%s partition=%d", event.OrderID, event.ProductID, partition)
		time.Sleep(2 * time.Second)
	}
}
