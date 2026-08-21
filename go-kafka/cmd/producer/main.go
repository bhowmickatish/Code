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
	flag.Parse()

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	producer, err := kafkapkg.NewProducer(ctx, *partitions)
	if err != nil {
		log.Fatalf("create producer: %v", err)
	}
	defer producer.Close()

	log.Printf("topic %q ready with %d partitions", kafkapkg.Topic, *partitions)

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
		partition, err := producer.Write(ctx, key, payload,
			kafkapkg.Header{Key: "content-type", Value: []byte("application/json")},
			kafkapkg.Header{Key: "schema-version", Value: []byte(strconv.Itoa(1))},
		)
		if err != nil {
			log.Fatalf("publish: %v", err)
		}

		log.Printf("produced order=%s product=%s partition=%d", event.OrderID, event.ProductID, partition)
		time.Sleep(2 * time.Second)
	}
}
