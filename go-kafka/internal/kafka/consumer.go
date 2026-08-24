package kafka

import (
	"context"
	"fmt"
	"sync"

	"github.com/segmentio/kafka-go"
)

type ConsumerConfig struct {
	// MaxInFlight limits how many messages can be processed concurrently.
	// Fetch blocks until a slot is available. Default 1 preserves strict ordering per consumer loop.
	MaxInFlight int
	// QueueCapacity limits how many messages the reader prefetches. Lower values apply tighter backpressure.
	QueueCapacity int
}

type Consumer struct {
	reader *kafka.Reader
}

type RecordHandler func(ctx context.Context, rec Record) error

func NewConsumer(ctx context.Context) (*Consumer, error) {
	return NewConsumerWithConfig(ctx, ConsumerConfig{})
}

func NewConsumerWithConfig(ctx context.Context, cfg ConsumerConfig) (*Consumer, error) {
	if err := EnsureTopic(ctx, PartitionCount()); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}

	queueCapacity := cfg.QueueCapacity
	if queueCapacity <= 0 {
		queueCapacity = 2
	}

	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        BrokerList(),
			Topic:          Topic,
			GroupID:        GroupID,
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0,
			StartOffset:    kafka.FirstOffset,
			QueueCapacity:  queueCapacity,
		}),
	}, nil
}

func (c *Consumer) Fetch(ctx context.Context) (Record, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Record{}, fmt.Errorf("fetch message: %w", err)
	}

	return recordFromMessage(msg), nil
}

// Consume fetches and processes messages with bounded in-flight concurrency.
// A slot is acquired before each fetch, so processing backpressure slows consumption.
// When MaxInFlight > 1, commits may complete out of order across partitions; handlers should be idempotent.
func (c *Consumer) Consume(ctx context.Context, cfg ConsumerConfig, handler RecordHandler) error {
	maxInFlight := cfg.MaxInFlight
	if maxInFlight <= 0 {
		maxInFlight = 1
	}

	slots := make(chan struct{}, maxInFlight)
	var wg sync.WaitGroup

	for {
		select {
		case <-ctx.Done():
			wg.Wait()
			return ctx.Err()
		case slots <- struct{}{}:
		}

		rec, err := c.Fetch(ctx)
		if err != nil {
			<-slots
			if ctx.Err() != nil {
				wg.Wait()
				return ctx.Err()
			}
			return err
		}

		wg.Add(1)
		go func(rec Record) {
			defer wg.Done()
			defer func() { <-slots }()

			if err := handler(ctx, rec); err != nil {
				return
			}
			_ = c.Commit(ctx, rec)
		}(rec)
	}
}

func (c *Consumer) Commit(ctx context.Context, rec Record) error {
	if err := c.reader.CommitMessages(ctx, kafka.Message{
		Key:       rec.Key,
		Value:     rec.Value,
		Partition: rec.Partition,
		Offset:    rec.Offset,
	}); err != nil {
		return fmt.Errorf("commit message: %w", err)
	}
	return nil
}

func (c *Consumer) Close() error {
	return c.reader.Close()
}

func recordFromMessage(msg kafka.Message) Record {
	return Record{
		Key:       msg.Key,
		Value:     msg.Value,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}
}
