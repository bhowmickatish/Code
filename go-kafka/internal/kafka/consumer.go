package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(ctx context.Context) (*Consumer, error) {
	if err := EnsureTopic(ctx, PartitionCount()); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
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
		}),
	}, nil
}

func (c *Consumer) Fetch(ctx context.Context) (Record, error) {
	msg, err := c.reader.FetchMessage(ctx)
	if err != nil {
		return Record{}, fmt.Errorf("fetch message: %w", err)
	}

	return Record{
		Key:       msg.Key,
		Value:     msg.Value,
		Partition: msg.Partition,
		Offset:    msg.Offset,
	}, nil
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
