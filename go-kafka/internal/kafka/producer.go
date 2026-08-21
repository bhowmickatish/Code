package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer     *kafka.Writer
	partitions int
}

func NewProducer(ctx context.Context, partitions int) (*Producer, error) {
	if err := EnsureTopic(ctx, partitions); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}

	return &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(BrokerList()...),
			Topic:        Topic,
			Balancer:     &kafka.Murmur2Balancer{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
		partitions: partitions,
	}, nil
}

func (p *Producer) Write(ctx context.Context, key, value []byte, headers ...Header) (partition int, err error) {
	partition = PartitionForKey(key, p.partitions)

	kafkaHeaders := make([]kafka.Header, len(headers))
	for i, h := range headers {
		kafkaHeaders[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
	})
	if err != nil {
		return 0, fmt.Errorf("write message: %w", err)
	}

	return partition, nil
}

func (p *Producer) Close() error {
	return p.writer.Close()
}
