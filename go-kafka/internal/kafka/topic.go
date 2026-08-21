package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func EnsureTopic(ctx context.Context, partitions int) error {
	conn, err := kafka.DialContext(ctx, "tcp", BrokerList()[0])
	if err != nil {
		return fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("get controller: %w", err)
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return fmt.Errorf("dial controller: %w", err)
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             Topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}
