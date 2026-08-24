package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func EnsureTopic(ctx context.Context, partitions int) error {
	controllerConn, err := dialController(ctx)
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	return controllerConn.CreateTopics(kafka.TopicConfig{
		Topic:             Topic,
		NumPartitions:     partitions,
		ReplicationFactor: 1,
	})
}

// ScalePartitions increases the topic partition count to target.
// Kafka only supports adding partitions; decreasing is not allowed.
// If target equals the current count, this is a no-op.
func ScalePartitions(ctx context.Context, target int) error {
	if target <= 0 {
		return fmt.Errorf("target partition count must be positive")
	}

	current, err := topicPartitionCount(ctx)
	if err != nil {
		return err
	}
	if current == 0 {
		return fmt.Errorf("topic %q does not exist; create it with EnsureTopic first", Topic)
	}
	if target < current {
		return fmt.Errorf("cannot decrease partitions from %d to %d", current, target)
	}
	if target == current {
		return nil
	}

	controller, err := controllerBroker(ctx)
	if err != nil {
		return err
	}

	client := &kafka.Client{
		Addr: kafka.TCP(fmt.Sprintf("%s:%d", controller.Host, controller.Port)),
	}

	res, err := client.CreatePartitions(ctx, &kafka.CreatePartitionsRequest{
		Topics: []kafka.TopicPartitionsConfig{
			{Name: Topic, Count: int32(target)},
		},
	})
	if err != nil {
		return fmt.Errorf("create partitions: %w", err)
	}
	if err := res.Errors[Topic]; err != nil {
		return fmt.Errorf("scale topic %q to %d partitions: %w", Topic, target, err)
	}

	return nil
}

func topicPartitionCount(ctx context.Context) (int, error) {
	conn, err := kafka.DialContext(ctx, "tcp", BrokerList()[0])
	if err != nil {
		return 0, fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	partitions, err := conn.ReadPartitions(Topic)
	if err != nil {
		return 0, fmt.Errorf("read partitions: %w", err)
	}

	return len(partitions), nil
}

func dialController(ctx context.Context) (*kafka.Conn, error) {
	controller, err := controllerBroker(ctx)
	if err != nil {
		return nil, err
	}

	controllerConn, err := kafka.DialContext(ctx, "tcp", fmt.Sprintf("%s:%d", controller.Host, controller.Port))
	if err != nil {
		return nil, fmt.Errorf("dial controller: %w", err)
	}

	return controllerConn, nil
}

func controllerBroker(ctx context.Context) (kafka.Broker, error) {
	conn, err := kafka.DialContext(ctx, "tcp", BrokerList()[0])
	if err != nil {
		return kafka.Broker{}, fmt.Errorf("dial broker: %w", err)
	}
	defer conn.Close()

	controller, err := conn.Controller()
	if err != nil {
		return kafka.Broker{}, fmt.Errorf("get controller: %w", err)
	}

	return controller, nil
}
