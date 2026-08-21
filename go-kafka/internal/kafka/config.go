package kafka

import (
	"os"
	"strconv"
	"strings"
)

const (
	Topic   = "orders"
	GroupID = "order-consumer-group"
)

func BrokerList() []string {
	if v := os.Getenv("KAFKA_BROKERS"); v != "" {
		return strings.Split(v, ",")
	}
	return []string{"localhost:9092"}
}

func PartitionCount() int {
	if v := os.Getenv("KAFKA_PARTITIONS"); v != "" {
		n, err := strconv.Atoi(v)
		if err == nil && n > 0 {
			return n
		}
	}
	return 6
}

func ConsumerID() string {
	if v := os.Getenv("CONSUMER_ID"); v != "" {
		return v
	}
	return "consumer-1"
}
