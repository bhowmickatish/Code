package kafka

import "errors"

// ErrBackpressure is returned when a non-blocking producer write cannot enqueue.
var ErrBackpressure = errors.New("kafka: producer queue full")

// ErrConsumerLag is returned when consumer group lag exceeds the configured threshold.
var ErrConsumerLag = errors.New("kafka: consumer lag exceeds threshold")
