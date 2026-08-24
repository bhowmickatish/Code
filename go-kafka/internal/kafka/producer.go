package kafka

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"
)

type ProducerConfig struct {
	Partitions int
	// QueueSize is the bounded in-memory buffer before messages are sent to Kafka.
	// Zero uses synchronous direct writes (caller blocks on broker ack).
	QueueSize int
	// MaxLag rejects or blocks writes when total consumer group lag exceeds this value.
	// Zero disables lag-based backpressure.
	MaxLag int64
	// LagPollInterval is how often consumer lag is refreshed from Kafka.
	LagPollInterval time.Duration
	// LagGroupID is the consumer group monitored for lag. Defaults to GroupID.
	LagGroupID string
}

type Producer struct {
	writer     *kafka.Writer
	partitions int
	queue      chan publishJob
	lag        *lagMonitor
	wg         sync.WaitGroup
}

type publishJob struct {
	ctx     context.Context
	key     []byte
	value   []byte
	headers []Header
	result  chan writeResult
}

type writeResult struct {
	partition int
	err       error
}

func NewProducer(ctx context.Context, partitions int) (*Producer, error) {
	return NewProducerWithConfig(ctx, ProducerConfig{Partitions: partitions})
}

func NewProducerWithConfig(ctx context.Context, cfg ProducerConfig) (*Producer, error) {
	if cfg.Partitions <= 0 {
		return nil, fmt.Errorf("partitions must be positive")
	}
	if err := EnsureTopic(ctx, cfg.Partitions); err != nil {
		return nil, fmt.Errorf("ensure topic: %w", err)
	}

	p := &Producer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(BrokerList()...),
			Topic:        Topic,
			Balancer:     &kafka.Murmur2Balancer{},
			RequiredAcks: kafka.RequireAll,
			Async:        false,
		},
		partitions: cfg.Partitions,
	}

	if cfg.MaxLag > 0 {
		p.lag = newLagMonitor(ctx, cfg.MaxLag, cfg.LagPollInterval, cfg.LagGroupID)
	}

	if cfg.QueueSize > 0 {
		p.queue = make(chan publishJob, cfg.QueueSize)
		p.wg.Add(1)
		go p.runWorker()
	}

	return p, nil
}

func (p *Producer) Write(ctx context.Context, key, value []byte, headers ...Header) (partition int, err error) {
	if err := p.waitForLag(ctx); err != nil {
		return 0, err
	}
	return p.writeWithQueue(ctx, key, value, headers)
}

// TryWrite enqueues a message without blocking on queue capacity or lag.
// Returns ErrBackpressure when the queue is full, or ErrConsumerLag when lag is too high.
func (p *Producer) TryWrite(ctx context.Context, key, value []byte, headers ...Header) (partition int, err error) {
	if err := p.checkLag(); err != nil {
		return 0, err
	}
	return p.tryWriteWithQueue(ctx, key, value, headers)
}

func (p *Producer) waitForLag(ctx context.Context) error {
	if p.lag == nil {
		return nil
	}
	return p.lag.wait(ctx)
}

func (p *Producer) checkLag() error {
	if p.lag != nil && p.lag.lagExceeded() {
		return ErrConsumerLag
	}
	return nil
}

func (p *Producer) writeWithQueue(ctx context.Context, key, value []byte, headers []Header) (int, error) {
	partition := PartitionForKey(key, p.partitions)

	if p.queue == nil {
		return partition, p.write(ctx, key, value, headers)
	}

	result := make(chan writeResult, 1)
	job := publishJob{
		ctx:     ctx,
		key:     key,
		value:   value,
		headers: headers,
		result:  result,
	}

	select {
	case p.queue <- job:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	select {
	case res := <-result:
		return res.partition, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (p *Producer) tryWriteWithQueue(ctx context.Context, key, value []byte, headers []Header) (int, error) {
	if p.queue == nil {
		return p.writeWithQueue(ctx, key, value, headers)
	}

	result := make(chan writeResult, 1)
	job := publishJob{
		ctx:     ctx,
		key:     key,
		value:   value,
		headers: headers,
		result:  result,
	}

	select {
	case p.queue <- job:
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
		return 0, ErrBackpressure
	}

	select {
	case res := <-result:
		return res.partition, res.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (p *Producer) runWorker() {
	defer p.wg.Done()

	for job := range p.queue {
		partition := PartitionForKey(job.key, p.partitions)
		err := p.write(job.ctx, job.key, job.value, job.headers)
		job.result <- writeResult{partition: partition, err: err}
	}
}

func (p *Producer) write(ctx context.Context, key, value []byte, headers []Header) error {
	kafkaHeaders := make([]kafka.Header, len(headers))
	for i, h := range headers {
		kafkaHeaders[i] = kafka.Header{Key: h.Key, Value: h.Value}
	}

	err := p.writer.WriteMessages(ctx, kafka.Message{
		Key:     key,
		Value:   value,
		Headers: kafkaHeaders,
	})
	if err != nil {
		return fmt.Errorf("write message: %w", err)
	}
	return nil
}

func (p *Producer) Close() error {
	if p.lag != nil {
		p.lag.close()
	}
	if p.queue != nil {
		close(p.queue)
		p.wg.Wait()
	}
	return p.writer.Close()
}
