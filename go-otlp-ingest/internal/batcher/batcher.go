package batcher

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/atish/go-otlp-ingest/internal/mapper"
)

var (
	ErrBackpressure = errors.New("ingest queue is full")
	ErrUnavailable  = errors.New("clickhouse writer is unavailable")
	ErrShutdown     = errors.New("ingest is shutting down")
)

type Writer interface {
	Insert(ctx context.Context, batch mapper.Batch) error
}

type Batcher struct {
	writer    Writer
	batchSize int
	capacity  int
	interval  time.Duration
	log       *slog.Logger

	mu         sync.Mutex
	pending    mapper.Batch
	pendingN   int
	shutdown   bool
	insertFail bool

	wake   chan struct{}
	done   chan struct{}
	closed chan struct{}
}

func New(writer Writer, batchSize, capacity int, interval time.Duration, log *slog.Logger) *Batcher {
	if batchSize < 1 {
		batchSize = 1
	}
	if capacity < 1 {
		capacity = 1
	}
	if interval <= 0 {
		interval = time.Second
	}
	if log == nil {
		log = slog.Default()
	}
	b := &Batcher{
		writer:    writer,
		batchSize: batchSize,
		capacity:  capacity,
		interval:  interval,
		log:       log,
		wake:      make(chan struct{}, 1),
		done:      make(chan struct{}),
		closed:    make(chan struct{}),
	}
	go b.loop()
	return b
}

func (b *Batcher) Enqueue(batch mapper.Batch) error {
	n := batch.Len()
	if n == 0 {
		return nil
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.shutdown {
		return ErrShutdown
	}
	if b.insertFail {
		return ErrUnavailable
	}
	if b.pendingN+n > b.capacity {
		return ErrBackpressure
	}
	b.pending.Append(batch)
	b.pendingN += n
	if b.pendingN >= b.batchSize {
		b.signal()
	}
	return nil
}

func (b *Batcher) Close(ctx context.Context) error {
	b.mu.Lock()
	if !b.shutdown {
		b.shutdown = true
		close(b.done)
	}
	b.mu.Unlock()

	select {
	case <-b.closed:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *Batcher) Unhealthy() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.insertFail || b.shutdown
}

func (b *Batcher) loop() {
	defer close(b.closed)
	ticker := time.NewTicker(b.interval)
	defer ticker.Stop()

	for {
		select {
		case <-b.done:
			b.flushUntilEmpty(context.Background())
			return
		case <-ticker.C:
			b.flushOnce(context.Background())
		case <-b.wake:
			b.flushOnce(context.Background())
		}
	}
}

func (b *Batcher) takeIf(min int) (mapper.Batch, int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.pendingN == 0 || b.pendingN < min {
		return mapper.Batch{}, 0
	}
	n := b.pendingN
	out := b.pending.Take()
	b.pendingN = 0
	return out, n
}

func (b *Batcher) flushOnce(ctx context.Context) {
	batch, n := b.takeIf(1)
	if n == 0 {
		return
	}
	b.send(ctx, batch, n)
}

func (b *Batcher) flushUntilEmpty(ctx context.Context) {
	for {
		batch, n := b.takeIf(1)
		if n == 0 {
			return
		}
		b.send(ctx, batch, n)
	}
}

func (b *Batcher) send(ctx context.Context, batch mapper.Batch, n int) {
	backoff := 100 * time.Millisecond
	for {
		err := b.writer.Insert(ctx, batch)
		if err == nil {
			b.mu.Lock()
			b.insertFail = false
			b.mu.Unlock()
			return
		}
		b.mu.Lock()
		b.insertFail = true
		shut := b.shutdown
		b.mu.Unlock()
		b.log.Error("clickhouse insert failed", "err", err, "rows", n)
		if shut {
			return
		}
		timer := time.NewTimer(backoff)
		select {
		case <-b.done:
			timer.Stop()
			_ = b.writer.Insert(context.Background(), batch)
			return
		case <-timer.C:
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (b *Batcher) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}
