package batcher

import (
	"context"
	"errors"
	"fmt"
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
	Insert(ctx context.Context, batch mapper.Batch) (remaining mapper.Batch, err error)
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
	flushErr   error
	waiters    []chan error

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

// Enqueue appends rows and blocks until they are successfully inserted into ClickHouse
// (or ctx is canceled). Export RPC success implies durable handoff to ClickHouse.
func (b *Batcher) Enqueue(ctx context.Context, batch mapper.Batch) error {
	n := batch.Len()
	if n == 0 {
		return nil
	}

	waitCh := make(chan error, 1)

	b.mu.Lock()
	if b.shutdown {
		b.mu.Unlock()
		return ErrShutdown
	}
	if b.insertFail {
		b.mu.Unlock()
		return ErrUnavailable
	}
	if b.pendingN+n > b.capacity {
		b.mu.Unlock()
		return ErrBackpressure
	}
	b.pending.Append(batch)
	b.pendingN += n
	b.waiters = append(b.waiters, waitCh)
	if b.pendingN >= b.batchSize {
		b.signalLocked()
	}
	b.mu.Unlock()

	b.signal()

	select {
	case err := <-waitCh:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
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
		b.mu.Lock()
		err := b.flushErr
		b.mu.Unlock()
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// IngestReady reports whether new Export RPCs should be accepted.
// False when ClickHouse inserts are failing or the batcher is shutting down.
func (b *Batcher) IngestReady() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.insertFail && !b.shutdown
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
	b.send(ctx, batch, n, false)
}

func (b *Batcher) flushUntilEmpty(ctx context.Context) {
	for {
		batch, n := b.takeIf(1)
		if n == 0 {
			return
		}
		b.send(ctx, batch, n, true)
	}
}

func (b *Batcher) send(ctx context.Context, batch mapper.Batch, n int, onShutdown bool) {
	remaining := batch
	backoff := 100 * time.Millisecond
	deadline := time.Time{}
	if onShutdown {
		deadline = time.Now().Add(10 * time.Second)
	}

	for remaining.Len() > 0 {
		var err error
		remaining, err = b.writer.Insert(ctx, remaining)
		if err == nil {
			b.setInsertFail(false)
			b.completeWaiters(nil)
			return
		}

		b.setInsertFail(true)
		b.log.Error("clickhouse insert failed",
			"err", err,
			"rows", remaining.Len(),
			"shutdown", onShutdown,
		)

		if onShutdown {
			if time.Now().After(deadline) {
				b.recordFlushDrop(remaining.Len(), err)
				return
			}
			time.Sleep(100 * time.Millisecond)
			continue
		}

		timer := time.NewTimer(backoff)
		select {
		case <-b.done:
			timer.Stop()
			b.send(context.Background(), remaining, remaining.Len(), true)
			return
		case <-timer.C:
		}
		if backoff < 5*time.Second {
			backoff *= 2
		}
	}
}

func (b *Batcher) completeWaiters(err error) {
	b.mu.Lock()
	waiters := b.waiters
	b.waiters = nil
	b.mu.Unlock()
	for _, ch := range waiters {
		ch <- err
	}
}

func (b *Batcher) setInsertFail(failed bool) {
	b.mu.Lock()
	b.insertFail = failed
	b.mu.Unlock()
}

func (b *Batcher) recordFlushDrop(rows int, err error) {
	b.mu.Lock()
	b.insertFail = true
	flushErr := fmt.Errorf("shutdown flush dropped %d rows: %w", rows, err)
	if b.flushErr == nil {
		b.flushErr = flushErr
	}
	b.mu.Unlock()
	b.log.Error("shutdown flush dropped rows", "rows", rows, "err", err)
	b.completeWaiters(flushErr)
}

func (b *Batcher) signal() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}

func (b *Batcher) signalLocked() {
	select {
	case b.wake <- struct{}{}:
	default:
	}
}
