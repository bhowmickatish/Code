package batcher

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/atish/go-otlp-ingest/internal/mapper"
)

type fakeWriter struct {
	mu    sync.Mutex
	rows  int
	fail  error
	calls int
}

func (f *fakeWriter) Insert(ctx context.Context, batch mapper.Batch) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail != nil {
		return f.fail
	}
	f.rows += batch.Len()
	return nil
}

func TestEnqueueFlushBySize(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, 2, 10, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	row := mapper.Batch{Gauges: []mapper.GaugeRow{{}}}
	if err := b.Enqueue(row); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(row); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		n := w.rows
		w.mu.Unlock()
		if n == 2 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("did not flush")
}

func TestBackpressure(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, 100, 1, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	row := mapper.Batch{Gauges: []mapper.GaugeRow{{}}}
	if err := b.Enqueue(row); err != nil {
		t.Fatal(err)
	}
	if err := b.Enqueue(row); err != ErrBackpressure {
		t.Fatalf("got %v want ErrBackpressure", err)
	}
}
