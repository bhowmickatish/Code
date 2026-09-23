package batcher

import (
	"context"
	"errors"
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

func (f *fakeWriter) Insert(ctx context.Context, batch mapper.Batch) (mapper.Batch, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.fail != nil {
		return batch, f.fail
	}
	f.rows += batch.Len()
	return mapper.Batch{}, nil
}

func TestEnqueueFlushBySize(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, 2, 10, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	ctx := context.Background()
	row := mapper.Batch{Gauges: []mapper.GaugeRow{{}}}
	go func() {
		if err := b.Enqueue(ctx, row); err != nil {
			t.Error(err)
		}
	}()
	if err := b.Enqueue(ctx, row); err != nil {
		t.Fatal(err)
	}
	w.mu.Lock()
	n := w.rows
	w.mu.Unlock()
	if n != 2 {
		t.Fatalf("rows=%d want 2", n)
	}
}

func TestBackpressure(t *testing.T) {
	w := &fakeWriter{}
	b := New(w, 100, 1, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	twoRows := mapper.Batch{Gauges: []mapper.GaugeRow{{}, {}}}
	if err := b.Enqueue(context.Background(), twoRows); err != ErrBackpressure {
		t.Fatalf("got %v want ErrBackpressure", err)
	}
}

func TestFlushChunksByBatchSize(t *testing.T) {
	w := &chunkTrackingWriter{}
	b := New(w, 2, 10, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	five := mapper.Batch{Gauges: []mapper.GaugeRow{{}, {}, {}, {}, {}}}
	if err := b.Enqueue(context.Background(), five); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.totalRows != 5 {
		t.Fatalf("rows=%d want 5", w.totalRows)
	}
	if w.maxInsert > 2 {
		t.Fatalf("maxInsert=%d want <= 2", w.maxInsert)
	}
	if w.calls < 3 {
		t.Fatalf("insert calls=%d want at least 3", w.calls)
	}
}

type chunkTrackingWriter struct {
	mu        sync.Mutex
	calls     int
	totalRows int
	maxInsert int
}

func (w *chunkTrackingWriter) Insert(ctx context.Context, batch mapper.Batch) (mapper.Batch, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n := batch.Len()
	w.calls++
	w.totalRows += n
	if n > w.maxInsert {
		w.maxInsert = n
	}
	return mapper.Batch{}, nil
}

func TestPartialInsertRetriesRemainingOnly(t *testing.T) {
	w := &trackingPartialWriter{}
	b := New(w, 10, 10, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	batch := mapper.Batch{
		Gauges: []mapper.GaugeRow{{}, {}},
		Sums:   []mapper.SumRow{{}},
	}
	if err := b.Enqueue(context.Background(), batch); err != nil {
		t.Fatal(err)
	}

	w.mu.Lock()
	gaugeCalls := w.gaugeCalls
	sumCalls := w.sumCalls
	w.mu.Unlock()
	if gaugeCalls != 1 || sumCalls != 1 {
		t.Fatalf("gaugeCalls=%d sumCalls=%d", gaugeCalls, sumCalls)
	}
}

func TestIngestReadyReflectsInsertFail(t *testing.T) {
	w := &fakeWriter{fail: errors.New("down")}
	b := New(w, 1, 10, time.Millisecond, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	if !b.IngestReady() {
		t.Fatal("expected ready before failure")
	}
	go func() {
		_ = b.Enqueue(context.Background(), mapper.Batch{Gauges: []mapper.GaugeRow{{}}})
	}()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if !b.IngestReady() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected ingest blocked after insert failure")
}

type trackingPartialWriter struct {
	mu         sync.Mutex
	gaugeCalls int
	sumCalls   int
}

func (w *trackingPartialWriter) Insert(ctx context.Context, batch mapper.Batch) (mapper.Batch, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if len(batch.Gauges) > 0 {
		w.gaugeCalls++
		remaining := batch
		remaining.Gauges = nil
		if remaining.Len() == 0 {
			return mapper.Batch{}, nil
		}
		return remaining, errors.New("sum failed")
	}
	if len(batch.Sums) > 0 {
		w.sumCalls++
		return mapper.Batch{}, nil
	}
	return mapper.Batch{}, nil
}
