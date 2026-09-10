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

func TestPartialInsertRetriesRemainingOnly(t *testing.T) {
	w := &trackingPartialWriter{}
	b := New(w, 1, 10, time.Hour, nil)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = b.Close(ctx)
	})

	batch := mapper.Batch{
		Gauges: []mapper.GaugeRow{{}, {}},
		Sums:   []mapper.SumRow{{}},
	}
	if err := b.Enqueue(batch); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		w.mu.Lock()
		gaugeCalls := w.gaugeCalls
		sumCalls := w.sumCalls
		w.mu.Unlock()
		if gaugeCalls == 1 && sumCalls == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	t.Fatalf("gaugeCalls=%d sumCalls=%d", w.gaugeCalls, w.sumCalls)
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
	if err := b.Enqueue(mapper.Batch{Gauges: []mapper.GaugeRow{{}}}); err != nil {
		t.Fatal(err)
	}
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
		return remaining, errors.New("sum failed")
	}
	if len(batch.Sums) > 0 {
		w.sumCalls++
		return mapper.Batch{}, nil
	}
	return mapper.Batch{}, nil
}
