package mapper

import "testing"

func TestBatchTakeUpTo(t *testing.T) {
	var b Batch
	b.Gauges = []GaugeRow{{}, {}}
	b.Sums = []SumRow{{}}

	out, n := b.TakeUpTo(2)
	if n != 2 || len(out.Gauges) != 2 || len(out.Sums) != 0 {
		t.Fatalf("first take: n=%d out=%v pending=%d", n, out.Len(), b.Len())
	}
	if b.Len() != 1 {
		t.Fatalf("pending=%d want 1", b.Len())
	}

	out2, n2 := b.TakeUpTo(10)
	if n2 != 1 || out2.Len() != 1 || b.Len() != 0 {
		t.Fatalf("second take: n2=%d pending=%d", n2, b.Len())
	}
}
