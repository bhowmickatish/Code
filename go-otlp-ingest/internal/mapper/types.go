package mapper

import "time"

type Limits struct {
	MaxAttrKeys  int
	MaxAttrValue int
}

type Batch struct {
	Gauges        []GaugeRow
	Sums          []SumRow
	Histograms    []HistogramRow
	ExpHistograms []ExpHistogramRow
	Summaries     []SummaryRow
}

func (b Batch) Len() int {
	return len(b.Gauges) + len(b.Sums) + len(b.Histograms) + len(b.ExpHistograms) + len(b.Summaries)
}

func (b *Batch) Append(other Batch) {
	b.Gauges = append(b.Gauges, other.Gauges...)
	b.Sums = append(b.Sums, other.Sums...)
	b.Histograms = append(b.Histograms, other.Histograms...)
	b.ExpHistograms = append(b.ExpHistograms, other.ExpHistograms...)
	b.Summaries = append(b.Summaries, other.Summaries...)
}

func (b *Batch) Take() Batch {
	out := *b
	*b = Batch{}
	return out
}

// TakeUpTo removes up to max rows from the batch (gauges first, then sums, etc.) and returns them.
func (b *Batch) TakeUpTo(max int) (Batch, int) {
	if max <= 0 || b.Len() == 0 {
		return Batch{}, 0
	}
	var out Batch
	taken := 0
	rem := max

	if n := min(rem, len(b.Gauges)); n > 0 {
		out.Gauges = append(out.Gauges, b.Gauges[:n]...)
		b.Gauges = b.Gauges[n:]
		taken += n
		rem -= n
	}
	if rem > 0 {
		if n := min(rem, len(b.Sums)); n > 0 {
			out.Sums = append(out.Sums, b.Sums[:n]...)
			b.Sums = b.Sums[n:]
			taken += n
			rem -= n
		}
	}
	if rem > 0 {
		if n := min(rem, len(b.Histograms)); n > 0 {
			out.Histograms = append(out.Histograms, b.Histograms[:n]...)
			b.Histograms = b.Histograms[n:]
			taken += n
			rem -= n
		}
	}
	if rem > 0 {
		if n := min(rem, len(b.ExpHistograms)); n > 0 {
			out.ExpHistograms = append(out.ExpHistograms, b.ExpHistograms[:n]...)
			b.ExpHistograms = b.ExpHistograms[n:]
			taken += n
			rem -= n
		}
	}
	if rem > 0 {
		if n := min(rem, len(b.Summaries)); n > 0 {
			out.Summaries = append(out.Summaries, b.Summaries[:n]...)
			b.Summaries = b.Summaries[n:]
			taken += n
		}
	}
	return out, taken
}

type Common struct {
	TimeUnix           time.Time
	StartTimeUnix      time.Time
	ServiceName        string
	MetricName         string
	MetricDescription  string
	MetricUnit         string
	ResourceAttributes map[string]string
	ScopeName          string
	ScopeVersion       string
	Attributes         map[string]string
}

type GaugeRow struct {
	Common
	Value float64
}

type SumRow struct {
	Common
	Value                  float64
	AggregationTemporality int32
	IsMonotonic            bool
}

type HistogramRow struct {
	Common
	Count          uint64
	Sum            *float64
	Min            *float64
	Max            *float64
	BucketCounts   []uint64
	ExplicitBounds []float64
}

type ExpHistogramRow struct {
	Common
	Count                uint64
	Sum                  *float64
	Scale                int32
	ZeroCount            uint64
	PositiveOffset       int32
	PositiveBucketCounts []uint64
	NegativeOffset       int32
	NegativeBucketCounts []uint64
}

type SummaryRow struct {
	Common
	Count     uint64
	Sum       float64
	Quantiles []float64
	Values    []float64
}

type Result struct {
	Batch        Batch
	Rejected     int64
	ErrorMessage string
}
