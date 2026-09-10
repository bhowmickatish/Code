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
	Sum            float64
	Min            float64
	Max            float64
	BucketCounts   []uint64
	ExplicitBounds []float64
}

type ExpHistogramRow struct {
	Common
	Count                uint64
	Sum                  float64
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
