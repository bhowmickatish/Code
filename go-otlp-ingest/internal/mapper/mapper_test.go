package mapper

import (
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func TestMapGaugeAndSum(t *testing.T) {
	recv := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	ts := uint64(recv.Add(time.Second).UnixNano())
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					kvString("service.name", "inventory-api"),
					kvInt("pid", 42),
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "lib", Version: "1.0"},
				Metrics: []*metricspb.Metric{
					{
						Name: "queue.depth",
						Unit: "1",
						Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
							DataPoints: []*metricspb.NumberDataPoint{{
								Attributes:   []*commonpb.KeyValue{kvString("device.id", "dev-9")},
								TimeUnixNano: ts,
								Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 7},
							}},
						}},
					},
					{
						Name: "requests.total",
						Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
							AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
							IsMonotonic:            true,
							DataPoints: []*metricspb.NumberDataPoint{{
								TimeUnixNano: ts,
								Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.5},
							}},
						}},
					},
				},
			}},
		}},
	}

	got := Map(req, recv, Limits{MaxAttrKeys: 64, MaxAttrValue: 1024})
	if got.Rejected != 0 {
		t.Fatalf("rejected=%d msg=%s", got.Rejected, got.ErrorMessage)
	}
	if len(got.Batch.Gauges) != 1 || got.Batch.Gauges[0].Value != 7 {
		t.Fatalf("gauges=%v", got.Batch.Gauges)
	}
	g := got.Batch.Gauges[0]
	if g.ServiceName != "inventory-api" || g.Attributes["device.id"] != "dev-9" {
		t.Fatalf("entity fields: %+v", g)
	}
	if g.ResourceAttributes["pid"] != "42" {
		t.Fatalf("pid stringify: %v", g.ResourceAttributes)
	}
	if len(got.Batch.Sums) != 1 || got.Batch.Sums[0].Value != 1.5 || !got.Batch.Sums[0].IsMonotonic {
		t.Fatalf("sums=%v", got.Batch.Sums)
	}
}

func TestMissingTimestampUsesReceiveTime(t *testing.T) {
	recv := time.Date(2026, 9, 10, 12, 0, 0, 0, time.UTC)
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "n",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1},
						}},
					}},
				}},
			}},
		}},
	}
	got := Map(req, recv, Limits{MaxAttrKeys: 8, MaxAttrValue: 32})
	if got.Batch.Gauges[0].TimeUnix != recv {
		t.Fatalf("time=%v want %v", got.Batch.Gauges[0].TimeUnix, recv)
	}
}

func TestAttrCapsAndBytesHex(t *testing.T) {
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "n",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							TimeUnixNano: 1,
							Attributes: []*commonpb.KeyValue{
								kvString("a", "12345"),
								kvString("b", "keep"),
								{Key: "c", Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BytesValue{BytesValue: []byte{0xde, 0xad}}}},
							},
						}},
					}},
				}},
			}},
		}},
	}
	got := Map(req, time.Now().UTC(), Limits{MaxAttrKeys: 2, MaxAttrValue: 3})
	attrs := got.Batch.Gauges[0].Attributes
	if len(attrs) != 2 {
		t.Fatalf("want 2 keys, got %v", attrs)
	}
	if attrs["a"] != "123" {
		t.Fatalf("truncate a: %q", attrs["a"])
	}
	if _, ok := attrs["c"]; ok {
		t.Fatalf("c should be dropped after key cap: %v", attrs)
	}
}

func TestUnknownMetricTypeRejected(t *testing.T) {
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{Name: "empty"}},
			}},
		}},
	}
	got := Map(req, time.Now().UTC(), Limits{MaxAttrKeys: 8, MaxAttrValue: 32})
	if got.Rejected != 1 {
		t.Fatalf("rejected=%d", got.Rejected)
	}
	if got.Batch.Len() != 0 {
		t.Fatalf("batch=%d", got.Batch.Len())
	}
}

func TestCountDataPoints(t *testing.T) {
	req := &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{
					{Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{}, {}},
					}}},
					{Name: "unknown"},
				},
			}},
		}},
	}
	if n := CountDataPoints(req); n != 3 {
		t.Fatalf("count=%d", n)
	}
}

func kvString(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

func kvInt(k string, v int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: v}},
	}
}
