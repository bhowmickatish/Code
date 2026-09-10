package otlpgrpc

import (
	"context"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"

	"github.com/atish/go-otlp-ingest/internal/batcher"
	"github.com/atish/go-otlp-ingest/internal/mapper"
)

type sink struct {
	n int
}

func (s *sink) Insert(ctx context.Context, batch mapper.Batch) error {
	s.n += batch.Len()
	return nil
}

func TestExportEmpty(t *testing.T) {
	b := batcher.New(&sink{}, 10, 100, time.Hour, nil)
	t.Cleanup(func() { closeBatcher(t, b) })
	srv := New(b, 100, mapper.Limits{MaxAttrKeys: 8, MaxAttrValue: 32})
	_, err := srv.Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code=%v err=%v", status.Code(err), err)
	}
}

func TestExportGauge(t *testing.T) {
	b := batcher.New(&sink{}, 1, 100, time.Hour, nil)
	t.Cleanup(func() { closeBatcher(t, b) })
	srv := New(b, 100, mapper.Limits{MaxAttrKeys: 8, MaxAttrValue: 32})
	if _, err := srv.Export(context.Background(), gaugeReq()); err != nil {
		t.Fatal(err)
	}
}

func TestExportTooMany(t *testing.T) {
	b := batcher.New(&sink{}, 10, 100, time.Hour, nil)
	t.Cleanup(func() { closeBatcher(t, b) })
	srv := New(b, 1, mapper.Limits{MaxAttrKeys: 8, MaxAttrValue: 32})
	req := gaugeReq()
	req.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints = append(
		req.ResourceMetrics[0].ScopeMetrics[0].Metrics[0].GetGauge().DataPoints,
		&metricspb.NumberDataPoint{Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 2}},
	)
	_, err := srv.Export(context.Background(), req)
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("code=%v", status.Code(err))
	}
}

func closeBatcher(t *testing.T, b *batcher.Batcher) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := b.Close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

func gaugeReq() *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Metrics: []*metricspb.Metric{{
					Name: "n",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							TimeUnixNano: 1,
							Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 1},
							Attributes: []*commonpb.KeyValue{{
								Key:   "device.id",
								Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "dev-9"}},
							}},
						}},
					}},
				}},
			}},
		}},
	}
}
