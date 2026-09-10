package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

func main() {
	addr := envOr("OTLP_ENDPOINT", "127.0.0.1:4317")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fatal(err)
	}
	defer conn.Close()

	client := colmetricspb.NewMetricsServiceClient(conn)
	now := uint64(time.Now().UnixNano())
	_, err = client.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{
				Attributes: []*commonpb.KeyValue{
					strKV("service.name", "sample-client"),
				},
			},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope: &commonpb.InstrumentationScope{Name: "go-otlp-ingest/cmd/client"},
				Metrics: []*metricspb.Metric{{
					Name:        "sample.gauge",
					Description: "sample gauge from cmd/client",
					Unit:        "1",
					Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{
							TimeUnixNano: now,
							Value:        &metricspb.NumberDataPoint_AsDouble{AsDouble: 1},
							Attributes: []*commonpb.KeyValue{
								strKV("device.id", "dev-9"),
							},
						}},
					}},
				}},
			}},
		}},
	})
	if err != nil {
		fatal(err)
	}
	fmt.Println("exported sample.gauge device.id=dev-9")
}

func strKV(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{
		Key:   k,
		Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}},
	}
}

func envOr(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
