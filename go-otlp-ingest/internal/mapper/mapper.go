package mapper

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

const serviceNameKey = "service.name"

func CountDataPoints(req *colmetricspb.ExportMetricsServiceRequest) int {
	if req == nil {
		return 0
	}
	n := 0
	for _, rm := range req.ResourceMetrics {
		if rm == nil {
			continue
		}
		for _, sm := range rm.ScopeMetrics {
			if sm == nil {
				continue
			}
			for _, m := range sm.Metrics {
				n += metricPointCount(m)
			}
		}
	}
	return n
}

func metricPointCount(m *metricspb.Metric) int {
	if m == nil {
		return 0
	}
	switch d := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		if d.Gauge == nil {
			return 0
		}
		return len(d.Gauge.DataPoints)
	case *metricspb.Metric_Sum:
		if d.Sum == nil {
			return 0
		}
		return len(d.Sum.DataPoints)
	case *metricspb.Metric_Histogram:
		if d.Histogram == nil {
			return 0
		}
		return len(d.Histogram.DataPoints)
	case *metricspb.Metric_ExponentialHistogram:
		if d.ExponentialHistogram == nil {
			return 0
		}
		return len(d.ExponentialHistogram.DataPoints)
	case *metricspb.Metric_Summary:
		if d.Summary == nil {
			return 0
		}
		return len(d.Summary.DataPoints)
	default:
		return 1
	}
}

func Map(req *colmetricspb.ExportMetricsServiceRequest, recv time.Time, lim Limits) Result {
	var out Result
	if req == nil {
		return out
	}
	if lim.MaxAttrKeys <= 0 {
		lim.MaxAttrKeys = 64
	}
	if lim.MaxAttrValue <= 0 {
		lim.MaxAttrValue = 1024
	}

	var rejectReasons []string

	for _, rm := range req.ResourceMetrics {
		if rm == nil {
			continue
		}
		resAttrs, serviceName := resourceAttrs(rm.Resource, lim)
		for _, sm := range rm.ScopeMetrics {
			if sm == nil {
				continue
			}
			scopeName, scopeVersion := "", ""
			if sm.Scope != nil {
				scopeName = sm.Scope.Name
				scopeVersion = sm.Scope.Version
			}
			for _, m := range sm.Metrics {
				if m == nil {
					continue
				}
				mapMetric(m, resAttrs, serviceName, scopeName, scopeVersion, recv, lim, &out, &rejectReasons)
			}
		}
	}

	if out.Rejected > 0 {
		out.ErrorMessage = strings.Join(unique(rejectReasons), "; ")
	}
	return out
}

func mapMetric(
	m *metricspb.Metric,
	resAttrs map[string]string,
	serviceName, scopeName, scopeVersion string,
	recv time.Time,
	lim Limits,
	out *Result,
	reasons *[]string,
) {
	base := func(attrs []*commonpb.KeyValue, start, ts uint64) Common {
		return Common{
			TimeUnix:           timestamp(ts, recv),
			StartTimeUnix:      timestamp(start, time.Time{}),
			ServiceName:        serviceName,
			MetricName:         m.Name,
			MetricDescription:  m.Description,
			MetricUnit:         m.Unit,
			ResourceAttributes: resAttrs,
			ScopeName:          scopeName,
			ScopeVersion:       scopeVersion,
			Attributes:         attrMap(attrs, lim),
		}
	}

	switch d := m.Data.(type) {
	case *metricspb.Metric_Gauge:
		if d.Gauge == nil {
			return
		}
		for _, p := range d.Gauge.DataPoints {
			if p == nil {
				continue
			}
			out.Batch.Gauges = append(out.Batch.Gauges, GaugeRow{
				Common: base(p.Attributes, p.StartTimeUnixNano, p.TimeUnixNano),
				Value:  numberValue(p),
			})
		}
	case *metricspb.Metric_Sum:
		if d.Sum == nil {
			return
		}
		for _, p := range d.Sum.DataPoints {
			if p == nil {
				continue
			}
			out.Batch.Sums = append(out.Batch.Sums, SumRow{
				Common:                 base(p.Attributes, p.StartTimeUnixNano, p.TimeUnixNano),
				Value:                  numberValue(p),
				AggregationTemporality: int32(d.Sum.AggregationTemporality),
				IsMonotonic:            d.Sum.IsMonotonic,
			})
		}
	case *metricspb.Metric_Histogram:
		if d.Histogram == nil {
			return
		}
		for _, p := range d.Histogram.DataPoints {
			if p == nil {
				continue
			}
			out.Batch.Histograms = append(out.Batch.Histograms, HistogramRow{
				Common:         base(p.Attributes, p.StartTimeUnixNano, p.TimeUnixNano),
				Count:          p.Count,
				Sum:            derefF64(p.Sum),
				Min:            derefF64(p.Min),
				Max:            derefF64(p.Max),
				BucketCounts:   p.BucketCounts,
				ExplicitBounds: p.ExplicitBounds,
			})
		}
	case *metricspb.Metric_ExponentialHistogram:
		if d.ExponentialHistogram == nil {
			return
		}
		for _, p := range d.ExponentialHistogram.DataPoints {
			if p == nil {
				continue
			}
			row := ExpHistogramRow{
				Common:    base(p.Attributes, p.StartTimeUnixNano, p.TimeUnixNano),
				Count:     p.Count,
				Sum:       derefF64(p.Sum),
				Scale:     p.Scale,
				ZeroCount: p.ZeroCount,
			}
			if p.Positive != nil {
				row.PositiveOffset = p.Positive.Offset
				row.PositiveBucketCounts = p.Positive.BucketCounts
			}
			if p.Negative != nil {
				row.NegativeOffset = p.Negative.Offset
				row.NegativeBucketCounts = p.Negative.BucketCounts
			}
			out.Batch.ExpHistograms = append(out.Batch.ExpHistograms, row)
		}
	case *metricspb.Metric_Summary:
		if d.Summary == nil {
			return
		}
		for _, p := range d.Summary.DataPoints {
			if p == nil {
				continue
			}
			qs := make([]float64, 0, len(p.QuantileValues))
			vs := make([]float64, 0, len(p.QuantileValues))
			for _, q := range p.QuantileValues {
				if q == nil {
					continue
				}
				qs = append(qs, q.Quantile)
				vs = append(vs, q.Value)
			}
			out.Batch.Summaries = append(out.Batch.Summaries, SummaryRow{
				Common:    base(p.Attributes, p.StartTimeUnixNano, p.TimeUnixNano),
				Count:     p.Count,
				Sum:       p.Sum,
				Quantiles: qs,
				Values:    vs,
			})
		}
	default:
		n := metricPointCount(m)
		if n < 1 {
			n = 1
		}
		out.Rejected += int64(n)
		*reasons = append(*reasons, "unknown metric type for "+m.Name)
	}
}

func resourceAttrs(res *resourcepb.Resource, lim Limits) (map[string]string, string) {
	if res == nil {
		return map[string]string{}, ""
	}
	m := attrMap(res.Attributes, lim)
	return m, m[serviceNameKey]
}

func attrMap(kvs []*commonpb.KeyValue, lim Limits) map[string]string {
	out := make(map[string]string, min(len(kvs), lim.MaxAttrKeys))
	for _, kv := range kvs {
		if kv == nil || kv.Key == "" {
			continue
		}
		if len(out) >= lim.MaxAttrKeys {
			break
		}
		out[kv.Key] = truncate(anyValueString(kv.Value), lim.MaxAttrValue)
	}
	return out
}

func anyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch x := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(x.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(x.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(x.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		if x.ArrayValue == nil {
			return "[]"
		}
		items := make([]any, 0, len(x.ArrayValue.Values))
		for _, el := range x.ArrayValue.Values {
			items = append(items, anyJSON(el))
		}
		b, err := json.Marshal(items)
		if err != nil {
			return "[]"
		}
		return string(b)
	case *commonpb.AnyValue_KvlistValue:
		if x.KvlistValue == nil {
			return "{}"
		}
		obj := make(map[string]any, len(x.KvlistValue.Values))
		for _, kv := range x.KvlistValue.Values {
			if kv == nil {
				continue
			}
			obj[kv.Key] = anyJSON(kv.Value)
		}
		b, err := json.Marshal(obj)
		if err != nil {
			return "{}"
		}
		return string(b)
	default:
		return ""
	}
}

func anyJSON(v *commonpb.AnyValue) any {
	if v == nil {
		return nil
	}
	switch x := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return x.StringValue
	case *commonpb.AnyValue_BoolValue:
		return x.BoolValue
	case *commonpb.AnyValue_IntValue:
		return x.IntValue
	case *commonpb.AnyValue_DoubleValue:
		return x.DoubleValue
	case *commonpb.AnyValue_BytesValue:
		return hex.EncodeToString(x.BytesValue)
	case *commonpb.AnyValue_ArrayValue:
		if x.ArrayValue == nil {
			return []any{}
		}
		items := make([]any, 0, len(x.ArrayValue.Values))
		for _, el := range x.ArrayValue.Values {
			items = append(items, anyJSON(el))
		}
		return items
	case *commonpb.AnyValue_KvlistValue:
		if x.KvlistValue == nil {
			return map[string]any{}
		}
		obj := make(map[string]any, len(x.KvlistValue.Values))
		for _, kv := range x.KvlistValue.Values {
			if kv == nil {
				continue
			}
			obj[kv.Key] = anyJSON(kv.Value)
		}
		return obj
	default:
		return nil
	}
}

func numberValue(p *metricspb.NumberDataPoint) float64 {
	if p == nil {
		return 0
	}
	switch n := p.Value.(type) {
	case *metricspb.NumberDataPoint_AsDouble:
		return n.AsDouble
	case *metricspb.NumberDataPoint_AsInt:
		return float64(n.AsInt)
	default:
		return 0
	}
}

func timestamp(unixNano uint64, fallback time.Time) time.Time {
	if unixNano == 0 {
		return fallback
	}
	return time.Unix(0, int64(unixNano)).UTC()
}

func derefF64(p *float64) float64 {
	if p == nil {
		return 0
	}
	return *p
}

func truncate(s string, max int) string {
	if max <= 0 || len(s) <= max {
		return s
	}
	return s[:max]
}

func unique(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	var out []string
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
