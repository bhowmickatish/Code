package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/atish/go-otlp-ingest/internal/mapper"
)

const (
	tableGauge        = "otel_metrics_gauge"
	tableSum          = "otel_metrics_sum"
	tableHistogram    = "otel_metrics_histogram"
	tableExpHistogram = "otel_metrics_exp_histogram"
	tableSummary      = "otel_metrics_summary"
)

const sharedCols = `
    ResourceAttributes Map(LowCardinality(String), String),
    ScopeName LowCardinality(String),
    ScopeVersion LowCardinality(String),
    ServiceName LowCardinality(String),
    MetricName LowCardinality(String),
    MetricDescription String,
    MetricUnit LowCardinality(String),
    Attributes Map(LowCardinality(String), String),
    StartTimeUnix DateTime64(9),
    TimeUnix DateTime64(9)
`

const engine = `
ENGINE = MergeTree
PARTITION BY toDate(TimeUnix)
ORDER BY (ServiceName, MetricName, toUnixTimestamp(TimeUnix), cityHash64(Attributes))
`

type Store struct {
	conn driver.Conn
}

func Open(ctx context.Context, dsn string) (*Store, error) {
	opts, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("clickhouse dsn: %w", err)
	}
	if opts.Settings == nil {
		opts.Settings = clickhouse.Settings{}
	}
	opts.Settings["async_insert"] = 1
	opts.Settings["wait_for_async_insert"] = 1
	if opts.DialTimeout == 0 {
		opts.DialTimeout = 5 * time.Second
	}

	db := opts.Auth.Database
	if db == "" {
		db = "default"
	}
	opts.Auth.Database = "default"
	bootstrap, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	if err := bootstrap.Ping(ctx); err != nil {
		_ = bootstrap.Close()
		return nil, err
	}
	if db != "default" {
		if err := bootstrap.Exec(ctx, "CREATE DATABASE IF NOT EXISTS "+quoteIdent(db)); err != nil {
			_ = bootstrap.Close()
			return nil, fmt.Errorf("create database: %w", err)
		}
	}
	_ = bootstrap.Close()

	opts.Auth.Database = db
	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	s := &Store{conn: conn}
	if err := s.ensureSchema(ctx); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error {
	if s == nil || s.conn == nil {
		return nil
	}
	return s.conn.Close()
}

func (s *Store) Ping(ctx context.Context) error {
	return s.conn.Ping(ctx)
}

func (s *Store) ensureSchema(ctx context.Context) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS ` + tableGauge + ` (` + sharedCols + `, Value Float64) ` + engine,
		`CREATE TABLE IF NOT EXISTS ` + tableSum + ` (` + sharedCols + `,
			Value Float64,
			AggregationTemporality Int32,
			IsMonotonic Bool
		) ` + engine,
		`CREATE TABLE IF NOT EXISTS ` + tableHistogram + ` (` + sharedCols + `,
			Count UInt64,
			Sum Nullable(Float64),
			Min Nullable(Float64),
			Max Nullable(Float64),
			BucketCounts Array(UInt64),
			ExplicitBounds Array(Float64)
		) ` + engine,
		`CREATE TABLE IF NOT EXISTS ` + tableExpHistogram + ` (` + sharedCols + `,
			Count UInt64,
			Sum Nullable(Float64),
			Scale Int32,
			ZeroCount UInt64,
			PositiveOffset Int32,
			PositiveBucketCounts Array(UInt64),
			NegativeOffset Int32,
			NegativeBucketCounts Array(UInt64)
		) ` + engine,
		`CREATE TABLE IF NOT EXISTS ` + tableSummary + ` (` + sharedCols + `,
			Count UInt64,
			Sum Float64,
			` + "`ValueAtQuantiles.Quantile`" + ` Array(Float64),
			` + "`ValueAtQuantiles.Value`" + ` Array(Float64)
		) ` + engine,
	}
	for _, q := range stmts {
		if err := s.conn.Exec(ctx, q); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

func (s *Store) Insert(ctx context.Context, batch mapper.Batch) (mapper.Batch, error) {
	remaining := batch

	if len(remaining.Gauges) > 0 {
		if err := insertGauges(ctx, s.conn, remaining.Gauges); err != nil {
			return remaining, fmt.Errorf("gauge: %w", err)
		}
		remaining.Gauges = nil
	}
	if len(remaining.Sums) > 0 {
		if err := insertSums(ctx, s.conn, remaining.Sums); err != nil {
			return remaining, fmt.Errorf("sum: %w", err)
		}
		remaining.Sums = nil
	}
	if len(remaining.Histograms) > 0 {
		if err := insertHistograms(ctx, s.conn, remaining.Histograms); err != nil {
			return remaining, fmt.Errorf("histogram: %w", err)
		}
		remaining.Histograms = nil
	}
	if len(remaining.ExpHistograms) > 0 {
		if err := insertExpHistograms(ctx, s.conn, remaining.ExpHistograms); err != nil {
			return remaining, fmt.Errorf("exp_histogram: %w", err)
		}
		remaining.ExpHistograms = nil
	}
	if len(remaining.Summaries) > 0 {
		if err := insertSummaries(ctx, s.conn, remaining.Summaries); err != nil {
			return remaining, fmt.Errorf("summary: %w", err)
		}
	}
	return mapper.Batch{}, nil
}

func insertGauges(ctx context.Context, conn driver.Conn, rows []mapper.GaugeRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableGauge)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(
			orEmpty(r.ResourceAttributes),
			r.ScopeName,
			r.ScopeVersion,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			orEmpty(r.Attributes),
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
		); err != nil {
			return err
		}
	}
	return b.Send()
}

func insertSums(ctx context.Context, conn driver.Conn, rows []mapper.SumRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableSum)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(
			orEmpty(r.ResourceAttributes),
			r.ScopeName,
			r.ScopeVersion,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			orEmpty(r.Attributes),
			r.StartTimeUnix,
			r.TimeUnix,
			r.Value,
			r.AggregationTemporality,
			r.IsMonotonic,
		); err != nil {
			return err
		}
	}
	return b.Send()
}

func insertHistograms(ctx context.Context, conn driver.Conn, rows []mapper.HistogramRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableHistogram)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(
			orEmpty(r.ResourceAttributes),
			r.ScopeName,
			r.ScopeVersion,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			orEmpty(r.Attributes),
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.Min,
			r.Max,
			orU64(r.BucketCounts),
			orF64(r.ExplicitBounds),
		); err != nil {
			return err
		}
	}
	return b.Send()
}

func insertExpHistograms(ctx context.Context, conn driver.Conn, rows []mapper.ExpHistogramRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableExpHistogram)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(
			orEmpty(r.ResourceAttributes),
			r.ScopeName,
			r.ScopeVersion,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			orEmpty(r.Attributes),
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			r.Scale,
			r.ZeroCount,
			r.PositiveOffset,
			orU64(r.PositiveBucketCounts),
			r.NegativeOffset,
			orU64(r.NegativeBucketCounts),
		); err != nil {
			return err
		}
	}
	return b.Send()
}

func insertSummaries(ctx context.Context, conn driver.Conn, rows []mapper.SummaryRow) error {
	if len(rows) == 0 {
		return nil
	}
	b, err := conn.PrepareBatch(ctx, "INSERT INTO "+tableSummary)
	if err != nil {
		return err
	}
	for _, r := range rows {
		if err := b.Append(
			orEmpty(r.ResourceAttributes),
			r.ScopeName,
			r.ScopeVersion,
			r.ServiceName,
			r.MetricName,
			r.MetricDescription,
			r.MetricUnit,
			orEmpty(r.Attributes),
			r.StartTimeUnix,
			r.TimeUnix,
			r.Count,
			r.Sum,
			orF64(r.Quantiles),
			orF64(r.Values),
		); err != nil {
			return err
		}
	}
	return b.Send()
}

func orEmpty(m map[string]string) map[string]string {
	if m == nil {
		return map[string]string{}
	}
	return m
}

func orU64(s []uint64) []uint64 {
	if s == nil {
		return []uint64{}
	}
	return s
}

func orF64(s []float64) []float64 {
	if s == nil {
		return []float64{}
	}
	return s
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "") + "`"
}
