package otlpgrpc

import (
	"context"
	"errors"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"

	"github.com/atish/go-otlp-ingest/internal/batcher"
	"github.com/atish/go-otlp-ingest/internal/mapper"
)

type Server struct {
	colmetricspb.UnimplementedMetricsServiceServer
	batcher   *batcher.Batcher
	maxPoints int
	limits    mapper.Limits
}

func New(b *batcher.Batcher, maxPoints int, limits mapper.Limits) *Server {
	return &Server{batcher: b, maxPoints: maxPoints, limits: limits}
}

func (s *Server) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, statusFromContext(err)
	}

	n := mapper.CountDataPoints(req)
	if n == 0 {
		return nil, status.Error(codes.InvalidArgument, "empty export request")
	}
	if s.maxPoints > 0 && n > s.maxPoints {
		return nil, status.Error(codes.InvalidArgument, "too many data points")
	}

	res := mapper.Map(req, time.Now().UTC(), s.limits)
	if err := ctx.Err(); err != nil {
		return nil, statusFromContext(err)
	}

	if err := s.batcher.Enqueue(res.Batch); err != nil {
		switch {
		case errors.Is(err, batcher.ErrBackpressure):
			return nil, status.Error(codes.ResourceExhausted, err.Error())
		case errors.Is(err, batcher.ErrUnavailable), errors.Is(err, batcher.ErrShutdown):
			return nil, status.Error(codes.Unavailable, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}

	out := &colmetricspb.ExportMetricsServiceResponse{}
	if res.Rejected > 0 {
		out.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: res.Rejected,
			ErrorMessage:       res.ErrorMessage,
		}
	}
	return out, nil
}

func statusFromContext(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, err.Error())
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, err.Error())
	}
	return status.Error(codes.Internal, err.Error())
}
