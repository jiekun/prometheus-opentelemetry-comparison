package receiver

import (
	"context"
	"github.com/VictoriaMetrics/metrics"
	otlp "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
)

var (
	otlpExportRequestTotal     = metrics.NewCounter(`requests_total{path="otlp.export"}`)
	otlpExportDecodeErrorTotal = metrics.NewCounter(`decode_error_total{path="otlp.export"}`)
	otlpExportSampleTotal      = metrics.NewCounter(`sampled_total{path="otlp.export",exporter="opentelemetry-collector"}`)
)

type noopOTLPMetricsServer struct {
	otlp.UnimplementedMetricsServiceServer
}

func NewOTLPMetricsEndpoint(grpcServer *grpc.Server) {
	otlp.RegisterMetricsServiceServer(grpcServer, &noopOTLPMetricsServer{})
}

func (s *noopOTLPMetricsServer) Export(ctx context.Context, req *otlp.ExportMetricsServiceRequest) (*otlp.ExportMetricsServiceResponse, error) {
	otlpExportRequestTotal.Inc()
	otlpExportSampleTotal.Add(countSamples(req))
	return &otlp.ExportMetricsServiceResponse{}, nil
}
