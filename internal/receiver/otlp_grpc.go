package receiver

import (
	"context"
	"github.com/VictoriaMetrics/metrics"
	otlp "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"sync/atomic"
)

var (
	otlpExportRequestTotal     = metrics.NewCounter(`requests_total{path="otlp.export"}`)
	otlpExportDecodeErrorTotal = metrics.NewCounter(`decode_error_total{path="otlp.export"}`)
	otlpExportSampleTotal      = metrics.NewCounter(`sampled_total{path="otlp.export",exporter="opentelemetry-collector"}`)
	// Size of the request, uncompressed. grpc-go decompresses (this route
	// is fed gzip-compressed traffic, see opentelemetry-collector.yaml) and
	// unmarshals the request before this handler ever sees it, so there's
	// no raw decompressed byte slice to measure directly - proto.Size(req)
	// recomputes what the message's marshaled size would be, which is the
	// same uncompressed size the sender had before gzip, modulo the usual
	// caveats of proto.Size (unknown/unrecognized fields, field ordering)
	// that make it not byte-for-byte identical to what was actually sent.
	otlpExportRequestSizeBytes = metrics.NewHistogram(`request_size_bytes{path="otlp.export"}`)
	otlpExportSampleCounter    atomic.Uint64
)

type noopOTLPMetricsServer struct {
	otlp.UnimplementedMetricsServiceServer
}

func NewOTLPMetricsEndpoint(grpcServer *grpc.Server) {
	otlp.RegisterMetricsServiceServer(grpcServer, &noopOTLPMetricsServer{})
}

func (s *noopOTLPMetricsServer) Export(ctx context.Context, req *otlp.ExportMetricsServiceRequest) (*otlp.ExportMetricsServiceResponse, error) {
	otlpExportRequestTotal.Inc()
	otlpExportRequestSizeBytes.Update(float64(proto.Size(req)))
	if n, ok := sampleEvery(&otlpExportSampleCounter); ok {
		logSampleProtoJSON("otlp grpc", n, req)
	}
	otlpExportSampleTotal.Add(countSamples(req))
	return &otlp.ExportMetricsServiceResponse{}, nil
}
