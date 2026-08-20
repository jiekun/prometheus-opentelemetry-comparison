package receiver

import (
	"github.com/VictoriaMetrics/metrics"
	"github.com/gin-gonic/gin"
	otlp "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	"google.golang.org/protobuf/proto"
	"io"
	"log"
)

var (
	otlpHTTPExportRequestTotal     = metrics.NewCounter(`requests_total{path="/otlp/export"}`)
	otlpHTTPExportReadErrorTotal   = metrics.NewCounter(`read_error_total{path="/otlp/export"}`)
	otlpHTTPExportDecodeErrorTotal = metrics.NewCounter(`decode_error_total{path="/otlp/export"}`)
	otlpHTTPExportSampleTotal      = metrics.NewCounter(`sampled_total{path="/otlp/export"}`)
	// Size of the request body as received. This route never decompresses
	// (see deployment/docker/agent/opentelemetry-collector.yaml's exporter
	// comment - it's only ever fed uncompressed traffic by design), so this
	// is already the uncompressed payload size.
	otlpHTTPExportRequestSizeBytes = metrics.NewHistogram(`request_size_bytes{path="/otlp/export"}`)
)

func NewOTLPHTTPRoute(r *gin.Engine) {
	r.POST("/otlp/export", func(c *gin.Context) {
		otlpHTTPExportRequestTotal.Inc()
		b, err := io.ReadAll(c.Request.Body)
		if err != nil {
			otlpHTTPExportReadErrorTotal.Inc()
			return
		}
		otlpHTTPExportRequestSizeBytes.Update(float64(len(b)))
		req := &otlp.ExportMetricsServiceRequest{}
		err = proto.Unmarshal(b, req)
		if err != nil {
			log.Printf("proto.Unmarshal err: %v\n", err)
			otlpHTTPExportDecodeErrorTotal.Inc()
			return
		}

		otlpHTTPExportSampleTotal.Add(countSamples(req))
	})
}
