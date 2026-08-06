// Command otel-app is a small HTTP service instrumented with the
// OpenTelemetry Go SDK, running the identical simulated workload as
// cmd/prometheus-app so the two instrumentation approaches can be
// compared side by side. Instrument choices are mirrored 1:1 with the
// Prometheus client SDK, staying within data types both models support:
//
//   - http_requests_total          Counter        <-> Prometheus Counter
//   - http_request_duration_seconds Histogram      <-> Prometheus Histogram (same buckets)
//   - http_requests_in_flight      UpDownCounter  <-> Prometheus Gauge (Inc/Dec)
//   - worker_queue_depth           Gauge          <-> Prometheus Gauge (Set)
//   - process_heap_alloc_bytes     Gauge          <-> Prometheus Gauge (Set)
//
// Deliberately avoided: OTel's delta temporality (Prometheus only
// understands cumulative series, so the periodic reader is left at its
// default CumulativeTemporality) and exponential histograms / summaries
// (no clean Prometheus classic-histogram equivalent).
//
// Metrics are pushed via OTLP/gRPC to a collector rather than scraped,
// since that's the OpenTelemetry-native pipeline shape (compare against
// deployment/docker/agent/opentelemetry-collector.yaml).
package main

import (
	"context"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// durationBuckets matches the bucket boundaries used for the Prometheus
// histogram in cmd/prometheus-app/main.go, so the two histograms bucket
// latency identically.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type instruments struct {
	requestsTotal    metric.Int64Counter
	requestDuration  metric.Float64Histogram
	requestsInFlight metric.Int64UpDownCounter
	queueDepth       metric.Float64Gauge
	heapAlloc        metric.Int64Gauge
}

func must[T any](v T, err error) T {
	if err != nil {
		log.Fatalf("instrument: %v", err)
	}
	return v
}

func newInstruments(meter metric.Meter) *instruments {
	return &instruments{
		requestsTotal: must(meter.Int64Counter("http_requests_total",
			metric.WithDescription("Total number of HTTP requests processed, by method, path and status code."),
		)),
		requestDuration: must(meter.Float64Histogram("http_request_duration_seconds",
			metric.WithDescription("HTTP request duration in seconds, by method and path."),
			metric.WithUnit("s"),
			metric.WithExplicitBucketBoundaries(durationBuckets...),
		)),
		requestsInFlight: must(meter.Int64UpDownCounter("http_requests_in_flight",
			metric.WithDescription("Number of HTTP requests currently being served, by path."),
		)),
		queueDepth: must(meter.Float64Gauge("worker_queue_depth",
			metric.WithDescription("Simulated depth of the background worker queue."),
		)),
		heapAlloc: must(meter.Int64Gauge("process_heap_alloc_bytes",
			metric.WithDescription("Bytes of allocated heap objects, as reported by the Go runtime (runtime.MemStats.HeapAlloc)."),
			metric.WithUnit("By"),
		)),
	}
}

// statusRecorder captures the status code written by a handler so it can
// be used as a metric attribute after the handler returns.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// instrument wraps next with the request-scoped metrics common to every
// route: in-flight up-down counter, duration histogram and status counter.
func (m *instruments) instrument(path string, next http.HandlerFunc) http.HandlerFunc {
	pathAttr := attribute.String("path", path)
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()
		methodAttr := attribute.String("method", r.Method)

		m.requestsInFlight.Add(ctx, 1, metric.WithAttributes(pathAttr))
		defer m.requestsInFlight.Add(ctx, -1, metric.WithAttributes(pathAttr))

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(rec, r)
		elapsed := time.Since(start).Seconds()

		m.requestDuration.Record(ctx, elapsed, metric.WithAttributes(methodAttr, pathAttr))
		m.requestsTotal.Add(ctx, 1, metric.WithAttributes(methodAttr, pathAttr, attribute.String("status", strconv.Itoa(rec.status))))
	}
}

// handleWork simulates a read-ish endpoint with variable latency and an
// occasional server error. Kept identical to cmd/prometheus-app's version
// so both apps generate the same underlying workload.
func handleWork(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(5+rand.IntN(195)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleSubmit simulates a write-ish endpoint with a mix of client and
// server errors. Kept identical to cmd/prometheus-app's version.
func handleSubmit(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(10+rand.IntN(290)) * time.Millisecond)
	switch n := rand.IntN(100); {
	case n < 10:
		http.Error(w, "bad request", http.StatusBadRequest)
	case n < 12:
		http.Error(w, "internal error", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created\n"))
	}
}

// simulateBackground periodically updates the gauges that aren't tied to
// individual requests: a random-walking queue depth and the process's
// real heap usage.
func simulateBackground(ctx context.Context, m *instruments) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	depth := 0.0
	var mem runtime.MemStats
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depth += rand.Float64()*10 - 5
			if depth < 0 {
				depth = 0
			}
			if depth > 100 {
				depth = 100
			}
			m.queueDepth.Record(ctx, depth)

			runtime.ReadMemStats(&mem)
			m.heapAlloc.Record(ctx, int64(mem.HeapAlloc))
		}
	}
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// otlpmetricgrpc.New honors the standard OTEL_EXPORTER_OTLP_* env vars
	// (endpoint, headers, TLS, ...). Default to a local, plaintext
	// collector for this comparison lab; override via
	// OTEL_EXPORTER_OTLP_ENDPOINT for anything else.
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4317"
		os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
	}

	exp, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		log.Fatalf("otlp metric exporter: %v", err)
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", "otel-app")),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
	)
	if err != nil {
		log.Fatalf("resource: %v", err)
	}

	// No WithInterval: leave the export cadence to the
	// OTEL_METRIC_EXPORT_INTERVAL env var (60s if unset), matching how the
	// Prometheus app's scrape cadence is also controlled externally
	// rather than hardcoded here.
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exp)),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(mp)
	meter := mp.Meter("prometheus-opentelemetry-comparison/otel-app")

	m := newInstruments(meter)

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/api/work", m.instrument("/api/work", handleWork))
	mux.Handle("/api/submit", m.instrument("/api/submit", handleSubmit))

	go simulateBackground(ctx, m)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("otel-app listening on %s (metrics pushed via OTLP to %s)", addr, endpoint)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	flushCtx, cancelFlush := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelFlush()
	if err := mp.Shutdown(flushCtx); err != nil {
		log.Printf("meter provider shutdown: %v", err)
	}
}
