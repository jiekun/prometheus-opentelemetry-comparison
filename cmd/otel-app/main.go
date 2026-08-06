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
//   - process_cpu_seconds_total    Counter        <-> Prometheus Counter
//   - process_resident_memory_bytes Gauge         <-> Prometheus Gauge (Set)
//   - process_virtual_memory_bytes Gauge          <-> Prometheus Gauge (Set)
//   - go_goroutines                Gauge          <-> Prometheus Gauge (Set)
//   - go_threads                   Gauge          <-> Prometheus Gauge (Set)
//   - go_gc_cycles_total           Counter        <-> Prometheus Counter
//
// Deliberately avoided: OTel's delta temporality (Prometheus only
// understands cumulative series, so the periodic reader is left at its
// default CumulativeTemporality) and exponential histograms / summaries
// (no clean Prometheus classic-histogram equivalent). The process/runtime
// metrics above are likewise hand-sampled rather than pulled in via
// go.opentelemetry.io/contrib/instrumentation/runtime, so both apps read
// the exact same underlying numbers through internal/procstats and differ
// only in how each SDK models and exports them.
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
	"runtime/pprof"
	"strconv"
	"syscall"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/jiekun/prometheus-opentelemetry-comparison/internal/procstats"
)

// durationBuckets matches the bucket boundaries used for the Prometheus
// histogram in cmd/prometheus-app/main.go, so the two histograms bucket
// latency identically. Boundaries are sized to the actual time.Sleep
// ranges used by the handlers below (2ms-799ms, see handleWork through
// handleReports) rather than Prometheus's stock defaults - still more
// buckets than those (11), but not exhaustively so.
var durationBuckets = []float64{
	0.002, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.07,
	0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.7, 0.8, 1, 2.5, 5, 10,
}

type otelMetrics struct {
	requestsTotal    metric.Int64Counter
	requestDuration  metric.Float64Histogram
	requestsInFlight metric.Int64UpDownCounter
	queueDepth       metric.Float64Gauge
	heapAlloc        metric.Int64Gauge
	cpuSeconds       metric.Float64Counter
	residentMemory   metric.Int64Gauge
	virtualMemory    metric.Int64Gauge
	goroutines       metric.Int64Gauge
	threads          metric.Int64Gauge
	gcCycles         metric.Int64Counter
}

func newInstruments(meter metric.Meter) *otelMetrics {
	requestsTotal, _ := meter.Int64Counter("http_requests_total",
		metric.WithDescription("Total number of HTTP requests processed, by method, path and status code."),
	)
	requestDuration, _ := meter.Float64Histogram("http_request_duration_seconds",
		metric.WithDescription("HTTP request duration in seconds, by method and path."),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(durationBuckets...),
	)
	requestsInFlight, _ := meter.Int64UpDownCounter("http_requests_in_flight",
		metric.WithDescription("Number of HTTP requests currently being served, by path."),
	)
	queueDepth, _ := meter.Float64Gauge("worker_queue_depth",
		metric.WithDescription("Simulated depth of the background worker queue."),
	)
	heapAlloc, _ := meter.Int64Gauge("process_heap_alloc_bytes",
		metric.WithDescription("Bytes of allocated heap objects, as reported by the Go runtime (runtime.MemStats.HeapAlloc)."),
		metric.WithUnit("By"),
	)
	cpuSeconds, _ := meter.Float64Counter("process_cpu_seconds_total",
		metric.WithDescription("Total user and system CPU time spent in seconds."),
		metric.WithUnit("s"),
	)
	residentMemory, _ := meter.Int64Gauge("process_resident_memory_bytes",
		metric.WithDescription("Resident memory size in bytes."),
		metric.WithUnit("By"),
	)
	virtualMemory, _ := meter.Int64Gauge("process_virtual_memory_bytes",
		metric.WithDescription("Virtual memory size in bytes."),
		metric.WithUnit("By"),
	)
	goroutines, _ := meter.Int64Gauge("go_goroutines",
		metric.WithDescription("Number of goroutines that currently exist."),
	)
	threads, _ := meter.Int64Gauge("go_threads",
		metric.WithDescription("Number of OS threads created."),
	)
	gcCycles, _ := meter.Int64Counter("go_gc_cycles_total",
		metric.WithDescription("Number of completed GC cycles, as reported by the Go runtime (runtime.MemStats.NumGC)."),
	)
	return &otelMetrics{
		requestsTotal:    requestsTotal,
		requestDuration:  requestDuration,
		requestsInFlight: requestsInFlight,
		queueDepth:       queueDepth,
		heapAlloc:        heapAlloc,
		cpuSeconds:       cpuSeconds,
		residentMemory:   residentMemory,
		virtualMemory:    virtualMemory,
		goroutines:       goroutines,
		threads:          threads,
		gcCycles:         gcCycles,
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
func (m *otelMetrics) instrument(path string, next http.HandlerFunc) http.HandlerFunc {
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

// handleListUsers simulates a fast, read-heavy list endpoint. Kept
// identical to cmd/prometheus-app's version.
func handleListUsers(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(38)) * time.Millisecond)
	if rand.IntN(100) < 2 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("[]\n"))
}

// handleGetUser simulates a single-record lookup, with a realistic
// not-found rate. Kept identical to cmd/prometheus-app's version.
func handleGetUser(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(28)) * time.Millisecond)
	if rand.IntN(100) < 8 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{}\n"))
}

// handleCreateUser simulates a write endpoint with validation and
// occasional server errors. Kept identical to cmd/prometheus-app's version.
func handleCreateUser(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(20+rand.IntN(130)) * time.Millisecond)
	switch n := rand.IntN(100); {
	case n < 8:
		http.Error(w, "bad request", http.StatusBadRequest)
	case n < 10:
		http.Error(w, "internal error", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("{}\n"))
	}
}

// handleUpdateUser simulates an idempotent update, occasionally hitting a
// missing record or a downstream failure. Kept identical to
// cmd/prometheus-app's version.
func handleUpdateUser(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(20+rand.IntN(130)) * time.Millisecond)
	switch n := rand.IntN(100); {
	case n < 5:
		http.Error(w, "not found", http.StatusNotFound)
	case n < 8:
		http.Error(w, "internal error", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}\n"))
	}
}

// handleDeleteUser simulates a fast delete, occasionally against an
// already-missing record. Kept identical to cmd/prometheus-app's version.
func handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(5+rand.IntN(45)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListOrders simulates a heavier read that fans out to a downstream
// store. Kept identical to cmd/prometheus-app's version.
func handleListOrders(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(50+rand.IntN(350)) * time.Millisecond)
	if rand.IntN(100) < 3 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("[]\n"))
}

// handleCreateOrder simulates a heavier write with a mix of validation and
// downstream failures. Kept identical to cmd/prometheus-app's version.
func handleCreateOrder(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(60+rand.IntN(390)) * time.Millisecond)
	switch n := rand.IntN(100); {
	case n < 6:
		http.Error(w, "bad request", http.StatusBadRequest)
	case n < 10:
		http.Error(w, "internal error", http.StatusInternalServerError)
	default:
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("{}\n"))
	}
}

// handleReports simulates a slow, aggregation-heavy endpoint that
// occasionally finds a downstream dependency unavailable. Kept identical
// to cmd/prometheus-app's version.
func handleReports(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(200+rand.IntN(600)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "service unavailable", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{}\n"))
}

// simulateBackground periodically updates the gauges that aren't tied to
// individual requests: a random-walking queue depth, the process's real
// heap/CPU/memory usage, and Go runtime counts.
func simulateBackground(ctx context.Context, m *otelMetrics, proc *procstats.Reader) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	depth := 0.0
	lastCPUSeconds := 0.0
	lastGCCycles := 0.0
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
			m.goroutines.Record(ctx, int64(runtime.NumGoroutine()))
			m.threads.Record(ctx, int64(pprof.Lookup("threadcreate").Count()))
			if delta := float64(mem.NumGC) - lastGCCycles; delta > 0 {
				m.gcCycles.Add(ctx, int64(delta))
				lastGCCycles = float64(mem.NumGC)
			}

			sample, err := proc.Sample()
			if err != nil {
				log.Printf("procstats sample: %v", err)
				continue
			}
			if delta := sample.CPUSeconds - lastCPUSeconds; delta > 0 {
				m.cpuSeconds.Add(ctx, delta)
				lastCPUSeconds = sample.CPUSeconds
			}
			m.residentMemory.Record(ctx, int64(sample.RSSBytes))
			m.virtualMemory.Record(ctx, int64(sample.VSizeBytes))
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

	proc, err := procstats.NewReader()
	if err != nil {
		log.Fatalf("procstats: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/api/work", m.instrument("/api/work", handleWork))
	mux.Handle("/api/submit", m.instrument("/api/submit", handleSubmit))
	mux.Handle("GET /api/users", m.instrument("/api/users", handleListUsers))
	mux.Handle("GET /api/users/{id}", m.instrument("/api/users/{id}", handleGetUser))
	mux.Handle("POST /api/users", m.instrument("/api/users", handleCreateUser))
	mux.Handle("PUT /api/users/{id}", m.instrument("/api/users/{id}", handleUpdateUser))
	mux.Handle("DELETE /api/users/{id}", m.instrument("/api/users/{id}", handleDeleteUser))
	mux.Handle("GET /api/orders", m.instrument("/api/orders", handleListOrders))
	mux.Handle("POST /api/orders", m.instrument("/api/orders", handleCreateOrder))
	mux.Handle("GET /api/reports", m.instrument("/api/reports", handleReports))

	go simulateBackground(ctx, m, proc)

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
