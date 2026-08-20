// Command otel-app is a small HTTP service instrumented with the
// OpenTelemetry Go SDK, serving 50 simulated API endpoints (handleAPI1
// through handleAPI50), running the identical simulated workload as
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
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"

	"github.com/jiekun/prometheus-opentelemetry-comparison/internal/procstats"
)

// durationBuckets matches the bucket boundaries used for the Prometheus
// histogram in cmd/prometheus-app/main.go, so the two histograms bucket
// latency identically. Boundaries are sized to the actual time.Sleep
// ranges used by the handlers below (2ms-550ms, see handleAPI1 through
// handleAPI50) rather than Prometheus's stock defaults - still more
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

// simulateLatency sleeps for meanMillis +/- 10%, uniformly at random. Every
// endpoint has its own fixed mean latency, but the per-request jitter around
// that mean is kept small and consistent across all 50 endpoints - unlike a
// jitter range comparable to the mean itself, this keeps request duration
// (and therefore in-flight concurrency, for a fixed request rate) from
// swinging widely from one request to the next, which otherwise shows up as
// CPU usage that drifts over time even under constant load.
func simulateLatency(meanMillis int) {
	jitter := meanMillis / 10
	if jitter < 1 {
		jitter = 1
	}
	time.Sleep(time.Duration(meanMillis-jitter+rand.IntN(2*jitter+1)) * time.Millisecond)
}

// handleAPI1 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI1(w http.ResponseWriter, r *http.Request) {
	simulateLatency(32)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI2 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI2(w http.ResponseWriter, r *http.Request) {
	simulateLatency(73)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI3 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI3(w http.ResponseWriter, r *http.Request) {
	simulateLatency(140)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI4 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI4(w http.ResponseWriter, r *http.Request) {
	simulateLatency(306)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI5 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI5(w http.ResponseWriter, r *http.Request) {
	simulateLatency(20)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI6 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI6(w http.ResponseWriter, r *http.Request) {
	simulateLatency(39)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI7 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI7(w http.ResponseWriter, r *http.Request) {
	simulateLatency(80)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI8 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI8(w http.ResponseWriter, r *http.Request) {
	simulateLatency(147)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI9 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI9(w http.ResponseWriter, r *http.Request) {
	simulateLatency(314)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI10 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI10(w http.ResponseWriter, r *http.Request) {
	simulateLatency(17)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI11 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI11(w http.ResponseWriter, r *http.Request) {
	simulateLatency(46)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI12 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI12(w http.ResponseWriter, r *http.Request) {
	simulateLatency(88)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI13 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI13(w http.ResponseWriter, r *http.Request) {
	simulateLatency(154)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI14 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI14(w http.ResponseWriter, r *http.Request) {
	simulateLatency(321)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI15 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI15(w http.ResponseWriter, r *http.Request) {
	simulateLatency(24)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI16 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI16(w http.ResponseWriter, r *http.Request) {
	simulateLatency(54)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI17 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI17(w http.ResponseWriter, r *http.Request) {
	simulateLatency(96)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI18 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI18(w http.ResponseWriter, r *http.Request) {
	simulateLatency(162)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI19 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI19(w http.ResponseWriter, r *http.Request) {
	simulateLatency(328)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI20 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI20(w http.ResponseWriter, r *http.Request) {
	simulateLatency(12)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI21 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI21(w http.ResponseWriter, r *http.Request) {
	simulateLatency(42)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI22 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI22(w http.ResponseWriter, r *http.Request) {
	simulateLatency(103)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI23 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI23(w http.ResponseWriter, r *http.Request) {
	simulateLatency(170)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI24 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI24(w http.ResponseWriter, r *http.Request) {
	simulateLatency(336)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI25 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI25(w http.ResponseWriter, r *http.Request) {
	simulateLatency(20)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI26 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI26(w http.ResponseWriter, r *http.Request) {
	simulateLatency(49)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI27 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI27(w http.ResponseWriter, r *http.Request) {
	simulateLatency(110)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI28 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI28(w http.ResponseWriter, r *http.Request) {
	simulateLatency(177)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI29 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI29(w http.ResponseWriter, r *http.Request) {
	simulateLatency(344)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI30 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI30(w http.ResponseWriter, r *http.Request) {
	simulateLatency(17)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI31 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI31(w http.ResponseWriter, r *http.Request) {
	simulateLatency(56)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI32 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI32(w http.ResponseWriter, r *http.Request) {
	simulateLatency(88)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI33 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI33(w http.ResponseWriter, r *http.Request) {
	simulateLatency(184)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI34 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI34(w http.ResponseWriter, r *http.Request) {
	simulateLatency(351)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI35 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI35(w http.ResponseWriter, r *http.Request) {
	simulateLatency(24)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI36 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI36(w http.ResponseWriter, r *http.Request) {
	simulateLatency(64)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI37 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI37(w http.ResponseWriter, r *http.Request) {
	simulateLatency(96)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI38 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI38(w http.ResponseWriter, r *http.Request) {
	simulateLatency(192)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI39 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI39(w http.ResponseWriter, r *http.Request) {
	simulateLatency(358)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI40 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI40(w http.ResponseWriter, r *http.Request) {
	simulateLatency(12)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI41 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI41(w http.ResponseWriter, r *http.Request) {
	simulateLatency(32)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI42 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI42(w http.ResponseWriter, r *http.Request) {
	simulateLatency(103)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI43 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI43(w http.ResponseWriter, r *http.Request) {
	simulateLatency(160)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI44 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI44(w http.ResponseWriter, r *http.Request) {
	simulateLatency(366)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI45 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI45(w http.ResponseWriter, r *http.Request) {
	simulateLatency(20)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI46 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI46(w http.ResponseWriter, r *http.Request) {
	simulateLatency(39)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI47 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI47(w http.ResponseWriter, r *http.Request) {
	simulateLatency(110)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI48 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI48(w http.ResponseWriter, r *http.Request) {
	simulateLatency(167)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI49 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI49(w http.ResponseWriter, r *http.Request) {
	simulateLatency(374)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI50 simulates a generic endpoint with a small amount of
// latency jitter.
func handleAPI50(w http.ResponseWriter, r *http.Request) {
	simulateLatency(17)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
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

// metricExportInterval mirrors the OTel SDK's own parsing of
// OTEL_METRIC_EXPORT_INTERVAL (env.go in go.opentelemetry.io/otel/sdk/metric):
// milliseconds, falling back to the SDK's 60s default if unset or invalid.
// It's re-read here (rather than exposed by the SDK) so startupJitter can
// size itself to whatever interval the periodic reader will actually use.
func metricExportInterval() time.Duration {
	const defaultInterval = 60 * time.Second
	v := os.Getenv("OTEL_METRIC_EXPORT_INTERVAL")
	if v == "" {
		return defaultInterval
	}
	ms, err := strconv.Atoi(v)
	if err != nil || ms <= 0 {
		return defaultInterval
	}
	return time.Duration(ms) * time.Millisecond
}

// startupJitter sleeps a random fraction of one metric-export interval
// before returning. Every instance of this app is normally started at
// once (see deployment/Makefile's run-otel-app, which forks 100 of these
// back to back), which would otherwise leave their periodic exporters
// ticking in lockstep - all 100 sending an OTLP export in the same
// instant every interval, rather than a steady trickle. Spreading each
// instance's first tick over a random offset within one interval spreads
// every subsequent tick with it, since the reader ticks on a fixed period
// from that first one.
func startupJitter(ctx context.Context) {
	interval := metricExportInterval()
	jitter := time.Duration(rand.Int64N(int64(interval)))
	select {
	case <-ctx.Done():
	case <-time.After(jitter):
	}
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// otlpmetrichttp.New honors the standard OTEL_EXPORTER_OTLP_* env vars
	// (endpoint, headers, TLS, ...). Default to a local, plaintext
	// collector for this comparison lab; override via
	// OTEL_EXPORTER_OTLP_ENDPOINT for anything else.
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:4318"
		os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", endpoint)
	}

	exp, err := otlpmetrichttp.New(ctx)
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

	// Stagger this instance's export phase before the periodic reader
	// starts ticking - see startupJitter.
	startupJitter(ctx)

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
	mux.Handle("/api/api1", m.instrument("/api/api1", handleAPI1))
	mux.Handle("/api/api2", m.instrument("/api/api2", handleAPI2))
	mux.Handle("/api/api3", m.instrument("/api/api3", handleAPI3))
	mux.Handle("/api/api4", m.instrument("/api/api4", handleAPI4))
	mux.Handle("/api/api5", m.instrument("/api/api5", handleAPI5))
	mux.Handle("/api/api6", m.instrument("/api/api6", handleAPI6))
	mux.Handle("/api/api7", m.instrument("/api/api7", handleAPI7))
	mux.Handle("/api/api8", m.instrument("/api/api8", handleAPI8))
	mux.Handle("/api/api9", m.instrument("/api/api9", handleAPI9))
	mux.Handle("/api/api10", m.instrument("/api/api10", handleAPI10))
	mux.Handle("/api/api11", m.instrument("/api/api11", handleAPI11))
	mux.Handle("/api/api12", m.instrument("/api/api12", handleAPI12))
	mux.Handle("/api/api13", m.instrument("/api/api13", handleAPI13))
	mux.Handle("/api/api14", m.instrument("/api/api14", handleAPI14))
	mux.Handle("/api/api15", m.instrument("/api/api15", handleAPI15))
	mux.Handle("/api/api16", m.instrument("/api/api16", handleAPI16))
	mux.Handle("/api/api17", m.instrument("/api/api17", handleAPI17))
	mux.Handle("/api/api18", m.instrument("/api/api18", handleAPI18))
	mux.Handle("/api/api19", m.instrument("/api/api19", handleAPI19))
	mux.Handle("/api/api20", m.instrument("/api/api20", handleAPI20))
	mux.Handle("/api/api21", m.instrument("/api/api21", handleAPI21))
	mux.Handle("/api/api22", m.instrument("/api/api22", handleAPI22))
	mux.Handle("/api/api23", m.instrument("/api/api23", handleAPI23))
	mux.Handle("/api/api24", m.instrument("/api/api24", handleAPI24))
	mux.Handle("/api/api25", m.instrument("/api/api25", handleAPI25))
	mux.Handle("/api/api26", m.instrument("/api/api26", handleAPI26))
	mux.Handle("/api/api27", m.instrument("/api/api27", handleAPI27))
	mux.Handle("/api/api28", m.instrument("/api/api28", handleAPI28))
	mux.Handle("/api/api29", m.instrument("/api/api29", handleAPI29))
	mux.Handle("/api/api30", m.instrument("/api/api30", handleAPI30))
	mux.Handle("/api/api31", m.instrument("/api/api31", handleAPI31))
	mux.Handle("/api/api32", m.instrument("/api/api32", handleAPI32))
	mux.Handle("/api/api33", m.instrument("/api/api33", handleAPI33))
	mux.Handle("/api/api34", m.instrument("/api/api34", handleAPI34))
	mux.Handle("/api/api35", m.instrument("/api/api35", handleAPI35))
	mux.Handle("/api/api36", m.instrument("/api/api36", handleAPI36))
	mux.Handle("/api/api37", m.instrument("/api/api37", handleAPI37))
	mux.Handle("/api/api38", m.instrument("/api/api38", handleAPI38))
	mux.Handle("/api/api39", m.instrument("/api/api39", handleAPI39))
	mux.Handle("/api/api40", m.instrument("/api/api40", handleAPI40))
	mux.Handle("/api/api41", m.instrument("/api/api41", handleAPI41))
	mux.Handle("/api/api42", m.instrument("/api/api42", handleAPI42))
	mux.Handle("/api/api43", m.instrument("/api/api43", handleAPI43))
	mux.Handle("/api/api44", m.instrument("/api/api44", handleAPI44))
	mux.Handle("/api/api45", m.instrument("/api/api45", handleAPI45))
	mux.Handle("/api/api46", m.instrument("/api/api46", handleAPI46))
	mux.Handle("/api/api47", m.instrument("/api/api47", handleAPI47))
	mux.Handle("/api/api48", m.instrument("/api/api48", handleAPI48))
	mux.Handle("/api/api49", m.instrument("/api/api49", handleAPI49))
	mux.Handle("/api/api50", m.instrument("/api/api50", handleAPI50))

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
