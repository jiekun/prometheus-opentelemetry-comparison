// Command prometheus-app is a small HTTP service instrumented with the
// Prometheus Go client SDK, serving 50 simulated API endpoints (handleAPI1
// through handleAPI50). It exists alongside cmd/otel-app, which
// instruments the same workload with the OpenTelemetry Go SDK, so the two
// can be compared side by side (metrics shape, overhead, pipeline
// behavior). The instrument choices here are mirrored as closely as
// possible in cmd/otel-app/main.go:
//
//   - http_requests_total          Counter   <-> OTel Counter
//   - http_request_duration_seconds Histogram <-> OTel Histogram (explicit buckets)
//   - http_requests_in_flight      Gauge     <-> OTel UpDownCounter
//   - worker_queue_depth           Gauge     <-> OTel synchronous Gauge
//   - process_heap_alloc_bytes     Gauge     <-> OTel synchronous Gauge
//   - process_cpu_seconds_total    Counter   <-> OTel Counter
//   - process_resident_memory_bytes Gauge    <-> OTel synchronous Gauge
//   - process_virtual_memory_bytes Gauge     <-> OTel synchronous Gauge
//   - go_goroutines                Gauge     <-> OTel synchronous Gauge
//   - go_threads                   Gauge     <-> OTel synchronous Gauge
//   - go_gc_cycles_total           Counter   <-> OTel Counter
//
// Alongside those, each request also records a set of non-histogram
// instruments (durationSum/durationCount/responseSize/lastDuration/
// latencyBucket/concurrencyLevel/outcomeClass in newMetrics below) that
// exist to counterbalance the histogram above in the comparison: a
// histogram unconditionally exports every configured bucket boundary as its
// own series regardless of what data arrives, whereas a plain counter/gauge
// only ever exports the label combinations actually observed. Most of them
// carry a synthetic "shard" label (see shardLabel) chosen by an independent
// random draw that has no bearing on request processing, purely to give
// them realistic label cardinality to export.
//
// A custom (non-default) registry is used so this app doesn't pull in the
// standard Go/process collectors wholesale: those expose far more series
// than the OTel side has an equivalent for, which would make the two
// exporters' payloads (and any overhead comparison) lopsided. The
// process/runtime metrics above are instead sampled by hand, mirroring the
// OTel side 1:1 the same way the request metrics do.
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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jiekun/prometheus-opentelemetry-comparison/internal/procstats"
)

// durationBuckets is shared (in spirit) with the explicit bucket
// boundaries used for the OTel histogram in cmd/otel-app/main.go, so the
// two histograms bucket latency identically. Boundaries are sized to the
// actual time.Sleep ranges used by the handlers below (2ms-550ms, see
// handleAPI1 through handleAPI50) rather than Prometheus's stock
// defaults - still more buckets than those (11), but not exhaustively so.
var durationBuckets = []float64{
	0.002, 0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.07,
	0.1, 0.15, 0.2, 0.3, 0.4, 0.5, 0.7, 0.8, 1, 2.5, 5, 10,
}

type prometheusMetrics struct {
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestsInFlight *prometheus.GaugeVec
	queueDepth       prometheus.Gauge
	heapAlloc        prometheus.Gauge
	cpuSeconds       prometheus.Counter
	residentMemory   prometheus.Gauge
	virtualMemory    prometheus.Gauge
	goroutines       prometheus.Gauge
	threads          prometheus.Gauge
	gcCycles         prometheus.Counter

	// Additional non-histogram, per-request instruments - see the package
	// doc comment above for why these exist.
	durationSum      *prometheus.CounterVec
	durationCount    *prometheus.CounterVec
	responseSize     *prometheus.CounterVec
	lastDuration     *prometheus.GaugeVec
	latencyBucket    *prometheus.CounterVec
	concurrencyLevel *prometheus.CounterVec
	outcomeClass     *prometheus.CounterVec
}

func newMetrics(reg prometheus.Registerer) *prometheusMetrics {
	m := &prometheusMetrics{
		requestsTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_total",
			Help: "Total number of HTTP requests processed, by method, path and status code.",
		}, []string{"method", "path", "status"}),
		requestDuration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "http_request_duration_seconds",
			Help:    "HTTP request duration in seconds, by method and path.",
			Buckets: durationBuckets,
		}, []string{"method", "path"}),
		requestsInFlight: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_requests_in_flight",
			Help: "Number of HTTP requests currently being served, by path.",
		}, []string{"path"}),
		queueDepth: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "worker_queue_depth",
			Help: "Simulated depth of the background worker queue.",
		}),
		heapAlloc: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "process_heap_alloc_bytes",
			Help: "Bytes of allocated heap objects, as reported by the Go runtime (runtime.MemStats.HeapAlloc).",
		}),
		cpuSeconds: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "process_cpu_seconds_total",
			Help: "Total user and system CPU time spent in seconds.",
		}),
		residentMemory: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "process_resident_memory_bytes",
			Help: "Resident memory size in bytes.",
		}),
		virtualMemory: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "process_virtual_memory_bytes",
			Help: "Virtual memory size in bytes.",
		}),
		goroutines: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "go_goroutines",
			Help: "Number of goroutines that currently exist.",
		}),
		threads: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "go_threads",
			Help: "Number of OS threads created.",
		}),
		gcCycles: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "go_gc_cycles_total",
			Help: "Number of completed GC cycles, as reported by the Go runtime (runtime.MemStats.NumGC).",
		}),
		durationSum: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_request_duration_sum_seconds_total",
			Help: "Sum of HTTP request durations in seconds, by path and shard - a manual, counter-based equivalent of the histogram's _sum.",
		}, []string{"path", "shard"}),
		durationCount: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_request_duration_count_total",
			Help: "Count of HTTP request durations recorded, by path and shard - a manual, counter-based equivalent of the histogram's _count.",
		}, []string{"path", "shard"}),
		responseSize: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_response_size_bytes_total",
			Help: "Total bytes written in HTTP responses, by path and shard.",
		}, []string{"path", "shard"}),
		lastDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "http_request_last_duration_seconds",
			Help: "Duration in seconds of the most recently completed HTTP request, by path and shard.",
		}, []string{"path", "shard"}),
		latencyBucket: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_request_latency_bucket_total",
			Help: "Count of HTTP requests whose duration fell in a given latency bucket, by path, shard and le - a manual, counter-based equivalent of the histogram's per-bucket counts.",
		}, []string{"path", "shard", "le"}),
		concurrencyLevel: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_concurrency_level_total",
			Help: "Count of HTTP requests started, classified by the in-flight request count for that path at start time, by path, shard and level.",
		}, []string{"path", "shard", "level"}),
		outcomeClass: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "http_requests_outcome_class_total",
			Help: "Count of HTTP requests, classified by completed duration against fixed latency thresholds, by path, shard and class.",
		}, []string{"path", "shard", "class"}),
	}
	reg.MustRegister(
		m.requestsTotal, m.requestDuration, m.requestsInFlight, m.queueDepth, m.heapAlloc,
		m.cpuSeconds, m.residentMemory, m.virtualMemory, m.goroutines, m.threads, m.gcCycles,
		m.durationSum, m.durationCount, m.responseSize, m.lastDuration, m.latencyBucket,
		m.concurrencyLevel, m.outcomeClass,
	)
	return m
}

// statusRecorder captures the status code written by a handler so it can
// be used as a metric label after the handler returns.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

// instrument wraps next with the request-scoped metrics common to every
// route: in-flight gauge, duration histogram and status counter.
func (m *prometheusMetrics) instrument(path string, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.requestsInFlight.WithLabelValues(path).Inc()
		defer m.requestsInFlight.WithLabelValues(path).Dec()

		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next(rec, r)
		elapsed := time.Since(start).Seconds()

		m.requestDuration.WithLabelValues(r.Method, path).Observe(elapsed)
		m.requestsTotal.WithLabelValues(r.Method, path, strconv.Itoa(rec.status)).Inc()
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
func simulateBackground(ctx context.Context, m *prometheusMetrics, proc *procstats.Reader) {
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
			m.queueDepth.Set(depth)

			runtime.ReadMemStats(&mem)
			m.heapAlloc.Set(float64(mem.HeapAlloc))
			m.goroutines.Set(float64(runtime.NumGoroutine()))
			m.threads.Set(float64(pprof.Lookup("threadcreate").Count()))
			if delta := float64(mem.NumGC) - lastGCCycles; delta > 0 {
				m.gcCycles.Add(delta)
				lastGCCycles = float64(mem.NumGC)
			}

			sample, err := proc.Sample()
			if err != nil {
				log.Printf("procstats sample: %v", err)
				continue
			}
			if delta := sample.CPUSeconds - lastCPUSeconds; delta > 0 {
				m.cpuSeconds.Add(delta)
				lastCPUSeconds = sample.CPUSeconds
			}
			m.residentMemory.Set(float64(sample.RSSBytes))
			m.virtualMemory.Set(float64(sample.VSizeBytes))
		}
	}
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	reg := prometheus.NewRegistry()
	m := newMetrics(reg)

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
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go simulateBackground(ctx, m, proc)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("prometheus-app listening on %s (metrics at /metrics)", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Println("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("shutdown: %v", err)
	}
}
