// Command prometheus-app is a small HTTP service instrumented with the
// Prometheus Go client SDK. It exists alongside cmd/otel-app, which
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
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/jiekun/prometheus-opentelemetry-comparison/internal/procstats"
)

// durationBuckets is shared (in spirit) with the explicit bucket
// boundaries used for the OTel histogram in cmd/otel-app/main.go, so the
// two histograms bucket latency identically. Boundaries are sized to the
// actual time.Sleep ranges used by the handlers below (2ms-799ms, see
// handleWork through handleReports) rather than Prometheus's stock
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
	}
	reg.MustRegister(
		m.requestsTotal, m.requestDuration, m.requestsInFlight, m.queueDepth, m.heapAlloc,
		m.cpuSeconds, m.residentMemory, m.virtualMemory, m.goroutines, m.threads, m.gcCycles,
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

// handleWork simulates a read-ish endpoint with variable latency and an
// occasional server error.
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
// server errors.
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

// handleListUsers simulates a fast, read-heavy list endpoint.
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
// not-found rate.
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
// occasional server errors.
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
// missing record or a downstream failure.
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
// already-missing record.
func handleDeleteUser(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(5+rand.IntN(45)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

// handleListOrders simulates a heavier read that fans out to a downstream
// store.
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
// downstream failures.
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
// occasionally finds a downstream dependency unavailable.
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
