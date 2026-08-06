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
//
// A custom (non-default) registry is used so this app doesn't expose the
// standard Go/process collectors: the OTel side has no equivalent
// auto-registered runtime metrics, so including them here would make the
// two exporters' payloads (and any overhead comparison) lopsided.
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

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// durationBuckets is shared (in spirit) with the explicit bucket
// boundaries used for the OTel histogram in cmd/otel-app/main.go, so the
// two histograms bucket latency identically.
var durationBuckets = []float64{0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10}

type metrics struct {
	requestsTotal    *prometheus.CounterVec
	requestDuration  *prometheus.HistogramVec
	requestsInFlight *prometheus.GaugeVec
	queueDepth       prometheus.Gauge
	heapAlloc        prometheus.Gauge
}

func newMetrics(reg prometheus.Registerer) *metrics {
	m := &metrics{
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
	}
	reg.MustRegister(m.requestsTotal, m.requestDuration, m.requestsInFlight, m.queueDepth, m.heapAlloc)
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
func (m *metrics) instrument(path string, next http.HandlerFunc) http.HandlerFunc {
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

// simulateBackground periodically updates the gauges that aren't tied to
// individual requests: a random-walking queue depth and the process's
// real heap usage.
func simulateBackground(ctx context.Context, m *metrics) {
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
			m.queueDepth.Set(depth)

			runtime.ReadMemStats(&mem)
			m.heapAlloc.Set(float64(mem.HeapAlloc))
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

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.Handle("/api/work", m.instrument("/api/work", handleWork))
	mux.Handle("/api/submit", m.instrument("/api/submit", handleSubmit))
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go simulateBackground(ctx, m)

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
