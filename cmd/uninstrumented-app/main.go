// Command uninstrumented-app is the control group for cmd/prometheus-app
// and cmd/otel-app: it runs the exact same simulated workload - the same
// 10 API handlers and the same background sampling of queue depth,
// heap/CPU/memory usage and Go runtime counts - but never calls into
// either metrics SDK. There are no counters, histograms or gauges, and no
// /metrics endpoint or OTLP export.
//
// The background loop still does the work of reading runtime.MemStats,
// sampling procstats and counting goroutines/threads/GC cycles - that
// collection cost exists in the other two apps regardless of which SDK
// records the result, so it belongs in the baseline too. What's missing
// here is purely the SDK-side recording, so any overhead measured between
// this app and the other two is attributable to instrumentation, not to
// the workload itself.
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
	"syscall"
	"time"

	"github.com/jiekun/prometheus-opentelemetry-comparison/internal/procstats"
)

// handleWork simulates a read-ish endpoint with variable latency and an
// occasional server error. Kept identical to cmd/prometheus-app's version.
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

// simulateBackground periodically does the same sampling work
// cmd/prometheus-app and cmd/otel-app do in their background loops - a
// random-walking queue depth, the process's real heap/CPU/memory usage,
// and Go runtime counts - but only logs a summary rather than recording
// any of it as a metric.
func simulateBackground(ctx context.Context, proc *procstats.Reader) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	logTicker := time.NewTicker(10 * time.Second)
	defer logTicker.Stop()

	depth := 0.0
	lastCPUSeconds := 0.0
	lastGCCycles := 0.0
	var mem runtime.MemStats
	var goroutines, threads int
	var residentMemory, virtualMemory uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-logTicker.C:
			log.Printf("queue_depth=%.1f heap_alloc=%d goroutines=%d threads=%d rss=%d vsize=%d",
				depth, mem.HeapAlloc, goroutines, threads, residentMemory, virtualMemory)
		case <-ticker.C:
			depth += rand.Float64()*10 - 5
			if depth < 0 {
				depth = 0
			}
			if depth > 100 {
				depth = 100
			}

			runtime.ReadMemStats(&mem)
			goroutines = runtime.NumGoroutine()
			threads = pprof.Lookup("threadcreate").Count()
			if delta := float64(mem.NumGC) - lastGCCycles; delta > 0 {
				lastGCCycles = float64(mem.NumGC)
			}

			sample, err := proc.Sample()
			if err != nil {
				log.Printf("procstats sample: %v", err)
				continue
			}
			if delta := sample.CPUSeconds - lastCPUSeconds; delta > 0 {
				lastCPUSeconds = sample.CPUSeconds
			}
			residentMemory = sample.RSSBytes
			virtualMemory = sample.VSizeBytes
		}
	}
}

func main() {
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}

	proc, err := procstats.NewReader()
	if err != nil {
		log.Fatalf("procstats: %v", err)
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/api/work", handleWork)
	mux.HandleFunc("/api/submit", handleSubmit)
	mux.HandleFunc("GET /api/users", handleListUsers)
	mux.HandleFunc("GET /api/users/{id}", handleGetUser)
	mux.HandleFunc("POST /api/users", handleCreateUser)
	mux.HandleFunc("PUT /api/users/{id}", handleUpdateUser)
	mux.HandleFunc("DELETE /api/users/{id}", handleDeleteUser)
	mux.HandleFunc("GET /api/orders", handleListOrders)
	mux.HandleFunc("POST /api/orders", handleCreateOrder)
	mux.HandleFunc("GET /api/reports", handleReports)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	go simulateBackground(ctx, proc)

	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		log.Printf("uninstrumented-app listening on %s (no metrics exposed)", addr)
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
