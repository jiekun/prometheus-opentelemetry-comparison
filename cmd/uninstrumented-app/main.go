// Command uninstrumented-app is the control group for cmd/prometheus-app
// and cmd/otel-app: it runs the exact same simulated workload - the same
// 50 API handlers (handleAPI1 through handleAPI50) and the same background
// sampling of queue depth, heap/CPU/memory usage and Go runtime counts -
// but never calls into either metrics SDK. There are no counters,
// histograms or gauges, and no /metrics endpoint or OTLP export.
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
	mux.HandleFunc("/api/api1", handleAPI1)
	mux.HandleFunc("/api/api2", handleAPI2)
	mux.HandleFunc("/api/api3", handleAPI3)
	mux.HandleFunc("/api/api4", handleAPI4)
	mux.HandleFunc("/api/api5", handleAPI5)
	mux.HandleFunc("/api/api6", handleAPI6)
	mux.HandleFunc("/api/api7", handleAPI7)
	mux.HandleFunc("/api/api8", handleAPI8)
	mux.HandleFunc("/api/api9", handleAPI9)
	mux.HandleFunc("/api/api10", handleAPI10)
	mux.HandleFunc("/api/api11", handleAPI11)
	mux.HandleFunc("/api/api12", handleAPI12)
	mux.HandleFunc("/api/api13", handleAPI13)
	mux.HandleFunc("/api/api14", handleAPI14)
	mux.HandleFunc("/api/api15", handleAPI15)
	mux.HandleFunc("/api/api16", handleAPI16)
	mux.HandleFunc("/api/api17", handleAPI17)
	mux.HandleFunc("/api/api18", handleAPI18)
	mux.HandleFunc("/api/api19", handleAPI19)
	mux.HandleFunc("/api/api20", handleAPI20)
	mux.HandleFunc("/api/api21", handleAPI21)
	mux.HandleFunc("/api/api22", handleAPI22)
	mux.HandleFunc("/api/api23", handleAPI23)
	mux.HandleFunc("/api/api24", handleAPI24)
	mux.HandleFunc("/api/api25", handleAPI25)
	mux.HandleFunc("/api/api26", handleAPI26)
	mux.HandleFunc("/api/api27", handleAPI27)
	mux.HandleFunc("/api/api28", handleAPI28)
	mux.HandleFunc("/api/api29", handleAPI29)
	mux.HandleFunc("/api/api30", handleAPI30)
	mux.HandleFunc("/api/api31", handleAPI31)
	mux.HandleFunc("/api/api32", handleAPI32)
	mux.HandleFunc("/api/api33", handleAPI33)
	mux.HandleFunc("/api/api34", handleAPI34)
	mux.HandleFunc("/api/api35", handleAPI35)
	mux.HandleFunc("/api/api36", handleAPI36)
	mux.HandleFunc("/api/api37", handleAPI37)
	mux.HandleFunc("/api/api38", handleAPI38)
	mux.HandleFunc("/api/api39", handleAPI39)
	mux.HandleFunc("/api/api40", handleAPI40)
	mux.HandleFunc("/api/api41", handleAPI41)
	mux.HandleFunc("/api/api42", handleAPI42)
	mux.HandleFunc("/api/api43", handleAPI43)
	mux.HandleFunc("/api/api44", handleAPI44)
	mux.HandleFunc("/api/api45", handleAPI45)
	mux.HandleFunc("/api/api46", handleAPI46)
	mux.HandleFunc("/api/api47", handleAPI47)
	mux.HandleFunc("/api/api48", handleAPI48)
	mux.HandleFunc("/api/api49", handleAPI49)
	mux.HandleFunc("/api/api50", handleAPI50)

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
