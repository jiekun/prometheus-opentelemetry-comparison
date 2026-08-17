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

// handleAPI1 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI1(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(11+rand.IntN(41)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI2 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI2(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(32+rand.IntN(82)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI3 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI3(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(63+rand.IntN(153)) * time.Millisecond)
	if rand.IntN(100) < 8 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI4 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI4(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(154+rand.IntN(304)) * time.Millisecond)
	if rand.IntN(100) < 10 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI5 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI5(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(7+rand.IntN(25)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI6 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI6(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(16+rand.IntN(46)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI7 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI7(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(37+rand.IntN(87)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI8 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI8(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(68+rand.IntN(158)) * time.Millisecond)
	if rand.IntN(100) < 7 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI9 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI9(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(159+rand.IntN(309)) * time.Millisecond)
	if rand.IntN(100) < 8 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI10 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI10(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(30)) * time.Millisecond)
	if rand.IntN(100) < 3 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI11 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI11(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(21+rand.IntN(51)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI12 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI12(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(42+rand.IntN(92)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI13 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI13(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(73+rand.IntN(163)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI14 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI14(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(164+rand.IntN(314)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI15 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI15(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(7+rand.IntN(35)) * time.Millisecond)
	if rand.IntN(100) < 2 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI16 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI16(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(26+rand.IntN(56)) * time.Millisecond)
	if rand.IntN(100) < 3 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI17 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI17(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(47+rand.IntN(97)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI18 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI18(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(78+rand.IntN(168)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI19 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI19(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(169+rand.IntN(319)) * time.Millisecond)
	if rand.IntN(100) < 11 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI20 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI20(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(20)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI21 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI21(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(11+rand.IntN(61)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI22 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI22(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(52+rand.IntN(102)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI23 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI23(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(83+rand.IntN(173)) * time.Millisecond)
	if rand.IntN(100) < 10 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI24 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI24(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(174+rand.IntN(324)) * time.Millisecond)
	if rand.IntN(100) < 9 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI25 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI25(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(7+rand.IntN(25)) * time.Millisecond)
	if rand.IntN(100) < 3 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI26 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI26(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(16+rand.IntN(66)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI27 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI27(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(57+rand.IntN(107)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI28 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI28(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(88+rand.IntN(178)) * time.Millisecond)
	if rand.IntN(100) < 9 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI29 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI29(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(179+rand.IntN(329)) * time.Millisecond)
	if rand.IntN(100) < 7 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI30 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI30(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(30)) * time.Millisecond)
	if rand.IntN(100) < 2 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI31 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI31(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(21+rand.IntN(71)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI32 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI32(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(32+rand.IntN(112)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI33 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI33(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(93+rand.IntN(183)) * time.Millisecond)
	if rand.IntN(100) < 8 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI34 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI34(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(184+rand.IntN(334)) * time.Millisecond)
	if rand.IntN(100) < 12 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI35 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI35(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(7+rand.IntN(35)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI36 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI36(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(26+rand.IntN(76)) * time.Millisecond)
	if rand.IntN(100) < 3 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI37 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI37(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(37+rand.IntN(117)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI38 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI38(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(98+rand.IntN(188)) * time.Millisecond)
	if rand.IntN(100) < 7 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI39 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI39(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(189+rand.IntN(339)) * time.Millisecond)
	if rand.IntN(100) < 10 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI40 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI40(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(20)) * time.Millisecond)
	if rand.IntN(100) < 3 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI41 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI41(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(11+rand.IntN(41)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI42 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI42(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(42+rand.IntN(122)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI43 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI43(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(63+rand.IntN(193)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI44 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI44(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(194+rand.IntN(344)) * time.Millisecond)
	if rand.IntN(100) < 8 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI45 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI45(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(7+rand.IntN(25)) * time.Millisecond)
	if rand.IntN(100) < 2 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI46 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI46(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(16+rand.IntN(46)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI47 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI47(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(47+rand.IntN(127)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI48 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI48(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(68+rand.IntN(198)) * time.Millisecond)
	if rand.IntN(100) < 5 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI49 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI49(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(199+rand.IntN(349)) * time.Millisecond)
	if rand.IntN(100) < 6 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

// handleAPI50 simulates a generic endpoint with variable latency and an
// occasional server error.
func handleAPI50(w http.ResponseWriter, r *http.Request) {
	time.Sleep(time.Duration(2+rand.IntN(30)) * time.Millisecond)
	if rand.IntN(100) < 4 {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
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
