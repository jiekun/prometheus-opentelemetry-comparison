// Command client is a load generator for cmd/prometheus-app and
// cmd/otel-app: it fires the same 50 API requests they expose (see the
// endpoints slice below) at random against one or more targets so their
// request/duration/in-flight metrics move. It never inspects the
// response - triggering the request is the only job here, so failures
// (including a target being down) are counted but otherwise ignored.
package main

import (
	"context"
	"io"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

// endpoint describes one request shape.
type endpoint struct {
	method string
	path   string
}

// endpoints mirrors the 50 routes (handleAPI1 through handleAPI50)
// registered by cmd/prometheus-app and cmd/otel-app in their main()
// functions.
var endpoints = []endpoint{
	{http.MethodGet, "/api/api1"},
	{http.MethodGet, "/api/api2"},
	{http.MethodGet, "/api/api3"},
	{http.MethodGet, "/api/api4"},
	{http.MethodGet, "/api/api5"},
	{http.MethodGet, "/api/api6"},
	{http.MethodGet, "/api/api7"},
	{http.MethodGet, "/api/api8"},
	{http.MethodGet, "/api/api9"},
	{http.MethodGet, "/api/api10"},
	{http.MethodGet, "/api/api11"},
	{http.MethodGet, "/api/api12"},
	{http.MethodGet, "/api/api13"},
	{http.MethodGet, "/api/api14"},
	{http.MethodGet, "/api/api15"},
	{http.MethodGet, "/api/api16"},
	{http.MethodGet, "/api/api17"},
	{http.MethodGet, "/api/api18"},
	{http.MethodGet, "/api/api19"},
	{http.MethodGet, "/api/api20"},
	{http.MethodGet, "/api/api21"},
	{http.MethodGet, "/api/api22"},
	{http.MethodGet, "/api/api23"},
	{http.MethodGet, "/api/api24"},
	{http.MethodGet, "/api/api25"},
	{http.MethodGet, "/api/api26"},
	{http.MethodGet, "/api/api27"},
	{http.MethodGet, "/api/api28"},
	{http.MethodGet, "/api/api29"},
	{http.MethodGet, "/api/api30"},
	{http.MethodGet, "/api/api31"},
	{http.MethodGet, "/api/api32"},
	{http.MethodGet, "/api/api33"},
	{http.MethodGet, "/api/api34"},
	{http.MethodGet, "/api/api35"},
	{http.MethodGet, "/api/api36"},
	{http.MethodGet, "/api/api37"},
	{http.MethodGet, "/api/api38"},
	{http.MethodGet, "/api/api39"},
	{http.MethodGet, "/api/api40"},
	{http.MethodGet, "/api/api41"},
	{http.MethodGet, "/api/api42"},
	{http.MethodGet, "/api/api43"},
	{http.MethodGet, "/api/api44"},
	{http.MethodGet, "/api/api45"},
	{http.MethodGet, "/api/api46"},
	{http.MethodGet, "/api/api47"},
	{http.MethodGet, "/api/api48"},
	{http.MethodGet, "/api/api49"},
	{http.MethodGet, "/api/api50"},
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return n
}

// worker repeatedly picks a random target and endpoint, fires the
// request, and waits a random interval before the next one. It ignores
// the response body and any error beyond counting it - it exists purely
// to make the target apps' metrics move.
func worker(ctx context.Context, client *http.Client, targets []string, minInterval, maxInterval time.Duration, sent, failed *atomic.Int64) {
	jitter := maxInterval - minInterval
	for {
		ep := endpoints[rand.IntN(len(endpoints))]
		target := targets[rand.IntN(len(targets))]

		req, err := http.NewRequestWithContext(ctx, ep.method, target+ep.path, nil)
		if err == nil {
			resp, err := client.Do(req)
			sent.Add(1)
			if err != nil {
				failed.Add(1)
			} else {
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}
		}

		sleep := minInterval
		if jitter > 0 {
			sleep += time.Duration(rand.Int64N(int64(jitter)))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(sleep):
		}
	}
}

func main() {
	targets := strings.Split(getenv("CLIENT_TARGETS", "http://localhost:8080"), ",")
	for i := range targets {
		targets[i] = strings.TrimRight(strings.TrimSpace(targets[i]), "/")
	}
	workers := getenvInt("CLIENT_WORKERS", 4)
	minInterval := time.Duration(getenvInt("CLIENT_MIN_INTERVAL_MS", 5)) * time.Millisecond
	maxInterval := time.Duration(getenvInt("CLIENT_MAX_INTERVAL_MS", 50)) * time.Millisecond
	timeout := time.Duration(getenvInt("CLIENT_TIMEOUT_MS", 2000)) * time.Millisecond
	if maxInterval < minInterval {
		maxInterval = minInterval
	}

	httpClient := &http.Client{Timeout: timeout}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("client generating load against %v with %d workers (%s-%s between requests per worker)",
		targets, workers, minInterval, maxInterval)

	var sent, failed atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			worker(ctx, httpClient, targets, minInterval, maxInterval, &sent, &failed)
		}()
	}

	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-statusTicker.C:
			log.Printf("sent=%d failed=%d", sent.Load(), failed.Load())
		}
	}

	log.Println("shutting down")
	wg.Wait()
	log.Printf("final: sent=%d failed=%d", sent.Load(), failed.Load())
}
