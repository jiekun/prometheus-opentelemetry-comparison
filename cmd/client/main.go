// Command client is a load generator for cmd/prometheus-app and
// cmd/otel-app: it fires the same 10 API requests they expose (see the
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

// endpoint describes one request shape. path may contain "{id}", which is
// substituted with a random id before the request is sent - it must stay
// out of the metrics' path label on the server side, but the client
// doesn't care about that; it just needs a URL that resolves.
type endpoint struct {
	method string
	path   string
}

// endpoints mirrors the 10 routes registered by cmd/prometheus-app and
// cmd/otel-app in their main() functions.
var endpoints = []endpoint{
	{http.MethodGet, "/api/work"},
	{http.MethodPost, "/api/submit"},
	{http.MethodGet, "/api/users"},
	{http.MethodGet, "/api/users/{id}"},
	{http.MethodPost, "/api/users"},
	{http.MethodPut, "/api/users/{id}"},
	{http.MethodDelete, "/api/users/{id}"},
	{http.MethodGet, "/api/orders"},
	{http.MethodPost, "/api/orders"},
	{http.MethodGet, "/api/reports"},
}

// resolve fills in any {id} placeholder with a random id.
func (e endpoint) resolve() string {
	if !strings.Contains(e.path, "{id}") {
		return e.path
	}
	return strings.ReplaceAll(e.path, "{id}", strconv.Itoa(1+rand.IntN(1000)))
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

		req, err := http.NewRequestWithContext(ctx, ep.method, target+ep.resolve(), nil)
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
