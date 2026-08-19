// Command client is a load generator for cmd/prometheus-app and
// cmd/otel-app: it fires the same 50 API requests they expose (see the
// endpoints slice below, chosen at random) at a fixed rate per target
// (CLIENT_TARGET_RATE_HZ), so every target receives the same, precisely
// controlled request rate - not a "best effort" rate that depends on how
// slow that target's handlers happen to run - which keeps the load
// comparable when measuring resource usage across targets. It never
// inspects the response - triggering the request is the only job here,
// so failures (including a target being down) are counted but otherwise
// ignored.
package main

import (
	"context"
	"io"
	"log"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
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

func getenvFloat(key string, fallback float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		log.Fatalf("invalid %s=%q: %v", key, v, err)
	}
	return f
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

// clientMetrics exposes, per target host, how many requests this client
// has sent it and how many of those failed - so the load applied to each
// target app instance is directly visible in Prometheus, not just in
// this process's own log.
type clientMetrics struct {
	sentTotal   *prometheus.CounterVec
	failedTotal *prometheus.CounterVec
}

func newClientMetrics(reg prometheus.Registerer) *clientMetrics {
	m := &clientMetrics{
		sentTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "client_requests_sent_total",
			Help: "Total number of requests this client has sent to each target host.",
		}, []string{"target"}),
		failedTotal: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "client_requests_failed_total",
			Help: "Total number of requests this client sent to each target host that failed (transport error, not HTTP status).",
		}, []string{"target"}),
	}
	reg.MustRegister(m.sentTotal, m.failedTotal)
	return m
}

// hostOf returns just the IP/hostname a target points at, stripping
// scheme and port, so many targets on the same host (e.g. one per port)
// group together in the status log.
func hostOf(target string) string {
	u, err := url.Parse(target)
	if err != nil || u.Host == "" {
		return target
	}
	if h, _, err := net.SplitHostPort(u.Host); err == nil {
		return h
	}
	return u.Host
}

// logStatus logs sent/failed request counts summed per host (as
// determined by hostOf), followed by the grand total across all hosts.
func logStatus(hosts []string, sentByTarget, failedByTarget []atomic.Int64) {
	type counts struct{ sent, failed int64 }
	byHost := map[string]*counts{}
	var order []string
	var totalSent, totalFailed int64

	for i, h := range hosts {
		s, f := sentByTarget[i].Load(), failedByTarget[i].Load()
		totalSent += s
		totalFailed += f
		c, ok := byHost[h]
		if !ok {
			c = &counts{}
			byHost[h] = c
			order = append(order, h)
		}
		c.sent += s
		c.failed += f
	}
	sort.Strings(order)
	for _, h := range order {
		c := byHost[h]
		log.Printf("  %s: sent=%d failed=%d", h, c.sent, c.failed)
	}
	log.Printf("sent=%d failed=%d", totalSent, totalFailed)
}

// targetCounters bundles the two places a completed request against one
// target gets recorded: the in-process atomic counters behind the
// periodic text log, and the Prometheus counters exposed for scraping.
type targetCounters struct {
	sentAtomic, failedAtomic *atomic.Int64
	sentMetric, failedMetric prometheus.Counter
}

func (c *targetCounters) recordSent() {
	c.sentAtomic.Add(1)
	c.sentMetric.Inc()
}

func (c *targetCounters) recordFailed() {
	c.failedAtomic.Add(1)
	c.failedMetric.Inc()
}

// targetWorker fires requests at target on a fixed-rate ticker (one tick
// every "interval") for as long as ctx is alive. Each tick's request runs
// in its own goroutine rather than blocking the ticker loop, so a slow or
// hung target delays only its own in-flight requests - never the rate at
// which new ones are fired, at this target or any other. A random
// startup delay (up to one interval) staggers the tickers across targets
// so the aggregate load doesn't arrive in synchronized bursts. It ignores
// the response body and any error beyond counting it - it exists purely
// to make the target apps' metrics move.
func targetWorker(ctx context.Context, client *http.Client, target string, interval time.Duration, c *targetCounters) {
	select {
	case <-ctx.Done():
		return
	case <-time.After(time.Duration(rand.Int64N(int64(interval)))):
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			ep := endpoints[rand.IntN(len(endpoints))]
			go func() {
				req, err := http.NewRequestWithContext(ctx, ep.method, target+ep.path, nil)
				if err != nil {
					return
				}
				resp, err := client.Do(req)
				c.recordSent()
				if err != nil {
					c.recordFailed()
					return
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
			}()
		}
	}
}

func main() {
	targets := strings.Split(getenv("CLIENT_TARGETS", "http://localhost:8080"), ",")
	for i := range targets {
		targets[i] = strings.TrimRight(strings.TrimSpace(targets[i]), "/")
	}
	rateHz := getenvFloat("CLIENT_TARGET_RATE_HZ", 10)
	if rateHz <= 0 {
		log.Fatalf("invalid CLIENT_TARGET_RATE_HZ=%v: must be > 0", rateHz)
	}
	interval := time.Duration(float64(time.Second) / rateHz)
	if interval <= 0 {
		interval = 1
	}
	timeout := time.Duration(getenvInt("CLIENT_TIMEOUT_MS", 2000)) * time.Millisecond
	metricsAddr := getenv("CLIENT_METRICS_ADDR", ":9200")

	httpClient := &http.Client{
		Timeout: timeout,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: 4,
		},
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	reg := prometheus.NewRegistry()
	metrics := newClientMetrics(reg)
	metricsMux := http.NewServeMux()
	metricsMux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsSrv := &http.Server{Addr: metricsAddr, Handler: metricsMux}
	go func() {
		log.Printf("client metrics listening on %s (metrics at /metrics)", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("metrics listen: %v", err)
		}
	}()

	log.Printf("client generating load against %d targets at %.3g req/s each (%s between requests per target, ~%.3g req/s total)",
		len(targets), rateHz, interval, rateHz*float64(len(targets)))

	hosts := make([]string, len(targets))
	for i, target := range targets {
		hosts[i] = hostOf(target)
	}
	sentByTarget := make([]atomic.Int64, len(targets))
	failedByTarget := make([]atomic.Int64, len(targets))

	var wg sync.WaitGroup
	for i, target := range targets {
		counters := &targetCounters{
			sentAtomic:   &sentByTarget[i],
			failedAtomic: &failedByTarget[i],
			sentMetric:   metrics.sentTotal.WithLabelValues(hosts[i]),
			failedMetric: metrics.failedTotal.WithLabelValues(hosts[i]),
		}
		wg.Add(1)
		go func(target string, counters *targetCounters) {
			defer wg.Done()
			targetWorker(ctx, httpClient, target, interval, counters)
		}(target, counters)
	}

	statusTicker := time.NewTicker(10 * time.Second)
	defer statusTicker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-statusTicker.C:
			logStatus(hosts, sentByTarget, failedByTarget)
		}
	}

	log.Println("shutting down")
	wg.Wait()
	log.Println("final:")
	logStatus(hosts, sentByTarget, failedByTarget)
}
