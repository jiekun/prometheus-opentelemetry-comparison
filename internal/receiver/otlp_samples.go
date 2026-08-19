package receiver

import (
	otlp "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
)

// countSamples returns how many Prometheus samples req represents, using
// the same accounting Prometheus itself uses when scraping or remote
// writing - not a per-data-point count, which would undercount anything
// bucketed. A Gauge or Sum data point is one sample each. A classic
// Histogram data point is flattened the way Prometheus flattens it on
// the wire: one sample per bucket (including the implicit +Inf bucket),
// plus _count, plus _sum if present. A Summary data point is flattened
// the same way: one sample per quantile, plus _count and _sum. An
// ExponentialHistogram data point is one sample, matching how a
// Prometheus native histogram stores the whole histogram as a single
// sample rather than flattening it (unlike a classic histogram, it
// generally isn't emitted by anything in this project, but is handled
// here for a receiver that might see one).
func countSamples(req *otlp.ExportMetricsServiceRequest) int {
	total := 0
	for _, rs := range req.GetResourceMetrics() {
		for _, sm := range rs.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				total += countMetricSamples(m)
			}
		}
	}
	return total
}

func countMetricSamples(m *metricspb.Metric) int {
	switch {
	case m.GetGauge() != nil:
		return len(m.GetGauge().GetDataPoints())
	case m.GetSum() != nil:
		return len(m.GetSum().GetDataPoints())
	case m.GetHistogram() != nil:
		n := 0
		for _, dp := range m.GetHistogram().GetDataPoints() {
			n += len(dp.GetBucketCounts()) // one _bucket sample per bucket, +Inf included
			n++                            // _count
			if dp.Sum != nil {
				n++ // _sum
			}
		}
		return n
	case m.GetExponentialHistogram() != nil:
		return len(m.GetExponentialHistogram().GetDataPoints())
	case m.GetSummary() != nil:
		n := 0
		for _, dp := range m.GetSummary().GetDataPoints() {
			n += len(dp.GetQuantileValues()) // one sample per quantile
			n += 2                           // _count, _sum
		}
		return n
	default:
		return 0
	}
}
