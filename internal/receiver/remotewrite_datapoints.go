package receiver

import (
	prompb "github.com/prometheus/prometheus/prompb"
	writev2 "github.com/prometheus/prometheus/prompb/io/prometheus/write/v2"
)

// nativeHistogramBuckets is satisfied by both prompb.Histogram (Remote
// Write 1.0's optional native-histogram field) and writev2.Histogram
// (Remote Write 2.0's) - both gogo-generated types with an identical
// bucket-field shape.
type nativeHistogramBuckets interface {
	GetPositiveDeltas() []int64
	GetPositiveCounts() []float64
	GetNegativeDeltas() []int64
	GetNegativeCounts() []float64
	GetCustomValues() []float64
}

// countHistogramDataPoints returns how many data points one native
// histogram represents, counting each populated bucket separately - the
// same convention countMetricSamples (otlp_samples.go) uses for OTLP's
// classic Histogram type. Deltas and Counts are alternate encodings of the
// same buckets (integer vs float histograms), so exactly one of each pair
// is ever populated; a histogram with no spans at all (just custom bucket
// boundaries, Remote Write 2.0's schema=-53) falls back to counting those,
// and one with neither counts as a single data point.
func countHistogramDataPoints(h nativeHistogramBuckets) int {
	n := len(h.GetPositiveDeltas()) + len(h.GetPositiveCounts()) +
		len(h.GetNegativeDeltas()) + len(h.GetNegativeCounts())
	if n > 0 {
		return n
	}
	if n := len(h.GetCustomValues()); n > 0 {
		return n
	}
	return 1
}

// countRemoteWriteV1DataPoints returns the total data point count for a
// Remote Write 1.0 request: one per regular sample, plus each native
// histogram's buckets counted separately (see countHistogramDataPoints).
// Classic Prometheus histograms don't appear here at all - scraped from
// exposition format, each of their buckets already arrives as its own
// TimeSeries with a regular Sample, so they're covered by the sample count.
func countRemoteWriteV1DataPoints(wr *prompb.WriteRequest) int {
	total := 0
	for _, ts := range wr.GetTimeseries() {
		total += len(ts.GetSamples())
		for _, h := range ts.GetHistograms() {
			total += countHistogramDataPoints(&h)
		}
	}
	return total
}

// countRemoteWriteV2DataPoints is countRemoteWriteV1DataPoints for Remote
// Write 2.0's distinct (but shape-identical) generated types.
func countRemoteWriteV2DataPoints(req *writev2.Request) int {
	total := 0
	for _, ts := range req.GetTimeseries() {
		total += len(ts.GetSamples())
		for _, h := range ts.GetHistograms() {
			total += countHistogramDataPoints(&h)
		}
	}
	return total
}
