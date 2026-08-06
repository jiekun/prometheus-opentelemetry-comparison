// Package procstats samples OS-level process metrics (CPU time, memory)
// that fall outside what the Go runtime reports on its own. Both
// cmd/prometheus-app and cmd/otel-app sample through this same package so
// any measured difference between the two comes from the instrumentation
// SDK, not from how the underlying numbers were collected.
package procstats

import (
	"os"

	"github.com/shirou/gopsutil/v3/process"
)

// Sample is a point-in-time read of process-level OS metrics.
type Sample struct {
	CPUSeconds float64 // cumulative user+system CPU time
	RSSBytes   uint64  // resident set size
	VSizeBytes uint64  // virtual memory size
}

// Reader samples the current process's OS-level metrics.
type Reader struct {
	proc *process.Process
}

// NewReader creates a Reader bound to the current process.
func NewReader() (*Reader, error) {
	p, err := process.NewProcess(int32(os.Getpid()))
	if err != nil {
		return nil, err
	}
	return &Reader{proc: p}, nil
}

// Sample reads the process's current cumulative CPU time and memory usage.
func (r *Reader) Sample() (Sample, error) {
	times, err := r.proc.Times()
	if err != nil {
		return Sample{}, err
	}
	mem, err := r.proc.MemoryInfo()
	if err != nil {
		return Sample{}, err
	}
	return Sample{
		CPUSeconds: times.User + times.System,
		RSSBytes:   mem.RSS,
		VSizeBytes: mem.VMS,
	}, nil
}
