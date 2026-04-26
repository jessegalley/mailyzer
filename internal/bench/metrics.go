package bench

import (
	"fmt"
	"io"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Metrics collects benchmark measurements in a thread-safe manner.
type Metrics struct {
	startTime time.Time
	count     atomic.Int64
	bytes     atomic.Int64
	errors    atomic.Int64
	mu        sync.Mutex
	latencies []time.Duration
	errlog    *log.Logger // nil means errors are counted but not logged
}

// NewMetrics initializes a Metrics instance. If errWriter is non-nil, error
// messages are written there in addition to being counted.
func NewMetrics(errWriter io.Writer) *Metrics {
	m := &Metrics{startTime: time.Now()}
	if errWriter != nil {
		m.errlog = log.New(errWriter, "", log.LstdFlags)
	}
	return m
}

// RecordOp records a completed IMAP command. latency is the round-trip
// duration; n is the number of messages; b is the total bytes transferred.
// One latency sample is stored per call.
func (m *Metrics) RecordOp(latency time.Duration, n, b int64) {
	m.count.Add(n)
	m.bytes.Add(b)
	m.mu.Lock()
	m.latencies = append(m.latencies, latency)
	m.mu.Unlock()
}

// RecordError increments the error counter and, if an errlog is configured,
// writes the formatted message there. It does not print to stdout/stderr.
func (m *Metrics) RecordError(format string, args ...any) {
	m.errors.Add(1)
	if m.errlog != nil {
		m.errlog.Printf(format, args...)
	}
}

// Count returns the current total message count.
func (m *Metrics) Count() int64 {
	return m.count.Load()
}

// Elapsed returns the time since the benchmark started.
func (m *Metrics) Elapsed() time.Duration {
	return time.Since(m.startTime)
}

// Rate returns overall messages per second since start.
func (m *Metrics) Rate() float64 {
	elapsed := m.Elapsed().Seconds()
	if elapsed == 0 {
		return 0
	}
	return float64(m.Count()) / elapsed
}

// ProgressState carries the per-tick snapshot used to compute windowed rates.
type ProgressState struct {
	Count int64
	Bytes int64
	Time  time.Time
}

// PrintProgress writes a single progress line and returns an updated snapshot
// for the next tick.
func (m *Metrics) PrintProgress(w io.Writer, prev ProgressState) ProgressState {
	now := time.Now()
	cur := m.count.Load()
	curB := m.bytes.Load()
	errs := m.errors.Load()
	elapsed := now.Sub(m.startTime)
	intervalSec := now.Sub(prev.Time).Seconds()

	var recentMsgRate, recentByteRate float64
	if intervalSec > 0 {
		recentMsgRate = float64(cur-prev.Count) / intervalSec
		recentByteRate = float64(curB-prev.Bytes) / intervalSec
	}
	overallMsgRate := float64(cur) / elapsed.Seconds()
	overallByteRate := float64(curB) / elapsed.Seconds()

	errStr := ""
	if errs > 0 {
		errStr = fmt.Sprintf("  errs=%d", errs)
	}

	fmt.Fprintf(w, "[%.1fs] msgs=%-8d  %.1f msg/s (recent %.1f)  %s/s (recent %s/s)%s\n",
		elapsed.Seconds(), cur,
		overallMsgRate, recentMsgRate,
		formatBytes(overallByteRate), formatBytes(recentByteRate),
		errStr,
	)
	return ProgressState{Count: cur, Bytes: curB, Time: now}
}

// PrintSummary writes the final benchmark report.
func (m *Metrics) PrintSummary(w io.Writer) {
	m.mu.Lock()
	lats := make([]time.Duration, len(m.latencies))
	copy(lats, m.latencies)
	m.mu.Unlock()

	elapsed := m.Elapsed()
	cnt := m.Count()
	rate := float64(cnt) / elapsed.Seconds()
	totalBytes := m.bytes.Load()
	byteRate := float64(totalBytes) / elapsed.Seconds()
	errs := m.errors.Load()

	fmt.Fprintf(w, "\n=== Benchmark Results ===\n")
	fmt.Fprintf(w, "Duration:  %.3fs  Messages: %d  Rate: %.2f msg/s\n",
		elapsed.Seconds(), cnt, rate)
	fmt.Fprintf(w, "Data:      %s total  %s/s\n",
		formatBytes(float64(totalBytes)), formatBytes(byteRate))
	fmt.Fprintf(w, "Errors:    %d\n", errs)

	if len(lats) == 0 {
		fmt.Fprintln(w, "Latency:   no samples")
		return
	}

	sort.Slice(lats, func(i, j int) bool { return lats[i] < lats[j] })
	fmt.Fprintf(w, "Latency:   p50=%s  p95=%s  p99=%s  p99.9=%s\n",
		formatDur(percentile(lats, 0.50)),
		formatDur(percentile(lats, 0.95)),
		formatDur(percentile(lats, 0.99)),
		formatDur(percentile(lats, 0.999)),
	)
}

func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)) * p)
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func formatBytes(bps float64) string {
	switch {
	case bps >= 1<<30:
		return fmt.Sprintf("%.2f GB", bps/(1<<30))
	case bps >= 1<<20:
		return fmt.Sprintf("%.2f MB", bps/(1<<20))
	case bps >= 1<<10:
		return fmt.Sprintf("%.2f KB", bps/(1<<10))
	default:
		return fmt.Sprintf("%.0f B", bps)
	}
}

func formatDur(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.3fs", d.Seconds())
	}
	if d >= time.Millisecond {
		return fmt.Sprintf("%.1fms", float64(d)/float64(time.Millisecond))
	}
	return fmt.Sprintf("%.1fµs", float64(d)/float64(time.Microsecond))
}
