package bench

import (
	"context"
	"math"
	"sync"
	"time"
)

// RateLimiter implements a host-wide exponential decay token bucket.
// All goroutines for a single host share one instance.
//
// Rate formula: rate = ln(2) / halfLife * threshold
// Default halfLife=1s, threshold=150 → ~103.97 ops/s
//
// With N users, the caller multiplies halfLife by N before constructing,
// which keeps the total host rate constant while dividing it equally per user.
type RateLimiter struct {
	mu        sync.Mutex
	load      float64
	lastTime  time.Time
	halfLife  time.Duration
	threshold float64
}

// NewRateLimiter creates a RateLimiter. halfLife should already be scaled
// by the number of users before calling (halfLife * numUsers).
func NewRateLimiter(halfLife time.Duration, threshold float64) *RateLimiter {
	return &RateLimiter{
		halfLife:  halfLife,
		threshold: threshold,
	}
}

// Rate returns the steady-state target rate in ops/s: ln(2)/halfLife * threshold.
func (r *RateLimiter) Rate() float64 {
	return math.Log(2) / r.halfLife.Seconds() * r.threshold
}

// Wait blocks until the caller is allowed to proceed, then increments the load
// counter. Returns ctx.Err() immediately if the context is cancelled while
// waiting — the caller should treat a non-nil return as a signal to stop.
//
// Uses a two-phase lock: compute sleep duration under lock, sleep outside lock
// (so other goroutines can proceed), then re-acquire to finalize the increment.
func (r *RateLimiter) Wait(ctx context.Context) error {
	r.mu.Lock()

	now := time.Now()
	halfLifeSec := r.halfLife.Seconds()

	if !r.lastTime.IsZero() {
		dt := now.Sub(r.lastTime).Seconds()
		r.load *= math.Pow(0.5, dt/halfLifeSec)
	}

	var sleepDur time.Duration
	if r.load >= r.threshold {
		// Solve: load * 0.5^(t/HL) < threshold → t = HL * log2(load/threshold)
		sleepSec := halfLifeSec * math.Log2(r.load/r.threshold)
		sleepDur = time.Duration(sleepSec * float64(time.Second))
	}

	r.mu.Unlock()

	if sleepDur > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(sleepDur):
		}
	}

	// Re-acquire, re-decay from the new moment, then commit the increment.
	r.mu.Lock()
	now2 := time.Now()
	if !r.lastTime.IsZero() {
		dt2 := now2.Sub(r.lastTime).Seconds()
		r.load *= math.Pow(0.5, dt2/halfLifeSec)
	}
	r.load++
	r.lastTime = now2
	r.mu.Unlock()
	return nil
}
