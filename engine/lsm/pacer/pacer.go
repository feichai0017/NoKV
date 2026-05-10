// Package pacer is a token-bucket rate limiter used to cap background
// compaction output. It is policy-free and engine-free: callers decide when to
// charge() bytes; this package only enforces the configured throughput.
//
// What it solves:
//
//	Background compaction can issue multi-hundred-MB writes in seconds. On
//	shared disks, that throughput burst contends with foreground writes and
//	reads, causing p99 latency spikes. Without pacing, a steady-state workload
//	sees periodic latency cliffs every time a large compaction kicks off.
//
//	Pacing converts those bursts into a steady output rate. p99 latency stays
//	flat at the cost of slightly slower compaction completion times.
//
// What it does NOT do:
//
//   - It does not pace memtable flush. Flush is the path that drains write
//     backlog; throttling it would manufacture write stalls. The decision to
//     skip flush belongs to the caller, not this package.
//   - It does not pace SST file IO directly; it only paces the in-process
//     copy step inside the table builder. The OS still owns kernel IO
//     scheduling.
//   - It is opt-in. New(bytesPerSec <= 0) returns nil so callers can use a
//     single nil-check short-circuit instead of branching on enabled state.
//
// Concurrency:
//
//	Charge is safe to call from multiple goroutines. The bucket is shared so
//	the configured rate caps total charged output, not per caller.
package pacer

import (
	"sync"
	"sync/atomic"
	"time"
)

// Pacer is a token-bucket rate limiter measured in bytes per second.
type Pacer struct {
	mu          sync.Mutex
	bytesPerSec int64
	tokens      int64
	last        time.Time

	// now and sleep are pluggable so tests can drive deterministic schedules.
	// Production callers leave them at the time.Now / time.Sleep defaults
	// installed by New.
	now   func() time.Time
	sleep func(time.Duration)

	// charged accumulates total bytes that have passed through Charge. It is
	// updated lock-free for cheap monitoring; treated as best-effort and may
	// briefly trail concrete bucket state under contention.
	charged atomic.Int64

	// sleptNs accumulates total wall-clock nanoseconds spent sleeping for
	// tokens. Together with charged, it lets monitors compute the effective
	// throttle ratio (sleptNs / charged) and surface pacer pressure.
	sleptNs atomic.Int64
}

// Stats captures observable pacer counters.
type Stats struct {
	BytesPerSec   int64
	BytesCharged  int64
	NanosThrottle int64
}

// New constructs a pacer with the configured throughput cap. Returns nil when
// bytesPerSec <= 0 so callers can use a single nil-check short-circuit instead
// of branching on enabled state.
func New(bytesPerSec int64) *Pacer {
	if bytesPerSec <= 0 {
		return nil
	}
	return &Pacer{
		bytesPerSec: bytesPerSec,
		now:         time.Now,
		sleep:       time.Sleep,
		last:        time.Now(),
	}
}

// Charge consumes n bytes from the bucket, blocking when tokens are
// insufficient. Safe to call with a nil receiver (no-op).
func (p *Pacer) Charge(n int) {
	if p == nil || n <= 0 || p.bytesPerSec <= 0 {
		return
	}
	remaining := int64(n)
	for remaining > 0 {
		p.mu.Lock()
		p.refillLocked(p.now())
		if p.tokens > 0 {
			take := min(p.tokens, remaining)
			p.tokens -= take
			remaining -= take
			p.charged.Add(take)
			p.mu.Unlock()
			continue
		}
		need := min(remaining, p.bytesPerSec)
		wait := durationForBytes(need, p.bytesPerSec)
		p.mu.Unlock()
		p.sleptNs.Add(wait.Nanoseconds())
		p.sleep(wait)
	}
}

// Stats returns a snapshot of pacer observability counters. Safe on nil.
func (p *Pacer) Stats() Stats {
	if p == nil {
		return Stats{}
	}
	return Stats{
		BytesPerSec:   p.bytesPerSec,
		BytesCharged:  p.charged.Load(),
		NanosThrottle: p.sleptNs.Load(),
	}
}

// SetClock overrides the time source for deterministic tests. Production code
// should not call this.
func (p *Pacer) SetClock(now func() time.Time, sleep func(time.Duration)) {
	if p == nil {
		return
	}
	p.now = now
	p.sleep = sleep
	p.last = now()
}

// PrefillForTest seeds the token bucket so deterministic benchmarks can
// exercise the no-sleep path.
func (p *Pacer) PrefillForTest(tokens int64) {
	if p == nil {
		return
	}
	p.tokens = tokens
}

func (p *Pacer) refillLocked(now time.Time) {
	if p.last.IsZero() {
		p.last = now
		return
	}
	elapsed := now.Sub(p.last)
	if elapsed <= 0 {
		return
	}
	add := elapsed.Nanoseconds() * p.bytesPerSec / int64(time.Second)
	if add <= 0 {
		return
	}
	p.tokens = min(p.tokens+add, p.bytesPerSec)
	p.last = now
}

func durationForBytes(bytes int64, bytesPerSec int64) time.Duration {
	if bytes <= 0 || bytesPerSec <= 0 {
		return 0
	}
	nanos := (bytes*int64(time.Second) + bytesPerSec - 1) / bytesPerSec
	return time.Duration(nanos)
}
