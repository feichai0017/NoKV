package lsm

import (
	"sync"
	"sync/atomic"
	"time"
)

// compactionPacer is a token-bucket rate limiter for background compaction
// output. It exists to keep compaction from monopolizing disk write bandwidth
// and amplifying foreground request tail latency.
//
// What it solves:
//
//	Background compaction can issue multi-hundred-MB writes in seconds. On
//	shared disks, that throughput burst contends with foreground writes and
//	reads, causing p99 latency spikes. Without pacing, a steady-state workload
//	sees periodic latency cliffs every time a large compaction kicks off.
//
//	Pacing converts those bursts into a steady output rate
//	(CompactionWriteBytesPerSec). p99 latency stays flat at the cost of
//	slightly slower compaction completion times.
//
// What it does NOT do:
//
//   - It does not pace memtable flush. Flush is the path that drains write
//     backlog; throttling it would manufacture write stalls.
//   - It does not pace SST file IO directly; it only paces the in-process
//     copy step inside the table builder. The OS still owns kernel IO
//     scheduling.
//   - It is opt-in. CompactionWriteBytesPerSec <= 0 disables it (no allocation,
//     no charge). Default behavior is unchanged.
//
// L0 bypass:
//
//	When L0 table count crosses CompactionPacingBypassL0, the next compaction
//	build skips pacing entirely. The reasoning: if L0 is approaching stall,
//	the foreground latency cost of unpaced compaction is cheaper than the
//	write stall a paced compaction would not prevent. Bypass is decided at
//	build start; an in-progress compaction does not switch mid-flight.
//
// Concurrency:
//
//	charge() is safe to call from multiple compaction goroutines. The bucket
//	is shared so the configured rate caps total compaction output, not per
//	compactor.
type compactionPacer struct {
	mu          sync.Mutex
	bytesPerSec int64
	tokens      int64
	last        time.Time
	now         func() time.Time
	sleep       func(time.Duration)

	// charged accumulates total bytes that have passed through charge(). It is
	// updated lock-free for cheap monitoring; treated as best-effort and may
	// briefly trail concrete bucket state under contention.
	charged atomic.Int64

	// sleptNs accumulates total wall-clock nanoseconds spent sleeping for
	// tokens. Together with charged, it lets monitors compute the effective
	// throttle ratio (sleptNs / charged) and surface pacer pressure.
	sleptNs atomic.Int64
}

// PacerStats captures observable pacer counters.
type PacerStats struct {
	BytesPerSec   int64
	BytesCharged  int64
	NanosThrottle int64
}

// newCompactionPacer constructs a pacer with the configured throughput cap.
// Returns nil when bytesPerSec <= 0 so callers can use a single nil-check
// short-circuit instead of branching on enabled state.
func newCompactionPacer(bytesPerSec int64) *compactionPacer {
	if bytesPerSec <= 0 {
		return nil
	}
	return &compactionPacer{
		bytesPerSec: bytesPerSec,
		now:         time.Now,
		sleep:       time.Sleep,
		last:        time.Now(),
	}
}

// charge consumes n bytes from the bucket, blocking when tokens are
// insufficient. Safe to call with a nil receiver (no-op).
func (p *compactionPacer) charge(n int) {
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
func (p *compactionPacer) Stats() PacerStats {
	if p == nil {
		return PacerStats{}
	}
	return PacerStats{
		BytesPerSec:   p.bytesPerSec,
		BytesCharged:  p.charged.Load(),
		NanosThrottle: p.sleptNs.Load(),
	}
}

func (p *compactionPacer) refillLocked(now time.Time) {
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

// compactionPacerForBuild returns the active pacer for a new build, or nil
// when pacing is disabled or L0 backlog suggests bypass is appropriate.
//
// Decision is taken at build start; an in-progress build never re-checks
// bypass. This keeps the charge() hot path branch-free past the nil check.
func (lm *levelManager) compactionPacerForBuild() *compactionPacer {
	if lm == nil || lm.compactionPacer == nil {
		return nil
	}
	if lm.compactionPacerBypassActive() {
		return nil
	}
	return lm.compactionPacer
}

// compactionPacerBypassActive reports whether L0 has reached the configured
// bypass threshold. When true, new compaction builds run unpaced so that L0
// can be drained quickly enough to avoid foreground write stalls.
func (lm *levelManager) compactionPacerBypassActive() bool {
	if lm == nil || lm.opt == nil || lm.opt.CompactionPacingBypassL0 <= 0 || len(lm.levels) == 0 || lm.levels[0] == nil {
		return false
	}
	return lm.levels[0].numTables() >= lm.opt.CompactionPacingBypassL0
}

// CompactionPacerStats exposes pacer observability for diagnostics. Returns
// zero stats when pacing is disabled.
func (lm *levelManager) CompactionPacerStats() PacerStats {
	if lm == nil {
		return PacerStats{}
	}
	return lm.compactionPacer.Stats()
}
