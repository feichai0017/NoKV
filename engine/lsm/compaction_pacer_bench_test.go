package lsm

import (
	"testing"
	"time"
)

// BenchmarkCompactionPacerNilCharge measures the hot-path overhead when pacing
// is disabled (the production default). Confirms the nil-check short-circuit
// keeps charge() effectively free for callers that opt out.
func BenchmarkCompactionPacerNilCharge(b *testing.B) {
	var p *compactionPacer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.charge(4 << 10)
	}
}

// BenchmarkCompactionPacerChargeFromBucket measures the cost of charge() when
// the bucket already has enough tokens (no-sleep path). This is the steady-
// state per-block cost of pacing on a budget that exceeds the workload rate.
//
// Compared against BenchmarkCompactionPacerNilCharge, the delta is the per-
// block overhead of enabling pacing under no contention.
func BenchmarkCompactionPacerChargeFromBucket(b *testing.B) {
	now := time.Unix(0, 0)
	p := newCompactionPacer(1 << 30) // 1 GiB/s budget
	p.now = func() time.Time { return now }
	p.sleep = func(d time.Duration) { now = now.Add(d) }
	p.last = now
	// Pre-fill bucket so charge() always finds tokens available.
	p.tokens = 1 << 30
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.charge(4 << 10)
	}
}

// BenchmarkCompactionPacerChargeWithMockSleep simulates the throttled path
// using a no-op sleep, isolating bookkeeping cost from real wall time. Useful
// to confirm the refill loop overhead stays bounded under sustained
// throttling.
func BenchmarkCompactionPacerChargeWithMockSleep(b *testing.B) {
	now := time.Unix(0, 0)
	p := newCompactionPacer(64 << 20) // 64 MiB/s
	p.now = func() time.Time { return now }
	p.sleep = func(d time.Duration) { now = now.Add(d) }
	p.last = now
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.charge(64 << 10)
	}
}

// BenchmarkCompactionPacerStats measures the cost of reading observability
// counters. Should be near-zero so monitoring loops can poll freely.
func BenchmarkCompactionPacerStats(b *testing.B) {
	p := newCompactionPacer(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Stats()
	}
}
