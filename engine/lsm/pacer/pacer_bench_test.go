// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package pacer

import (
	"testing"
	"time"
)

// BenchmarkPacerNilCharge measures the hot-path overhead when pacing is
// disabled (the production default). Confirms the nil-check short-circuit
// keeps Charge effectively free for callers that opt out.
func BenchmarkPacerNilCharge(b *testing.B) {
	var p *Pacer
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Charge(4 << 10)
	}
}

// BenchmarkPacerChargeFromBucket measures the cost of Charge when the bucket
// already has enough tokens (no-sleep path). This is the steady-state per-block
// cost of pacing on a budget that exceeds the workload rate.
//
// Compared against BenchmarkPacerNilCharge, the delta is the per-block
// overhead of enabling pacing under no contention.
func BenchmarkPacerChargeFromBucket(b *testing.B) {
	now := time.Unix(0, 0)
	p := New(1 << 30) // 1 GiB/s budget
	p.SetClock(func() time.Time { return now }, func(d time.Duration) { now = now.Add(d) })
	// Pre-fill bucket so Charge always finds tokens available.
	p.PrefillForTest(1 << 30)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Charge(4 << 10)
	}
}

// BenchmarkPacerChargeWithMockSleep simulates the throttled path using a
// no-op sleep, isolating bookkeeping cost from real wall time. Useful to
// confirm the refill loop overhead stays bounded under sustained throttling.
func BenchmarkPacerChargeWithMockSleep(b *testing.B) {
	now := time.Unix(0, 0)
	p := New(64 << 20) // 64 MiB/s
	p.SetClock(func() time.Time { return now }, func(d time.Duration) { now = now.Add(d) })
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p.Charge(64 << 10)
	}
}

// BenchmarkPacerStats measures the cost of reading observability counters.
// Should be near-zero so monitoring loops can poll freely.
func BenchmarkPacerStats(b *testing.B) {
	p := New(1 << 20)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Stats()
	}
}
