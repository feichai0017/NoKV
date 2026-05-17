// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"sync/atomic"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

// recordUint64Max bumps the running maximum stored in max if value is larger,
// using a CAS loop so concurrent recordX calls do not lose a peak update.
func recordUint64Max(max *atomic.Uint64, value uint64) {
	if max == nil {
		return
	}
	for {
		old := max.Load()
		if value <= old {
			return
		}
		if max.CompareAndSwap(old, value) {
			return
		}
	}
}

// recordSlow attributes one slow-path admission to its slow reason bucket.
func (s *visibleAdmissionCounters) recordSlow(reason compile.SlowReason) {
	s.slowTotal.Add(1)
	switch reason {
	case compile.SlowReasonReadOnly:
		s.slowReadOnlyTotal.Add(1)
	case compile.SlowReasonRangeRead:
		s.slowRangeReadTotal.Add(1)
	case compile.SlowReasonDurabilityBarrier:
		s.slowDurabilityTotal.Add(1)
	case compile.SlowReasonCrossBucket:
		s.slowCrossBucketTotal.Add(1)
	case compile.SlowReasonSharedQuota:
		s.slowSharedQuotaTotal.Add(1)
	case compile.SlowReasonDynamicWriteSet:
		s.slowDynamicWriteTotal.Add(1)
	case compile.SlowReasonMaintenanceScan:
		s.slowMaintenanceTotal.Add(1)
	default:
		s.slowUnknownTotal.Add(1)
	}
}

// recordFallback bumps the per-affinity fallback counter and updates the
// monotonic consecutive-fallback gauge.
func (s *atomicOnePhaseCounters) recordFallback(affinity string) {
	s.mu.Lock()
	next := s.fallbacksByAffinity[affinity] + 1
	s.fallbacksByAffinity[affinity] = next
	s.mu.Unlock()
	s.consecutiveFallbacks.Store(next)
}

// recordSuccess clears the per-affinity fallback streak.
func (s *atomicOnePhaseCounters) recordSuccess(affinity string) {
	s.mu.Lock()
	delete(s.fallbacksByAffinity, affinity)
	s.mu.Unlock()
	s.consecutiveFallbacks.Store(0)
}
