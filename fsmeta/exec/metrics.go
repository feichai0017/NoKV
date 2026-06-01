// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package exec

import (
	"sync/atomic"
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
