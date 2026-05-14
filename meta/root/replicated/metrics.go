// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package replicated

import "sync/atomic"

type metrics struct {
	observeCacheHitTotal      atomic.Uint64
	observeCacheMissTotal     atomic.Uint64
	checkpointLoadTotal       atomic.Uint64
	committedTailReadTotal    atomic.Uint64
	observeCacheInvalidations atomic.Uint64
}

func (m *metrics) snapshot() map[string]any {
	if m == nil {
		return map[string]any{}
	}
	return map[string]any{
		"observe_cache_hit_total":           m.observeCacheHitTotal.Load(),
		"observe_cache_miss_total":          m.observeCacheMissTotal.Load(),
		"checkpoint_load_total":             m.checkpointLoadTotal.Load(),
		"committed_tail_read_total":         m.committedTailReadTotal.Load(),
		"observe_cache_invalidations_total": m.observeCacheInvalidations.Load(),
	}
}
