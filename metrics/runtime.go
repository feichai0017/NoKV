// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

import "sync/atomic"

// MmapAdviceSnapshot captures mmap access-hint calls from the file layer.
type MmapAdviceSnapshot struct {
	Madvise       uint64 `json:"madvise"`
	MadviseFailed uint64 `json:"madvise_failed"`
}

// TablePrefetchSnapshot captures iterator block-prefetch scheduling counters.
type TablePrefetchSnapshot struct {
	Launched  uint64 `json:"launched"`
	Aborted   uint64 `json:"aborted"`
	Completed uint64 `json:"completed"`
}

var (
	mmapMadvise       atomic.Uint64
	mmapMadviseFailed atomic.Uint64

	tablePrefetchLaunched  atomic.Uint64
	tablePrefetchAborted   atomic.Uint64
	tablePrefetchCompleted atomic.Uint64
)

// RecordMmapMadvise records the outcome of one mmap access-hint call.
func RecordMmapMadvise(ok bool) {
	if ok {
		mmapMadvise.Add(1)
		return
	}
	mmapMadviseFailed.Add(1)
}

// MmapAdviceStats returns process-wide mmap access-hint counters.
func MmapAdviceStats() MmapAdviceSnapshot {
	return MmapAdviceSnapshot{
		Madvise:       mmapMadvise.Load(),
		MadviseFailed: mmapMadviseFailed.Load(),
	}
}

// RecordTablePrefetchLaunched records one successfully queued prefetch block.
func RecordTablePrefetchLaunched() {
	tablePrefetchLaunched.Add(1)
}

// RecordTablePrefetchAborted records one prefetch that was skipped or failed.
func RecordTablePrefetchAborted() {
	tablePrefetchAborted.Add(1)
}

// RecordTablePrefetchCompleted records one prefetch that loaded a block.
func RecordTablePrefetchCompleted() {
	tablePrefetchCompleted.Add(1)
}

// TablePrefetchStats returns process-wide iterator prefetch counters.
func TablePrefetchStats() TablePrefetchSnapshot {
	return TablePrefetchSnapshot{
		Launched:  tablePrefetchLaunched.Load(),
		Aborted:   tablePrefetchAborted.Load(),
		Completed: tablePrefetchCompleted.Load(),
	}
}
