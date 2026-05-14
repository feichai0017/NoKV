// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
)

func TestWriteMetricsSnapshot(t *testing.T) {
	m := NewWriteMetrics()
	m.UpdateQueue(5, 10, 1024)
	m.RecordBatch(2, 20, 2048, int64(3*time.Millisecond))
	m.RecordApply(7 * time.Millisecond)
	m.RecordSync(2*time.Millisecond, 3)

	snap := m.Snapshot()
	if snap.QueueLen != 5 || snap.QueueEntries != 10 || snap.QueueBytes != 1024 {
		t.Fatalf("queue snapshot mismatch: %+v", snap)
	}
	if snap.Batches != 2 || snap.AvgBatchEntries <= 0 || snap.AvgBatchBytes <= 0 {
		t.Fatalf("batch snapshot mismatch: %+v", snap)
	}
	if snap.AvgRequestWaitMs <= 0 || snap.AvgApplyMs <= 0 {
		t.Fatalf("timing averages missing: %+v", snap)
	}
	if snap.SyncSamples != 1 {
		t.Fatalf("expected 1 sync sample, got %d", snap.SyncSamples)
	}
	if snap.AvgSyncMs <= 0 {
		t.Fatalf("expected positive AvgSyncMs, got %f", snap.AvgSyncMs)
	}
	if snap.AvgSyncBatch != 3.0 {
		t.Fatalf("expected AvgSyncBatch=3, got %f", snap.AvgSyncBatch)
	}
}

func TestCacheCountersSnapshot(t *testing.T) {
	counters := NewCacheCounters()
	counters.RecordBlock(0, true)
	counters.RecordBlock(0, false)
	counters.RecordBlock(1, true)
	counters.RecordBlock(1, false)
	counters.RecordIndex(true)
	counters.RecordIndex(false)

	snap := counters.Snapshot()
	if snap.L0Hits != 1 || snap.L0Misses != 1 {
		t.Fatalf("unexpected L0 counters: %+v", snap)
	}
	if snap.L1Hits != 1 || snap.L1Misses != 1 {
		t.Fatalf("unexpected L1 counters: %+v", snap)
	}
	if snap.IndexHits != 1 || snap.IndexMisses != 1 {
		t.Fatalf("unexpected index counters: %+v", snap)
	}
}

func TestRegionMetrics(t *testing.T) {
	rm := NewRegionMetrics()
	rm.RecordState(1, metaregion.ReplicaStateRunning)
	rm.RecordState(2, metaregion.ReplicaStateRemoving)
	rm.RecordState(1, metaregion.ReplicaStateTombstone)

	snap := rm.Snapshot()
	if snap.Total != 2 {
		t.Fatalf("expected total=2, got %d", snap.Total)
	}
	if snap.Running != 0 || snap.Removing != 1 || snap.Tombstone != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	rm.RecordRemove(1)
	snap = rm.Snapshot()
	if snap.Total != 1 || snap.Tombstone != 0 {
		t.Fatalf("unexpected snapshot after remove: %+v", snap)
	}
}
