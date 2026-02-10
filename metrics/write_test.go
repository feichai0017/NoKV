package metrics

import (
	"testing"
	"time"

	"github.com/feichai0017/NoKV/manifest"
)

func TestWriteMetricsSnapshot(t *testing.T) {
	m := NewWriteMetrics()
	m.UpdateQueue(5, 10, 1024)
	m.RecordBatch(2, 20, 2048, int64(3*time.Millisecond))
	m.RecordValueLog(5 * time.Millisecond)
	m.RecordApply(7 * time.Millisecond)

	snap := m.Snapshot()
	if snap.QueueLen != 5 || snap.QueueEntries != 10 || snap.QueueBytes != 1024 {
		t.Fatalf("queue snapshot mismatch: %+v", snap)
	}
	if snap.Batches != 2 || snap.AvgBatchEntries <= 0 || snap.AvgBatchBytes <= 0 {
		t.Fatalf("batch snapshot mismatch: %+v", snap)
	}
	if snap.AvgRequestWaitMs <= 0 || snap.AvgValueLogMs <= 0 || snap.AvgApplyMs <= 0 {
		t.Fatalf("timing averages missing: %+v", snap)
	}
}

func TestCacheCountersSnapshot(t *testing.T) {
	counters := NewCacheCounters()
	counters.RecordBlock(0, true)
	counters.RecordBlock(0, false)
	counters.RecordBlock(1, true)
	counters.RecordBlock(1, false)
	counters.RecordBloom(true)
	counters.RecordBloom(false)
	counters.RecordIndex(true)
	counters.RecordIndex(false)

	snap := counters.Snapshot()
	if snap.L0Hits != 1 || snap.L0Misses != 1 {
		t.Fatalf("unexpected L0 counters: %+v", snap)
	}
	if snap.L1Hits != 1 || snap.L1Misses != 1 {
		t.Fatalf("unexpected L1 counters: %+v", snap)
	}
	if snap.BloomHits != 1 || snap.BloomMisses != 1 {
		t.Fatalf("unexpected bloom counters: %+v", snap)
	}
	if snap.IndexHits != 1 || snap.IndexMisses != 1 {
		t.Fatalf("unexpected index counters: %+v", snap)
	}
}

func TestRedisMetricsCounters(t *testing.T) {
	rm := NewRedisMetrics([]string{"get"})
	rm.IncCommand("get")
	rm.IncCommand("set")
	rm.IncError()
	rm.ConnOpened()
	rm.ConnClosed()

	snap := rm.Snapshot()
	if snap.CommandsTotal != 2 {
		t.Fatalf("expected commands_total=2, got %d", snap.CommandsTotal)
	}
	if snap.ErrorsTotal != 1 {
		t.Fatalf("expected errors_total=1, got %d", snap.ErrorsTotal)
	}
	if snap.ConnectionsAccepted != 1 {
		t.Fatalf("expected connections_accepted=1, got %d", snap.ConnectionsAccepted)
	}
	if snap.ConnectionsActive != 0 {
		t.Fatalf("expected connections_active=0, got %d", snap.ConnectionsActive)
	}
	cmds := snap.CommandsPerOperation
	if cmds["GET"] != 1 || cmds["SET"] != 1 {
		t.Fatalf("unexpected command counts: %+v", cmds)
	}
}

func TestRegionMetricsHooks(t *testing.T) {
	rm := NewRegionMetrics()
	hooks := rm.Hooks()
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 1, State: manifest.RegionStateRunning})
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 2, State: manifest.RegionStateRemoving})
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 1, State: manifest.RegionStateTombstone})

	snap := rm.Snapshot()
	if snap.Total != 2 {
		t.Fatalf("expected total=2, got %d", snap.Total)
	}
	if snap.Running != 0 || snap.Removing != 1 || snap.Tombstone != 1 {
		t.Fatalf("unexpected snapshot: %+v", snap)
	}

	hooks.OnRegionRemove(1)
	snap = rm.Snapshot()
	if snap.Total != 1 || snap.Tombstone != 0 {
		t.Fatalf("unexpected snapshot after remove: %+v", snap)
	}
}

func TestValueLogCounters(t *testing.T) {
	ResetValueLogGCMetricsForTesting()
	before := ValueLogGCMetricsSnapshot()

	IncValueLogGCRuns()
	IncValueLogSegmentsRemoved()
	IncValueLogHeadUpdates()
	IncValueLogGCScheduled()
	IncValueLogGCThrottled()
	IncValueLogGCSkipped()
	IncValueLogGCRejected()
	IncValueLogGCActive()
	DecValueLogGCActive()
	SetValueLogGCParallelism(3)

	after := ValueLogGCMetricsSnapshot()
	if after.GCRuns != before.GCRuns+1 {
		t.Fatalf("expected gc runs to increment")
	}
	if after.SegmentsRemoved != before.SegmentsRemoved+1 {
		t.Fatalf("expected segments removed to increment")
	}
	if after.HeadUpdates != before.HeadUpdates+1 {
		t.Fatalf("expected head updates to increment")
	}
	if after.GCScheduled != 1 || after.GCThrottled != 1 || after.GCSkipped != 1 || after.GCRejected != 1 {
		t.Fatalf("unexpected gc counters: %+v", after)
	}
	if after.GCActive != 0 {
		t.Fatalf("expected gc active to return to zero, got %d", after.GCActive)
	}
	if after.GCParallelism != 3 {
		t.Fatalf("expected gc parallelism=3, got %d", after.GCParallelism)
	}
}
