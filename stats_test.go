package NoKV

import (
	"expvar"
	"testing"

	"github.com/feichai0017/NoKV/utils"
)

func TestStatsCollectSnapshots(t *testing.T) {
	clearDir()
	opt.DetectConflicts = true
	db := Open(opt)
	defer func() { _ = db.Close() }()

	if err := db.Update(func(txn *Txn) error {
		return txn.SetEntry(utils.NewEntry([]byte("stats-key"), []byte("stats-value")))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := db.Get([]byte("stats-key")); err != nil {
		t.Fatalf("get: %v", err)
	}

	snap := db.Info().Snapshot()
	if len(snap.HotKeys) == 0 {
		t.Fatalf("expected hot key stats to be populated")
	}
	
	if snap.WALSegmentCount == 0 {
			t.Fatalf("expected wal segment count to be tracked")
		}
		if snap.WriteBatchesTotal == 0 {
			t.Fatalf("expected write batch metrics to be recorded, snapshot=%+v", snap)
		}
		if snap.WriteThrottleActive {
			t.Fatalf("expected throttle to be disabled in snapshot")
		}
		if snap.IteratorReused != db.iterPool.reused() {
			t.Fatalf("expected iterator reuse snapshot to match pool, snap=%d pool=%d", snap.IteratorReused, db.iterPool.reused())
		}

	if snap.FlushPending != db.lsm.FlushPending() {
		t.Fatalf("snapshot flush pending mismatch: %d vs %d", snap.FlushPending, db.lsm.FlushPending())
	}

	db.stats.collect()

	if got := expvar.Get("NoKV.Stats.Flush.Pending").(*expvar.Int).Value(); got != snap.FlushPending {
		t.Fatalf("flush pending mismatch expvar=%d snapshot=%d", got, snap.FlushPending)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.Backlog").(*expvar.Int).Value(); got != snap.CompactionBacklog {
		t.Fatalf("compaction backlog mismatch expvar=%d snapshot=%d", got, snap.CompactionBacklog)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.MaxScore").(*expvar.Float).Value(); got != snap.CompactionMaxScore {
		t.Fatalf("compaction max score mismatch expvar=%f snapshot=%f", got, snap.CompactionMaxScore)
	}

	vstats := db.vlog.metrics()
	if got := expvar.Get("NoKV.Stats.ValueLog.Segments").(*expvar.Int).Value(); got != int64(vstats.Segments) {
		t.Fatalf("value log segments mismatch expvar=%d metrics=%d", got, vstats.Segments)
	}
	if got := expvar.Get("NoKV.Stats.ValueLog.PendingDeletes").(*expvar.Int).Value(); got != int64(vstats.PendingDeletes) {
		t.Fatalf("value log pending deletes mismatch expvar=%d metrics=%d", got, vstats.PendingDeletes)
	}
	if got := expvar.Get("NoKV.Stats.ValueLog.DiscardQueue").(*expvar.Int).Value(); got != int64(vstats.DiscardQueue) {
		t.Fatalf("value log discard queue mismatch expvar=%d metrics=%d", got, vstats.DiscardQueue)
	}
	if got := expvar.Get("NoKV.Stats.Flush.QueueLength").(*expvar.Int).Value(); got != snap.FlushQueueLength {
		t.Fatalf("flush queue length mismatch expvar=%d snapshot=%d", got, snap.FlushQueueLength)
	}
	if got := expvar.Get("NoKV.Stats.Flush.Active").(*expvar.Int).Value(); got != snap.FlushActive {
		t.Fatalf("flush active mismatch expvar=%d snapshot=%d", got, snap.FlushActive)
	}
	if got := expvar.Get("NoKV.Stats.Flush.Completed").(*expvar.Int).Value(); got != snap.FlushCompleted {
		t.Fatalf("flush completed mismatch expvar=%d snapshot=%d", got, snap.FlushCompleted)
	}
	if got := expvar.Get("NoKV.Stats.Flush.WaitMs").(*expvar.Float).Value(); got != snap.FlushWaitMs {
		t.Fatalf("flush wait mismatch expvar=%f snapshot=%f", got, snap.FlushWaitMs)
	}
	if got := expvar.Get("NoKV.Stats.Write.QueueDepth").(*expvar.Int).Value(); got != snap.WriteQueueDepth {
		t.Fatalf("write queue depth mismatch expvar=%d snapshot=%d", got, snap.WriteQueueDepth)
	}
	if got := expvar.Get("NoKV.Stats.Write.QueueEntries").(*expvar.Int).Value(); got != snap.WriteQueueEntries {
		t.Fatalf("write queue entries mismatch expvar=%d snapshot=%d", got, snap.WriteQueueEntries)
	}
	if got := expvar.Get("NoKV.Stats.Write.QueueBytes").(*expvar.Int).Value(); got != snap.WriteQueueBytes {
		t.Fatalf("write queue bytes mismatch expvar=%d snapshot=%d", got, snap.WriteQueueBytes)
	}
	if got := expvar.Get("NoKV.Stats.Write.BatchAvgEntries").(*expvar.Float).Value(); got != snap.WriteAvgBatchEntries {
		t.Fatalf("write batch avg entries mismatch expvar=%f snapshot=%f", got, snap.WriteAvgBatchEntries)
	}
	if got := expvar.Get("NoKV.Stats.Write.BatchAvgBytes").(*expvar.Float).Value(); got != snap.WriteAvgBatchBytes {
		t.Fatalf("write batch avg bytes mismatch expvar=%f snapshot=%f", got, snap.WriteAvgBatchBytes)
	}
	if got := expvar.Get("NoKV.Stats.Write.Batches").(*expvar.Int).Value(); got != snap.WriteBatchesTotal {
		t.Fatalf("write batches mismatch expvar=%d snapshot=%d", got, snap.WriteBatchesTotal)
	}
	if got := expvar.Get("NoKV.Stats.Write.Throttle").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected write throttle to be zero, got %d", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.Segments").(*expvar.Int).Value(); got != snap.WALSegmentCount {
		t.Fatalf("wal segment count mismatch expvar=%d snapshot=%d", got, snap.WALSegmentCount)
	}
	if got := expvar.Get("NoKV.Stats.WAL.ActiveSegment").(*expvar.Int).Value(); got != snap.WALActiveSegment {
		t.Fatalf("wal active segment mismatch expvar=%d snapshot=%d", got, snap.WALActiveSegment)
	}
	if got := expvar.Get("NoKV.Stats.WAL.Removed").(*expvar.Int).Value(); got != int64(snap.WALSegmentsRemoved) {
		t.Fatalf("wal removed mismatch expvar=%d snapshot=%d", got, snap.WALSegmentsRemoved)
	}
	if v := expvar.Get("NoKV.Stats.Cache.L0HitRate"); v == nil {
		t.Fatalf("expected L0 hit rate metric to be exported")
	}
	if v := expvar.Get("NoKV.Stats.Cache.L1HitRate"); v == nil {
		t.Fatalf("expected L1 hit rate metric to be exported")
	}
	if v := expvar.Get("NoKV.Stats.Cache.BloomHitRate"); v == nil {
		t.Fatalf("expected bloom hit rate metric to be exported")
	}
	if v := expvar.Get("NoKV.Stats.Iterator.Reused"); v == nil {
		t.Fatalf("expected iterator reuse metric to be exported")
	}
	if snap.TxnsActive != 0 {
		t.Fatalf("expected zero active txns, got %d", snap.TxnsActive)
	}
	if snap.TxnsCommitted == 0 || snap.TxnsStarted == 0 {
		t.Fatalf("expected txn counters to be populated, snapshot=%+v", snap)
	}
	if got := expvar.Get("NoKV.Txns.Active").(*expvar.Int).Value(); got != snap.TxnsActive {
		t.Fatalf("txn active mismatch expvar=%d snapshot=%d", got, snap.TxnsActive)
	}
	if got := expvar.Get("NoKV.Txns.Started").(*expvar.Int).Value(); got != int64(snap.TxnsStarted) {
		t.Fatalf("txn started mismatch expvar=%d snapshot=%d", got, snap.TxnsStarted)
	}
	if got := expvar.Get("NoKV.Txns.Committed").(*expvar.Int).Value(); got != int64(snap.TxnsCommitted) {
		t.Fatalf("txn committed mismatch expvar=%d snapshot=%d", got, snap.TxnsCommitted)
	}
}
