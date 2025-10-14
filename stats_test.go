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

	snap := db.Info().Snapshot()

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
