package NoKV

import (
	"expvar"
	"math"
	"testing"

	"github.com/feichai0017/NoKV/utils"
)

func TestStatsCollectSnapshots(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()

	entry := utils.NewEntry([]byte("stats-key"), []byte("stats-value"))
	entry.Key = utils.KeyWithTs(entry.Key, math.MaxUint32)

	if err := db.batchSet([]*utils.Entry{entry}); err != nil {
		t.Fatalf("batchSet: %v", err)
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
}
