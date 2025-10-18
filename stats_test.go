package NoKV

import (
	"expvar"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestStatsCollectSnapshots(t *testing.T) {
	clearDir()
	opt.DetectConflicts = true
	db := Open(opt)
	defer func() { _ = db.Close() }()

	rm := storepkg.NewRegionMetrics()
	db.SetRegionMetrics(rm)
	hooks := rm.Hooks()
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 1, State: manifest.RegionStateRunning})
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 2, State: manifest.RegionStateRemoving})

	if err := db.Update(func(txn *Txn) error {
		return txn.SetEntry(utils.NewEntry([]byte("stats-key"), []byte("stats-value")))
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := db.Get([]byte("stats-key")); err != nil {
		t.Fatalf("get: %v", err)
	}

	snap := db.Info().Snapshot()
	if snap.Entries == 0 {
		t.Fatalf("expected entry count to be populated")
	}
	if len(snap.HotKeys) == 0 {
		t.Fatalf("expected hot key stats to be populated")
	}

	if snap.WALSegmentCount == 0 {
		t.Fatalf("expected wal segment count to be tracked")
	}
	if snap.WALRecordCounts.Entries == 0 {
		t.Fatalf("expected wal record counts to include entry records")
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
	if cfStats, ok := snap.ColumnFamilies[utils.CFDefault.String()]; !ok || cfStats.Writes == 0 || cfStats.Reads == 0 {
		t.Fatalf("expected default column family stats to be populated, snapshot=%+v", snap.ColumnFamilies)
	}

	if snap.FlushPending != db.lsm.FlushPending() {
		t.Fatalf("snapshot flush pending mismatch: %d vs %d", snap.FlushPending, db.lsm.FlushPending())
	}
	if snap.RegionTotal != 2 || snap.RegionRunning != 1 || snap.RegionRemoving != 1 {
		t.Fatalf("expected region metrics populated, snapshot=%+v", snap)
	}

	db.stats.collect()

	if got := expvar.Get("NoKV.Stats.Entries").(*expvar.Int).Value(); got != snap.Entries {
		t.Fatalf("entry count mismatch expvar=%d snapshot=%d", got, snap.Entries)
	}
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
	if got := expvar.Get("NoKV.Stats.Flush.WaitLastMs").(*expvar.Float).Value(); got != snap.FlushLastWaitMs {
		t.Fatalf("flush wait last mismatch expvar=%f snapshot=%f", got, snap.FlushLastWaitMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.WaitMaxMs").(*expvar.Float).Value(); got != snap.FlushMaxWaitMs {
		t.Fatalf("flush wait max mismatch expvar=%f snapshot=%f", got, snap.FlushMaxWaitMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.BuildMs").(*expvar.Float).Value(); got != snap.FlushBuildMs {
		t.Fatalf("flush build mismatch expvar=%f snapshot=%f", got, snap.FlushBuildMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.BuildLastMs").(*expvar.Float).Value(); got != snap.FlushLastBuildMs {
		t.Fatalf("flush build last mismatch expvar=%f snapshot=%f", got, snap.FlushLastBuildMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.BuildMaxMs").(*expvar.Float).Value(); got != snap.FlushMaxBuildMs {
		t.Fatalf("flush build max mismatch expvar=%f snapshot=%f", got, snap.FlushMaxBuildMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.ReleaseMs").(*expvar.Float).Value(); got != snap.FlushReleaseMs {
		t.Fatalf("flush release mismatch expvar=%f snapshot=%f", got, snap.FlushReleaseMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.ReleaseLastMs").(*expvar.Float).Value(); got != snap.FlushLastReleaseMs {
		t.Fatalf("flush release last mismatch expvar=%f snapshot=%f", got, snap.FlushLastReleaseMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.ReleaseMaxMs").(*expvar.Float).Value(); got != snap.FlushMaxReleaseMs {
		t.Fatalf("flush release max mismatch expvar=%f snapshot=%f", got, snap.FlushMaxReleaseMs)
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
	if got := expvar.Get("NoKV.Stats.WAL.ActiveSize").(*expvar.Int).Value(); got != snap.WALActiveSize {
		t.Fatalf("wal active size mismatch expvar=%d snapshot=%d", got, snap.WALActiveSize)
	}
	if v := expvar.Get("NoKV.Stats.WAL.RecordCounts"); v == nil {
		t.Fatalf("expected wal record count map to be exported")
	} else {
		m := v.(*expvar.Map)
		entries := getExpvarInt(t, m, "entries")
		if entries == 0 {
			t.Fatalf("expected wal entry count to be non-zero")
		}
		total := getExpvarInt(t, m, "total")
		if total == 0 || total < entries {
			t.Fatalf("unexpected wal record totals entries=%d total=%d", entries, total)
		}
	}
	if got := expvar.Get("NoKV.Stats.WAL.RaftSegments").(*expvar.Int).Value(); got != int64(snap.WALSegmentsWithRaftRecords) {
		t.Fatalf("wal raft segment count mismatch expvar=%d snapshot=%d", got, snap.WALSegmentsWithRaftRecords)
	}
	if got := expvar.Get("NoKV.Stats.WAL.RaftSegmentsRemovable").(*expvar.Int).Value(); got != int64(snap.WALRemovableRaftSegments) {
		t.Fatalf("wal removable raft segments mismatch expvar=%d snapshot=%d", got, snap.WALRemovableRaftSegments)
	}
	if got := expvar.Get("NoKV.Stats.WAL.Removed").(*expvar.Int).Value(); got != int64(snap.WALSegmentsRemoved) {
		t.Fatalf("wal removed mismatch expvar=%d snapshot=%d", got, snap.WALSegmentsRemoved)
	}
	if got := expvar.Get("NoKV.Stats.Region.Total").(*expvar.Int).Value(); got != snap.RegionTotal {
		t.Fatalf("region total mismatch expvar=%d snapshot=%d", got, snap.RegionTotal)
	}
	if got := expvar.Get("NoKV.Stats.Region.Running").(*expvar.Int).Value(); got != snap.RegionRunning {
		t.Fatalf("region running mismatch expvar=%d snapshot=%d", got, snap.RegionRunning)
	}
	if got := expvar.Get("NoKV.Stats.Region.Removing").(*expvar.Int).Value(); got != snap.RegionRemoving {
		t.Fatalf("region removing mismatch expvar=%d snapshot=%d", got, snap.RegionRemoving)
	}
	if v := expvar.Get("NoKV.Stats.Raft.Groups"); v == nil {
		t.Fatalf("expected raft group metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.RaftGroupCount) {
		t.Fatalf("raft group count mismatch expvar=%d snapshot=%d", got, snap.RaftGroupCount)
	}
	if v := expvar.Get("NoKV.Stats.Raft.LaggingGroups"); v == nil {
		t.Fatalf("expected raft lagging metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.RaftLaggingGroups) {
		t.Fatalf("raft lagging mismatch expvar=%d snapshot=%d", got, snap.RaftLaggingGroups)
	}
	if v := expvar.Get("NoKV.Stats.Raft.MaxLagSegments"); v == nil {
		t.Fatalf("expected raft max lag metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != snap.RaftMaxLagSegments {
		t.Fatalf("raft max lag mismatch expvar=%d snapshot=%d", got, snap.RaftMaxLagSegments)
	}
	if v := expvar.Get("NoKV.Stats.Raft.MinSegment"); v == nil {
		t.Fatalf("expected raft min segment metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.RaftMinLogSegment) {
		t.Fatalf("raft min segment mismatch expvar=%d snapshot=%d", got, snap.RaftMinLogSegment)
	}
	if v := expvar.Get("NoKV.Stats.Raft.MaxSegment"); v == nil {
		t.Fatalf("expected raft max segment metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.RaftMaxLogSegment) {
		t.Fatalf("raft max segment mismatch expvar=%d snapshot=%d", got, snap.RaftMaxLogSegment)
	}
	if snap.RaftLagWarnThreshold != db.opt.RaftLagWarnSegments {
		t.Fatalf("expected raft lag threshold to match options: got=%d want=%d", snap.RaftLagWarnThreshold, db.opt.RaftLagWarnSegments)
	}
	if v := expvar.Get("NoKV.Stats.Raft.LagWarning"); v == nil {
		t.Fatalf("expected raft lag warning metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected raft lag warning expvar to be zero, got %d", got)
	}
	if snap.RaftLagWarning {
		t.Fatalf("expected raft lag warning flag to be false with no groups")
	}
	if got := expvar.Get("NoKV.Stats.Compaction.LastDurationMs").(*expvar.Float).Value(); got != snap.CompactionLastDurationMs {
		t.Fatalf("compaction last duration mismatch expvar=%f snapshot=%f", got, snap.CompactionLastDurationMs)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.MaxDurationMs").(*expvar.Float).Value(); got != snap.CompactionMaxDurationMs {
		t.Fatalf("compaction max duration mismatch expvar=%f snapshot=%f", got, snap.CompactionMaxDurationMs)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.RunsTotal").(*expvar.Int).Value(); got != int64(snap.CompactionRuns) {
		t.Fatalf("compaction runs mismatch expvar=%d snapshot=%d", got, snap.CompactionRuns)
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
	cfVar := expvar.Get("NoKV.Stats.ColumnFamilies")
	if cfVar == nil {
		t.Fatalf("expected column family expvar map to be published")
	}
}

func TestStatsSnapshotTracksThrottleAndWalRemovals(t *testing.T) {
	clearDir()
	opt.DetectConflicts = true
	db := Open(opt)
	defer func() { _ = db.Close() }()

	// Create a WAL record so that rotation and removal have work to do.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(utils.NewEntry([]byte("wal-metrics"), []byte("value")))
	}))

	require.NoError(t, db.wal.Rotate())

	segments, err := db.wal.ListSegments()
	require.NoError(t, err)
	if len(segments) < 1 {
		t.Fatalf("expected at least one WAL segment after rotation")
	}

	var removedID uint32
	_, err = fmt.Sscanf(filepath.Base(segments[0]), "%05d.wal", &removedID)
	require.NoError(t, err)
	require.NoError(t, db.wal.RemoveSegment(removedID))

	db.applyThrottle(true)
	defer db.applyThrottle(false)

	snap := db.Info().Snapshot()
	if !snap.WriteThrottleActive {
		t.Fatalf("expected write throttle to report active")
	}
	if snap.WALSegmentsRemoved == 0 {
		t.Fatalf("expected WAL removal metric to be populated")
	}
	if snap.WALSegmentCount == 0 {
		t.Fatalf("expected WAL segment count to remain positive")
	}

	db.stats.collect()
	if got := expvar.Get("NoKV.Stats.WAL.Removed").(*expvar.Int).Value(); got != int64(snap.WALSegmentsRemoved) {
		t.Fatalf("expected WAL removal expvar to match snapshot: got=%d want=%d", got, snap.WALSegmentsRemoved)
	}
	if got := expvar.Get("NoKV.Stats.Write.Throttle").(*expvar.Int).Value(); got != 1 {
		t.Fatalf("expected write throttle expvar to be 1 while throttled, got %d", got)
	}

	db.applyThrottle(false)
	snapAfter := db.Info().Snapshot()
	if snapAfter.WriteThrottleActive {
		t.Fatalf("expected write throttle to be cleared after release")
	}

	db.stats.collect()
	if got := expvar.Get("NoKV.Stats.Write.Throttle").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected write throttle expvar to reset after release, got %d", got)
	}
}

func getExpvarInt(t *testing.T, m *expvar.Map, key string) int64 {
	t.Helper()
	if m == nil {
		t.Fatalf("expvar map is nil")
	}
	v := m.Get(key)
	if v == nil {
		t.Fatalf("expvar map missing key %s", key)
	}
	iv, ok := v.(*expvar.Int)
	if !ok {
		t.Fatalf("expvar value for %s is not int", key)
	}
	return iv.Value()
}
