package NoKV

import (
	"expvar"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
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
		return txn.SetEntry(kv.NewEntry([]byte("stats-key"), []byte("stats-value")))
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
	if len(snap.Hot.ReadKeys) == 0 {
		t.Fatalf("expected hot key stats to be populated")
	}

	if snap.WAL.SegmentCount == 0 {
		t.Fatalf("expected wal segment count to be tracked")
	}
	if snap.WAL.RecordCounts.Entries == 0 {
		t.Fatalf("expected wal record counts to include entry records")
	}
	if snap.WAL.TypedRecordRatio < 0 {
		t.Fatalf("expected typed record ratio to be non-negative")
	}
	if snap.WAL.TypedRecordWarning {
		t.Fatalf("expected typed record warning to be false in baseline snapshot")
	}
	if snap.WAL.AutoGCRuns != 0 || snap.WAL.AutoGCRemoved != 0 {
		t.Fatalf("expected wal auto gc counters to be zero, snapshot=%+v", snap)
	}
	if snap.Write.BatchesTotal == 0 {
		t.Fatalf("expected write batch metrics to be recorded, snapshot=%+v", snap)
	}
	if snap.Write.ThrottleActive {
		t.Fatalf("expected throttle to be disabled in snapshot")
	}
	if snap.Cache.IteratorReused != db.iterPool.reused() {
		t.Fatalf("expected iterator reuse snapshot to match pool, snap=%d pool=%d", snap.Cache.IteratorReused, db.iterPool.reused())
	}
	if cfStats, ok := snap.LSM.ColumnFamilies[kv.CFDefault.String()]; !ok || cfStats.Writes == 0 || cfStats.Reads == 0 {
		t.Fatalf("expected default column family stats to be populated, snapshot=%+v", snap.LSM.ColumnFamilies)
	}

	if snap.Flush.Pending != db.lsm.FlushPending() {
		t.Fatalf("snapshot flush pending mismatch: %d vs %d", snap.Flush.Pending, db.lsm.FlushPending())
	}
	if snap.Region.Total != 2 || snap.Region.Running != 1 || snap.Region.Removing != 1 {
		t.Fatalf("expected region metrics populated, snapshot=%+v", snap)
	}

	db.stats.collect()

	if got := expvar.Get("NoKV.Stats.Entries").(*expvar.Int).Value(); got != snap.Entries {
		t.Fatalf("entry count mismatch expvar=%d snapshot=%d", got, snap.Entries)
	}
	if got := expvar.Get("NoKV.Stats.Flush.Pending").(*expvar.Int).Value(); got != snap.Flush.Pending {
		t.Fatalf("flush pending mismatch expvar=%d snapshot=%d", got, snap.Flush.Pending)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.Backlog").(*expvar.Int).Value(); got != snap.Compaction.Backlog {
		t.Fatalf("compaction backlog mismatch expvar=%d snapshot=%d", got, snap.Compaction.Backlog)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.MaxScore").(*expvar.Float).Value(); got != snap.Compaction.MaxScore {
		t.Fatalf("compaction max score mismatch expvar=%f snapshot=%f", got, snap.Compaction.MaxScore)
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
	if got := expvar.Get("NoKV.Stats.Flush.QueueLength").(*expvar.Int).Value(); got != snap.Flush.QueueLength {
		t.Fatalf("flush queue length mismatch expvar=%d snapshot=%d", got, snap.Flush.QueueLength)
	}
	if got := expvar.Get("NoKV.Stats.Flush.Active").(*expvar.Int).Value(); got != snap.Flush.Active {
		t.Fatalf("flush active mismatch expvar=%d snapshot=%d", got, snap.Flush.Active)
	}
	if got := expvar.Get("NoKV.Stats.Flush.Completed").(*expvar.Int).Value(); got != snap.Flush.Completed {
		t.Fatalf("flush completed mismatch expvar=%d snapshot=%d", got, snap.Flush.Completed)
	}
	if got := expvar.Get("NoKV.Stats.Flush.WaitMs").(*expvar.Float).Value(); got != snap.Flush.WaitMs {
		t.Fatalf("flush wait mismatch expvar=%f snapshot=%f", got, snap.Flush.WaitMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.WaitLastMs").(*expvar.Float).Value(); got != snap.Flush.LastWaitMs {
		t.Fatalf("flush wait last mismatch expvar=%f snapshot=%f", got, snap.Flush.LastWaitMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.WaitMaxMs").(*expvar.Float).Value(); got != snap.Flush.MaxWaitMs {
		t.Fatalf("flush wait max mismatch expvar=%f snapshot=%f", got, snap.Flush.MaxWaitMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.BuildMs").(*expvar.Float).Value(); got != snap.Flush.BuildMs {
		t.Fatalf("flush build mismatch expvar=%f snapshot=%f", got, snap.Flush.BuildMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.BuildLastMs").(*expvar.Float).Value(); got != snap.Flush.LastBuildMs {
		t.Fatalf("flush build last mismatch expvar=%f snapshot=%f", got, snap.Flush.LastBuildMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.BuildMaxMs").(*expvar.Float).Value(); got != snap.Flush.MaxBuildMs {
		t.Fatalf("flush build max mismatch expvar=%f snapshot=%f", got, snap.Flush.MaxBuildMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.ReleaseMs").(*expvar.Float).Value(); got != snap.Flush.ReleaseMs {
		t.Fatalf("flush release mismatch expvar=%f snapshot=%f", got, snap.Flush.ReleaseMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.ReleaseLastMs").(*expvar.Float).Value(); got != snap.Flush.LastReleaseMs {
		t.Fatalf("flush release last mismatch expvar=%f snapshot=%f", got, snap.Flush.LastReleaseMs)
	}
	if got := expvar.Get("NoKV.Stats.Flush.ReleaseMaxMs").(*expvar.Float).Value(); got != snap.Flush.MaxReleaseMs {
		t.Fatalf("flush release max mismatch expvar=%f snapshot=%f", got, snap.Flush.MaxReleaseMs)
	}
	if got := expvar.Get("NoKV.Stats.Write.QueueDepth").(*expvar.Int).Value(); got != snap.Write.QueueDepth {
		t.Fatalf("write queue depth mismatch expvar=%d snapshot=%d", got, snap.Write.QueueDepth)
	}
	if got := expvar.Get("NoKV.Stats.Write.QueueEntries").(*expvar.Int).Value(); got != snap.Write.QueueEntries {
		t.Fatalf("write queue entries mismatch expvar=%d snapshot=%d", got, snap.Write.QueueEntries)
	}
	if got := expvar.Get("NoKV.Stats.Write.QueueBytes").(*expvar.Int).Value(); got != snap.Write.QueueBytes {
		t.Fatalf("write queue bytes mismatch expvar=%d snapshot=%d", got, snap.Write.QueueBytes)
	}
	if got := expvar.Get("NoKV.Stats.Write.BatchAvgEntries").(*expvar.Float).Value(); got != snap.Write.AvgBatchEntries {
		t.Fatalf("write batch avg entries mismatch expvar=%f snapshot=%f", got, snap.Write.AvgBatchEntries)
	}
	if got := expvar.Get("NoKV.Stats.Write.BatchAvgBytes").(*expvar.Float).Value(); got != snap.Write.AvgBatchBytes {
		t.Fatalf("write batch avg bytes mismatch expvar=%f snapshot=%f", got, snap.Write.AvgBatchBytes)
	}
	if got := expvar.Get("NoKV.Stats.Write.Batches").(*expvar.Int).Value(); got != snap.Write.BatchesTotal {
		t.Fatalf("write batches mismatch expvar=%d snapshot=%d", got, snap.Write.BatchesTotal)
	}
	if got := expvar.Get("NoKV.Stats.Write.Throttle").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected write throttle to be zero, got %d", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.Segments").(*expvar.Int).Value(); got != snap.WAL.SegmentCount {
		t.Fatalf("wal segment count mismatch expvar=%d snapshot=%d", got, snap.WAL.SegmentCount)
	}
	if got := expvar.Get("NoKV.Stats.WAL.ActiveSegment").(*expvar.Int).Value(); got != snap.WAL.ActiveSegment {
		t.Fatalf("wal active segment mismatch expvar=%d snapshot=%d", got, snap.WAL.ActiveSegment)
	}
	if got := expvar.Get("NoKV.Stats.WAL.ActiveSize").(*expvar.Int).Value(); got != snap.WAL.ActiveSize {
		t.Fatalf("wal active size mismatch expvar=%d snapshot=%d", got, snap.WAL.ActiveSize)
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
	if got := expvar.Get("NoKV.Stats.WAL.RaftSegments").(*expvar.Int).Value(); got != int64(snap.WAL.SegmentsWithRaftRecords) {
		t.Fatalf("wal raft segment count mismatch expvar=%d snapshot=%d", got, snap.WAL.SegmentsWithRaftRecords)
	}
	if got := expvar.Get("NoKV.Stats.WAL.RaftSegmentsRemovable").(*expvar.Int).Value(); got != int64(snap.WAL.RemovableRaftSegments) {
		t.Fatalf("wal removable raft segments mismatch expvar=%d snapshot=%d", got, snap.WAL.RemovableRaftSegments)
	}
	if got := expvar.Get("NoKV.Stats.WAL.TypedRatio").(*expvar.Float).Value(); got != snap.WAL.TypedRecordRatio {
		t.Fatalf("wal typed ratio mismatch expvar=%f snapshot=%f", got, snap.WAL.TypedRecordRatio)
	}
	if got := expvar.Get("NoKV.Stats.WAL.TypedWarning").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected typed warning expvar to be zero, got %d", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.TypedReason").(*expvar.String).String(); got != `""` {
		t.Fatalf("expected typed warning reason to be empty, got %s", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.AutoRuns").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected wal auto gc runs to be zero, got %d", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.AutoRemoved").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected wal auto gc removed to be zero, got %d", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.AutoLastUnix").(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected wal auto gc last unix to be zero, got %d", got)
	}
	if got := expvar.Get("NoKV.Stats.WAL.Removed").(*expvar.Int).Value(); got != int64(snap.WAL.SegmentsRemoved) {
		t.Fatalf("wal removed mismatch expvar=%d snapshot=%d", got, snap.WAL.SegmentsRemoved)
	}
	if got := expvar.Get("NoKV.Stats.Region.Total").(*expvar.Int).Value(); got != snap.Region.Total {
		t.Fatalf("region total mismatch expvar=%d snapshot=%d", got, snap.Region.Total)
	}
	if got := expvar.Get("NoKV.Stats.Region.Running").(*expvar.Int).Value(); got != snap.Region.Running {
		t.Fatalf("region running mismatch expvar=%d snapshot=%d", got, snap.Region.Running)
	}
	if got := expvar.Get("NoKV.Stats.Region.Removing").(*expvar.Int).Value(); got != snap.Region.Removing {
		t.Fatalf("region removing mismatch expvar=%d snapshot=%d", got, snap.Region.Removing)
	}
	if v := expvar.Get("NoKV.Stats.Raft.Groups"); v == nil {
		t.Fatalf("expected raft group metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.Raft.GroupCount) {
		t.Fatalf("raft group count mismatch expvar=%d snapshot=%d", got, snap.Raft.GroupCount)
	}
	if v := expvar.Get("NoKV.Stats.Raft.LaggingGroups"); v == nil {
		t.Fatalf("expected raft lagging metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.Raft.LaggingGroups) {
		t.Fatalf("raft lagging mismatch expvar=%d snapshot=%d", got, snap.Raft.LaggingGroups)
	}
	if v := expvar.Get("NoKV.Stats.Raft.MaxLagSegments"); v == nil {
		t.Fatalf("expected raft max lag metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != snap.Raft.MaxLagSegments {
		t.Fatalf("raft max lag mismatch expvar=%d snapshot=%d", got, snap.Raft.MaxLagSegments)
	}
	if v := expvar.Get("NoKV.Stats.Raft.MinSegment"); v == nil {
		t.Fatalf("expected raft min segment metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.Raft.MinLogSegment) {
		t.Fatalf("raft min segment mismatch expvar=%d snapshot=%d", got, snap.Raft.MinLogSegment)
	}
	if v := expvar.Get("NoKV.Stats.Raft.MaxSegment"); v == nil {
		t.Fatalf("expected raft max segment metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != int64(snap.Raft.MaxLogSegment) {
		t.Fatalf("raft max segment mismatch expvar=%d snapshot=%d", got, snap.Raft.MaxLogSegment)
	}
	if snap.Raft.LagWarnThreshold != db.opt.RaftLagWarnSegments {
		t.Fatalf("expected raft lag threshold to match options: got=%d want=%d", snap.Raft.LagWarnThreshold, db.opt.RaftLagWarnSegments)
	}
	if v := expvar.Get("NoKV.Stats.Raft.LagWarning"); v == nil {
		t.Fatalf("expected raft lag warning metric to be exported")
	} else if got := v.(*expvar.Int).Value(); got != 0 {
		t.Fatalf("expected raft lag warning expvar to be zero, got %d", got)
	}
	if snap.Raft.LagWarning {
		t.Fatalf("expected raft lag warning flag to be false with no groups")
	}
	if got := expvar.Get("NoKV.Stats.Compaction.LastDurationMs").(*expvar.Float).Value(); got != snap.Compaction.LastDurationMs {
		t.Fatalf("compaction last duration mismatch expvar=%f snapshot=%f", got, snap.Compaction.LastDurationMs)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.MaxDurationMs").(*expvar.Float).Value(); got != snap.Compaction.MaxDurationMs {
		t.Fatalf("compaction max duration mismatch expvar=%f snapshot=%f", got, snap.Compaction.MaxDurationMs)
	}
	if got := expvar.Get("NoKV.Stats.Compaction.RunsTotal").(*expvar.Int).Value(); got != int64(snap.Compaction.Runs) {
		t.Fatalf("compaction runs mismatch expvar=%d snapshot=%d", got, snap.Compaction.Runs)
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
	if snap.Txn.Active != 0 {
		t.Fatalf("expected zero active txns, got %d", snap.Txn.Active)
	}
	if snap.Txn.Committed == 0 || snap.Txn.Started == 0 {
		t.Fatalf("expected txn counters to be populated, snapshot=%+v", snap)
	}
	if got := expvar.Get("NoKV.Txns.Active").(*expvar.Int).Value(); got != snap.Txn.Active {
		t.Fatalf("txn active mismatch expvar=%d snapshot=%d", got, snap.Txn.Active)
	}
	if got := expvar.Get("NoKV.Txns.Started").(*expvar.Int).Value(); got != int64(snap.Txn.Started) {
		t.Fatalf("txn started mismatch expvar=%d snapshot=%d", got, snap.Txn.Started)
	}
	if got := expvar.Get("NoKV.Txns.Committed").(*expvar.Int).Value(); got != int64(snap.Txn.Committed) {
		t.Fatalf("txn committed mismatch expvar=%d snapshot=%d", got, snap.Txn.Committed)
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
		return txn.SetEntry(kv.NewEntry([]byte("wal-metrics"), []byte("value")))
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
	if !snap.Write.ThrottleActive {
		t.Fatalf("expected write throttle to report active")
	}
	if snap.WAL.SegmentsRemoved == 0 {
		t.Fatalf("expected WAL removal metric to be populated")
	}
	if snap.WAL.SegmentCount == 0 {
		t.Fatalf("expected WAL segment count to remain positive")
	}

	db.stats.collect()
	if got := expvar.Get("NoKV.Stats.WAL.Removed").(*expvar.Int).Value(); got != int64(snap.WAL.SegmentsRemoved) {
		t.Fatalf("expected WAL removal expvar to match snapshot: got=%d want=%d", got, snap.WAL.SegmentsRemoved)
	}
	if got := expvar.Get("NoKV.Stats.Write.Throttle").(*expvar.Int).Value(); got != 1 {
		t.Fatalf("expected write throttle expvar to be 1 while throttled, got %d", got)
	}

	db.applyThrottle(false)
	snapAfter := db.Info().Snapshot()
	if snapAfter.Write.ThrottleActive {
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
