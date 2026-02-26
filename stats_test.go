package NoKV

import (
	"encoding/json"
	"expvar"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/stretchr/testify/require"
)

func TestStatsCollectSnapshots(t *testing.T) {
	clearDir()
	opt.DetectConflicts = true
	db := Open(opt)
	defer func() { _ = db.Close() }()

	rm := metrics.NewRegionMetrics()
	db.SetRegionMetrics(rm)
	hooks := rm.Hooks()
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 1, State: manifest.RegionStateRunning})
	hooks.OnRegionUpdate(manifest.RegionMeta{ID: 2, State: manifest.RegionStateRemoving})

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(kv.NewEntry([]byte("stats-key"), []byte("stats-value")))
	}))
	entry, err := db.Get([]byte("stats-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("stats-value"), entry.Value)

	snap := db.Info().Snapshot()
	require.Greater(t, snap.Entries, int64(0))
	require.NotEmpty(t, snap.Hot.ReadKeys)
	require.Greater(t, snap.WAL.SegmentCount, int64(0))
	require.Greater(t, snap.WAL.RecordCounts.Entries, uint64(0))
	require.False(t, snap.WAL.TypedRecordWarning)
	require.Equal(t, uint64(0), snap.WAL.AutoGCRuns)
	require.Equal(t, uint64(0), snap.WAL.AutoGCRemoved)
	require.Greater(t, snap.Write.BatchesTotal, int64(0))
	require.False(t, snap.Write.ThrottleActive)
	require.Equal(t, db.iterPool.reused(), snap.Cache.IteratorReused)

	cfStats, ok := snap.LSM.ColumnFamilies[kv.CFDefault.String()]
	require.True(t, ok)
	require.Greater(t, cfStats.Writes, uint64(0))
	require.Greater(t, cfStats.Reads, uint64(0))

	require.Equal(t, db.lsm.FlushPending(), snap.Flush.Pending)
	require.Equal(t, int64(2), snap.Region.Total)
	require.Equal(t, int64(1), snap.Region.Running)
	require.Equal(t, int64(1), snap.Region.Removing)

	db.stats.collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.Entries, exported.Entries)
	require.Equal(t, snap.Flush.Pending, exported.Flush.Pending)
	require.Equal(t, snap.Compaction.Backlog, exported.Compaction.Backlog)
	require.Equal(t, snap.Write.BatchesTotal, exported.Write.BatchesTotal)
	require.Equal(t, snap.WAL.ActiveSegment, exported.WAL.ActiveSegment)
	require.Equal(t, snap.WAL.SegmentsRemoved, exported.WAL.SegmentsRemoved)
	require.Equal(t, snap.Region.Total, exported.Region.Total)
	require.Equal(t, snap.Txn.Started, exported.Txn.Started)

	// Legacy scalar keys are intentionally removed.
	require.Nil(t, expvar.Get("NoKV.Stats.Flush.Pending"))
	require.Nil(t, expvar.Get("NoKV.Stats.WAL.ActiveSegment"))
	require.Nil(t, expvar.Get("NoKV.Txns.Active"))
	require.Nil(t, expvar.Get("NoKV.Redis"))
}

func TestStatsSnapshotTracksThrottleAndWalRemovals(t *testing.T) {
	clearDir()
	opt.DetectConflicts = true
	db := Open(opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(kv.NewEntry([]byte("wal-metrics"), []byte("value")))
	}))
	require.NoError(t, db.wal.Rotate())

	segments, err := db.wal.ListSegments()
	require.NoError(t, err)
	require.NotEmpty(t, segments)

	var removedID uint32
	_, err = fmt.Sscanf(filepath.Base(segments[0]), "%05d.wal", &removedID)
	require.NoError(t, err)
	require.NoError(t, db.wal.RemoveSegment(removedID))

	db.applyThrottle(true)
	defer db.applyThrottle(false)

	snap := db.Info().Snapshot()
	require.True(t, snap.Write.ThrottleActive)
	require.Greater(t, snap.WAL.SegmentsRemoved, uint64(0))
	require.Greater(t, snap.WAL.SegmentCount, int64(0))

	db.stats.collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.WAL.SegmentsRemoved, exported.WAL.SegmentsRemoved)
	require.True(t, exported.Write.ThrottleActive)

	db.applyThrottle(false)
	snapAfter := db.Info().Snapshot()
	require.False(t, snapAfter.Write.ThrottleActive)

	db.stats.collect()
	exportedAfter := loadExpvarStatsSnapshot(t)
	require.False(t, exportedAfter.Write.ThrottleActive)

	// Legacy scalar key should remain absent.
	require.Nil(t, expvar.Get("NoKV.Stats.Write.Throttle"))
}

func loadExpvarStatsSnapshot(t *testing.T) StatsSnapshot {
	t.Helper()

	v := expvar.Get("NoKV.Stats")
	require.NotNil(t, v)

	var snap StatsSnapshot
	require.NoError(t, json.Unmarshal([]byte(v.String()), &snap))
	return snap
}
