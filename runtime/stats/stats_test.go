package stats_test

// External-test integration tests for the Stats subsystem, exercised
// through the root NoKV.DB facade. Helpers are intentionally inlined:
// shared db_test.go fixtures live in the root package and an external
// test package can't see unexported identifiers.

import (
	"encoding/json"
	"expvar"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/metrics"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/percolator/mvcc"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/feichai0017/NoKV/runtime/stats"
	"github.com/stretchr/testify/require"
)

func openTestDB(t testing.TB, opt *NoKV.Options) *NoKV.DB {
	t.Helper()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}

func newTestOptions(t *testing.T) *NoKV.Options {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.SSTableMaxSz = 1 << 12
	opt.MemTableSize = 1 << 12
	opt.MaxBatchCount = 10
	opt.MaxBatchSize = 1 << 20
	opt.DetectConflicts = true
	return opt
}

func TestStatsCollectSnapshots(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	rm := metrics.NewRegionMetrics()
	db.SetRegionMetrics(rm)
	rm.RecordUpdate(localmeta.RegionMeta{ID: 1, State: localmeta.RegionStateRunning})
	rm.RecordUpdate(localmeta.RegionMeta{ID: 2, State: localmeta.RegionStateRemoving})

	require.NoError(t, db.Set([]byte("stats-key"), []byte("stats-value")))
	entry, err := db.Get([]byte("stats-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("stats-value"), entry.Value)

	snap := db.Info().Snapshot()
	require.Greater(t, snap.Entries, int64(0))
	require.Empty(t, snap.Hot.WriteKeys)
	require.Greater(t, snap.WAL.SegmentCount, int64(0))
	require.Greater(t, snap.WAL.RecordCounts.Entries, uint64(0))
	require.False(t, snap.WAL.TypedRecordWarning)
	require.Equal(t, uint64(0), snap.WAL.AutoGCRuns)
	require.Equal(t, uint64(0), snap.WAL.AutoGCRemoved)
	require.Greater(t, snap.Write.BatchesTotal, int64(0))
	require.False(t, snap.Write.ThrottleActive)
	require.Equal(t, db.IteratorReused(), snap.Cache.IteratorReused)

	require.Equal(t, int64(2), snap.Region.Total)
	require.Equal(t, int64(1), snap.Region.Running)
	require.Equal(t, int64(1), snap.Region.Removing)

	db.Info().Collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.Entries, exported.Entries)
	require.Equal(t, snap.Flush.Pending, exported.Flush.Pending)
	require.Equal(t, snap.Compaction.Backlog, exported.Compaction.Backlog)
	require.Equal(t, snap.Write.BatchesTotal, exported.Write.BatchesTotal)
	require.Equal(t, snap.WAL.ActiveSegment, exported.WAL.ActiveSegment)
	require.Equal(t, snap.WAL.SegmentsRemoved, exported.WAL.SegmentsRemoved)
	require.Equal(t, snap.Region.Total, exported.Region.Total)

	// Legacy scalar keys are intentionally removed.
	require.Nil(t, expvar.Get("NoKV.Stats.Flush.Pending"))
	require.Nil(t, expvar.Get("NoKV.Stats.WAL.ActiveSegment"))
	require.Nil(t, expvar.Get("NoKV.Txns.Active"))
}

func TestStatsSnapshotTracksThrottleAndWalRemovals(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.Set([]byte("wal-metrics"), []byte("value")))
	lsmCore := db.LSM().(*lsm.LSM)
	require.NoError(t, lsmCore.Rotate())
	require.Eventually(t, func() bool {
		return db.Info().Snapshot().WAL.SegmentsRemoved > 0
	}, 5*time.Second, 10*time.Millisecond)

	db.ApplyThrottle(lsm.WriteThrottleStop)
	defer db.ApplyThrottle(lsm.WriteThrottleNone)

	snap := db.Info().Snapshot()
	require.True(t, snap.Write.ThrottleActive)
	require.Equal(t, "stop", snap.Write.ThrottleMode)
	require.Equal(t, uint32(1000), snap.Write.ThrottlePressure)
	require.Equal(t, uint64(0), snap.Write.ThrottleRate)
	require.Greater(t, snap.WAL.SegmentsRemoved, uint64(0))
	require.Greater(t, snap.WAL.SegmentCount, int64(0))

	db.Info().Collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.WAL.SegmentsRemoved, exported.WAL.SegmentsRemoved)
	require.True(t, exported.Write.ThrottleActive)
	require.Equal(t, "stop", exported.Write.ThrottleMode)
	require.Equal(t, uint32(1000), exported.Write.ThrottlePressure)
	require.Equal(t, uint64(0), exported.Write.ThrottleRate)

	db.ApplyThrottle(lsm.WriteThrottleNone)
	snapAfter := db.Info().Snapshot()
	require.False(t, snapAfter.Write.ThrottleActive)
	require.Equal(t, "none", snapAfter.Write.ThrottleMode)
	require.Equal(t, uint32(0), snapAfter.Write.ThrottlePressure)
	require.Equal(t, uint64(0), snapAfter.Write.ThrottleRate)

	db.Info().Collect()
	exportedAfter := loadExpvarStatsSnapshot(t)
	require.False(t, exportedAfter.Write.ThrottleActive)

	// Legacy scalar key should remain absent.
	require.Nil(t, expvar.Get("NoKV.Stats.Write.Throttle"))
}

func TestStatsSnapshotIncludesMVCCGCPlanner(t *testing.T) {
	opt := newTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePointFn = func() uint64 { return 100 }
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	db.SetMVCCMaintenanceSnapshotSource(func() storemvcc.MaintenanceSnapshot {
		return storemvcc.MaintenanceSnapshot{
			Enabled:              true,
			Runs:                 2,
			LastUnix:             123,
			LastDurationMs:       4.5,
			LastResolveError:     "resolve warn",
			LastSafePointSkipped: true,
			LastResolveLocks: storemvcc.ResolveLocksStats{
				ScannedLocks:       5,
				ExpiredLocks:       4,
				ResolvedLocks:      3,
				CommittedLocks:     2,
				RolledBackLocks:    1,
				DeletedLockMarkers: 3,
			},
			LastApply: storemvcc.ApplyStats{
				AppliedWriteDeletes:   7,
				AppliedDefaultDeletes: 8,
			},
			LastOrphanDefaults: storemvcc.OrphanDefaultStats{
				OrphanDefaults:        9,
				AppliedDefaultDeletes: 10,
			},
		}
	})

	key := []byte("stats-mvcc-gc-key")
	applyStatsMVCCWrite(t, db, key, 150, 140)
	applyStatsMVCCWrite(t, db, key, 90, 80)
	applyStatsMVCCWrite(t, db, key, 40, 30)

	var snap stats.StatsSnapshot
	require.Eventually(t, func() bool {
		snap = db.Info().Snapshot()
		return snap.MVCCGC.Enabled &&
			snap.MVCCGC.Runs > 0 &&
			snap.MVCCGC.LastError == "" &&
			snap.MVCCGC.DroppableWrites == 1
	}, time.Second, 10*time.Millisecond)
	require.Equal(t, uint64(1), snap.MVCCGC.ScannedKeys)
	require.Equal(t, uint64(3), snap.MVCCGC.WriteVersions)
	require.Equal(t, uint64(1), snap.MVCCGC.AnchorWrites)
	require.Equal(t, uint64(3), snap.MVCCGC.MaxVersionsPerKey)
	require.Equal(t, uint64(100), snap.MVCCGC.MaxEffectiveSafePoint)
	require.True(t, snap.MVCCGC.MaintenanceEnabled)
	require.Equal(t, uint64(2), snap.MVCCGC.MaintenanceRuns)
	require.Equal(t, "resolve warn", snap.MVCCGC.MaintenanceResolveError)
	require.True(t, snap.MVCCGC.MaintenanceSafePointSkipped)
	require.Equal(t, uint64(3), snap.MVCCGC.ResolvedLocks)
	require.Equal(t, uint64(7), snap.MVCCGC.AppliedWriteDeletes)
	require.Equal(t, uint64(10), snap.MVCCGC.AppliedOrphanDefaults)

	db.Info().Collect()
	exported := loadExpvarStatsSnapshot(t)
	require.Equal(t, snap.MVCCGC.Enabled, exported.MVCCGC.Enabled)
	require.Equal(t, snap.MVCCGC.Runs, exported.MVCCGC.Runs)
	require.Equal(t, snap.MVCCGC.DroppableWrites, exported.MVCCGC.DroppableWrites)
	require.Equal(t, snap.MVCCGC.MaxVersionsPerKey, exported.MVCCGC.MaxVersionsPerKey)
	require.Equal(t, snap.MVCCGC.MaintenanceRuns, exported.MVCCGC.MaintenanceRuns)
	require.Equal(t, snap.MVCCGC.MaintenanceResolveError, exported.MVCCGC.MaintenanceResolveError)
	require.Equal(t, snap.MVCCGC.MaintenanceSafePointSkipped, exported.MVCCGC.MaintenanceSafePointSkipped)
	require.Equal(t, snap.MVCCGC.ResolvedLocks, exported.MVCCGC.ResolvedLocks)
	require.Equal(t, snap.MVCCGC.AppliedWriteDeletes, exported.MVCCGC.AppliedWriteDeletes)
}

func applyStatsMVCCWrite(t *testing.T, db *NoKV.DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := txnmvcc.EncodeWrite(txnmvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	entry := entrykv.NewInternalEntry(entrykv.CFWrite, key, commitTs, write, 0, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*entrykv.Entry{entry}))
}

func loadExpvarStatsSnapshot(t *testing.T) stats.StatsSnapshot {
	t.Helper()

	v := expvar.Get("NoKV.Stats")
	require.NotNil(t, v)

	var snap stats.StatsSnapshot
	require.NoError(t, json.Unmarshal([]byte(v.String()), &snap))
	return snap
}
