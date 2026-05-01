package mvcc_test

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator/mvcc"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/stretchr/testify/require"
)

func newMVCCGCPlannerTestOptions(t *testing.T) *NoKV.Options {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	return opt
}

func openMVCCGCPlannerTestDB(t *testing.T, opt *NoKV.Options) *NoKV.DB {
	t.Helper()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}

func applyMVCCGCPlannerEntry(t *testing.T, db *NoKV.DB, cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := kv.NewInternalEntry(cf, key, version, kv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))
}

func applyMVCCGCPlannerWrite(t *testing.T, db *NoKV.DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := mvcc.EncodeWrite(mvcc.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	applyMVCCGCPlannerEntry(t, db, kv.CFWrite, key, commitTs, write, 0)
}

func applyMVCCGCPlannerLock(t *testing.T, db *NoKV.DB, key []byte, startTs uint64) {
	t.Helper()
	lock := mvcc.EncodeLock(mvcc.Lock{
		Primary: key,
		Ts:      startTs,
	})
	applyMVCCGCPlannerEntry(t, db, kv.CFLock, key, kv.MaxVersion, lock, 0)
}

func waitMVCCGCPlannerSnapshot(t *testing.T, db *NoKV.DB, fn func(storemvcc.GCPlanSnapshot) bool) storemvcc.GCPlanSnapshot {
	t.Helper()
	var snap storemvcc.GCPlanSnapshot
	require.Eventually(t, func() bool {
		snap = db.MVCCGCPlanSnapshot()
		return fn(snap)
	}, time.Second, 10*time.Millisecond)
	return snap
}

func TestMVCCGCPlannerDisabledByDefault(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.False(t, db.MVCCGCPlanSnapshot().Enabled)
}

func TestMVCCGCPlannerRunsReadOnlyPlan(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePointFn = func() uint64 { return 100 }
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("planner-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)

	snap := waitMVCCGCPlannerSnapshot(t, db, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})
	require.True(t, snap.Enabled)
	require.Greater(t, snap.Runs, uint64(0))
	require.Equal(t, uint64(1), snap.LastPlan.ScannedKeys)

	entry, err := db.GetInternalEntry(kv.CFWrite, key, 40)
	require.NoError(t, err)
	defer entry.DecrRef()
	require.Zero(t, entry.Meta&kv.BitDelete)
}

func TestMVCCGCPlannerReadsSafePointEachRun(t *testing.T) {
	var safePoint atomic.Uint64
	opt := newMVCCGCPlannerTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePointFn = safePoint.Load
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("dynamic-safe-point-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	waitMVCCGCPlannerSnapshot(t, db, func(s storemvcc.GCPlanSnapshot) bool {
		return s.Runs > 0 && s.SkippedRuns > 0 && s.LastPlan.ScannedKeys == 0
	})

	safePoint.Store(100)
	waitMVCCGCPlannerSnapshot(t, db, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})
}

func TestMVCCGCPlannerRetainsLastPlanWhenSafePointDisabled(t *testing.T) {
	var safePoint atomic.Uint64
	safePoint.Store(100)
	opt := newMVCCGCPlannerTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePointFn = safePoint.Load
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("disabled-safe-point-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	waitMVCCGCPlannerSnapshot(t, db, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})

	safePoint.Store(0)
	time.Sleep(20 * time.Millisecond)
	snap := db.MVCCGCPlanSnapshot()
	require.Equal(t, uint64(1), snap.LastPlan.DroppableWrites)
	require.Greater(t, snap.SkippedRuns, uint64(0))
}

func TestMVCCGCPlannerHonorsSnapshotRetentionAndTxnFloor(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePointFn = func() uint64 { return 100 }
	opt.MVCCGCSnapshotRetentionFn = func() rootstate.SnapshotRetentionIndex {
		return rootstate.SnapshotRetentionIndex{
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		}
	}
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	volKey, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	otherKey, err := fsmeta.EncodeInodeKey("other", 10)
	require.NoError(t, err)
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCPlannerWrite(t, db, key, 150, 140)
		applyMVCCGCPlannerWrite(t, db, key, 90, 80)
		applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	}
	applyMVCCGCPlannerLock(t, db, []byte("active-lock"), 60)

	snap := waitMVCCGCPlannerSnapshot(t, db, func(s storemvcc.GCPlanSnapshot) bool {
		return s.LastError == "" &&
			s.LastTxnFloor.OldestStartTs == 60 &&
			s.LastPlan.ScannedKeys == 2 &&
			s.LastPlan.SafePointClampedKeys == 2
	})
	require.Equal(t, uint64(1), snap.LastTxnFloor.ActiveLocks)
	require.Equal(t, uint64(0), snap.LastPlan.DroppableWrites)
	require.Equal(t, uint64(50), snap.LastPlan.MinEffectiveSafePoint)
	require.Equal(t, uint64(60), snap.LastPlan.MaxEffectiveSafePoint)
}

func TestMVCCGCPlannerRecordsTxnFloorErrors(t *testing.T) {
	opt := newMVCCGCPlannerTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePointFn = func() uint64 { return 100 }
	db := openMVCCGCPlannerTestDB(t, opt)
	defer func() { _ = db.Close() }()

	applyMVCCGCPlannerEntry(t, db, kv.CFLock, []byte("bad-lock"), kv.MaxVersion, []byte{0xff}, 0)

	snap := waitMVCCGCPlannerSnapshot(t, db, func(s storemvcc.GCPlanSnapshot) bool {
		return strings.Contains(s.LastError, "decode CFLock")
	})
	require.Equal(t, uint64(0), snap.LastPlan.ScannedKeys)
}
