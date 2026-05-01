package NoKV

import (
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/stretchr/testify/require"
)

func applyMVCCGCPlannerWrite(t *testing.T, db *DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := percolator.EncodeWrite(percolator.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	applyVersionedEntryForTest(t, db, kv.CFWrite, key, commitTs, write, 0)
}

func applyMVCCGCPlannerLock(t *testing.T, db *DB, key []byte, startTs uint64) {
	t.Helper()
	lock := percolator.EncodeLock(percolator.Lock{
		Primary: key,
		Ts:      startTs,
	})
	applyVersionedEntryForTest(t, db, kv.CFLock, key, kv.MaxVersion, lock, 0)
}

func waitMVCCGCPlannerSnapshot(t *testing.T, db *DB, fn func(MVCCGCPlanSnapshot) bool) MVCCGCPlanSnapshot {
	t.Helper()
	var snap MVCCGCPlanSnapshot
	require.Eventually(t, func() bool {
		snap = db.MVCCGCPlanSnapshot()
		return fn(snap)
	}, time.Second, 10*time.Millisecond)
	return snap
}

func TestMVCCGCPlannerDisabledByDefault(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.False(t, db.MVCCGCPlanSnapshot().Enabled)
}

func TestMVCCGCPlannerRunsReadOnlyPlan(t *testing.T) {
	opt := newTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePoint = func() uint64 { return 100 }
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("planner-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)

	snap := waitMVCCGCPlannerSnapshot(t, db, func(s MVCCGCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})
	require.True(t, snap.Enabled)
	require.Greater(t, snap.Runs, uint64(0))
	require.Equal(t, uint64(1), snap.LastPlan.Keys)

	entry, err := db.GetInternalEntry(kv.CFWrite, key, 40)
	require.NoError(t, err)
	defer entry.DecrRef()
	require.Zero(t, entry.Meta&kv.BitDelete)
}

func TestMVCCGCPlannerReadsSafePointEachRun(t *testing.T) {
	var safePoint atomic.Uint64
	opt := newTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePoint = safePoint.Load
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("dynamic-safe-point-key")
	applyMVCCGCPlannerWrite(t, db, key, 150, 140)
	applyMVCCGCPlannerWrite(t, db, key, 90, 80)
	applyMVCCGCPlannerWrite(t, db, key, 40, 30)
	waitMVCCGCPlannerSnapshot(t, db, func(s MVCCGCPlanSnapshot) bool {
		return s.Runs > 0 && s.LastPlan.Keys == 0
	})

	safePoint.Store(100)
	waitMVCCGCPlannerSnapshot(t, db, func(s MVCCGCPlanSnapshot) bool {
		return s.LastError == "" && s.LastPlan.DroppableWrites == 1
	})
}

func TestMVCCGCPlannerHonorsSnapshotRetentionAndTxnFloor(t *testing.T) {
	opt := newTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePoint = func() uint64 { return 100 }
	opt.MVCCGCSnapshotRetention = func() rootstate.SnapshotRetentionIndex {
		return rootstate.SnapshotRetentionIndex{
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		}
	}
	db := openTestDB(t, opt)
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

	snap := waitMVCCGCPlannerSnapshot(t, db, func(s MVCCGCPlanSnapshot) bool {
		return s.LastError == "" &&
			s.LastTxnFloor.OldestStartTs == 60 &&
			s.LastPlan.Keys == 2 &&
			s.LastPlan.SafePointClampedKeys == 2
	})
	require.Equal(t, uint64(1), snap.LastTxnFloor.ActiveLocks)
	require.Equal(t, uint64(0), snap.LastPlan.DroppableWrites)
	require.Equal(t, uint64(50), snap.LastPlan.MinEffectiveSafePoint)
	require.Equal(t, uint64(60), snap.LastPlan.MaxEffectiveSafePoint)
}

func TestMVCCGCPlannerRecordsTxnFloorErrors(t *testing.T) {
	opt := newTestOptions(t)
	opt.MVCCGCPlanInterval = 5 * time.Millisecond
	opt.MVCCGCSafePoint = func() uint64 { return 100 }
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	applyVersionedEntryForTest(t, db, kv.CFLock, []byte("bad-lock"), kv.MaxVersion, []byte{0xff}, 0)

	snap := waitMVCCGCPlannerSnapshot(t, db, func(s MVCCGCPlanSnapshot) bool {
		return strings.Contains(s.LastError, "decode CFLock")
	})
	require.Equal(t, uint64(0), snap.LastPlan.Keys)
}
