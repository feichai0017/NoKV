package kv

import (
	"context"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/fsmeta"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/stretchr/testify/require"
)

func openMVCCGCPlanTestDB(t *testing.T) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func applyMVCCGCWrite(t *testing.T, db *NoKV.DB, key []byte, commitTs, startTs uint64) {
	t.Helper()
	write := percolator.EncodeWrite(percolator.Write{Kind: kvrpcpb.Mutation_Put, StartTs: startTs})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, commitTs, write, 0, 0)
}

func applyMVCCGCPutVersion(t *testing.T, db *NoKV.DB, key []byte, commitTs, startTs uint64, value string) {
	t.Helper()
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, startTs, []byte(value), 0, 0)
	applyMVCCGCWrite(t, db, key, commitTs, startTs)
}

func TestPlanMVCCGCReportsMountScopedPlan(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	volKey, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	otherKey, err := fsmeta.EncodeInodeKey("other", 10)
	require.NoError(t, err)
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCWrite(t, db, key, 150, 140)
		applyMVCCGCWrite(t, db, key, 90, 80)
		applyMVCCGCWrite(t, db, key, 40, 30)
	}

	stats, err := PlanMVCCGC(context.Background(), db, MVCCGCSafePointPolicy{
		RequestedSafePoint: 100,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.Keys)
	require.Equal(t, uint64(6), stats.WriteVersions)
	require.Equal(t, uint64(5), stats.RetainedWrites)
	require.Equal(t, uint64(1), stats.DroppableWrites)
	require.Equal(t, uint64(2), stats.AnchorWrites)
	require.Equal(t, uint64(5), stats.RetainedDefaultRefs)
	require.Equal(t, uint64(1), stats.SafePointClampedKeys)
	require.Equal(t, uint64(3), stats.MaxVersionsPerKey)
	require.Equal(t, uint64(50), stats.MinEffectiveSafePoint)
	require.Equal(t, uint64(100), stats.MaxEffectiveSafePoint)
}

func TestPlanMVCCGCDoesNotDeleteData(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	applyMVCCGCWrite(t, db, key, 90, 80)
	applyMVCCGCWrite(t, db, key, 40, 30)

	_, err = PlanMVCCGC(context.Background(), db, MVCCGCSafePointPolicy{RequestedSafePoint: 100})
	require.NoError(t, err)

	entry, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer entry.DecrRef()
	require.NotNil(t, entry.Value)
}

func TestPlanMVCCGCRejectsCorruptWritePayload(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFWrite, key, 90, []byte{0xff}, 0, 0)

	_, err = PlanMVCCGC(context.Background(), db, MVCCGCSafePointPolicy{RequestedSafePoint: 100})
	require.ErrorContains(t, err, "decode CFWrite")
}

func TestPlanMVCCGCHonorsContextCancellation(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	applyMVCCGCWrite(t, db, key, 90, 80)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err = PlanMVCCGC(ctx, db, MVCCGCSafePointPolicy{RequestedSafePoint: 100})
	require.ErrorIs(t, err, context.Canceled)
}

func TestApplyMVCCGCDeletesDroppableWriteAndDefault(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
	applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
	applyMVCCGCPutVersion(t, db, key, 40, 30, "old")

	stats, err := ApplyMVCCGC(context.Background(), db, MVCCGCSafePointPolicy{RequestedSafePoint: 100}, MVCCGCApplyOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)
	require.Equal(t, uint64(1), stats.DroppableWrites)
	require.Equal(t, uint64(1), stats.AnchorWrites)

	droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
	require.NoError(t, err)
	defer droppedWrite.DecrRef()
	require.NotZero(t, droppedWrite.Meta&entrykv.BitDelete)

	droppedDefault, err := db.GetInternalEntry(entrykv.CFDefault, key, 30)
	require.NoError(t, err)
	defer droppedDefault.DecrRef()
	require.NotZero(t, droppedDefault.Meta&entrykv.BitDelete)

	anchorDefault, err := db.GetInternalEntry(entrykv.CFDefault, key, 80)
	require.NoError(t, err)
	defer anchorDefault.DecrRef()
	require.Zero(t, anchorDefault.Meta&entrykv.BitDelete)
	require.Equal(t, []byte("anchor"), anchorDefault.Value)
}

func TestApplyMVCCGCHonorsMountScopedRetention(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	volKey, err := fsmeta.EncodeInodeKey("vol", 10)
	require.NoError(t, err)
	otherKey, err := fsmeta.EncodeInodeKey("other", 10)
	require.NoError(t, err)
	for _, key := range [][]byte{volKey, otherKey} {
		applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
		applyMVCCGCPutVersion(t, db, key, 90, 80, "mid")
		applyMVCCGCPutVersion(t, db, key, 40, 30, "old")
	}

	stats, err := ApplyMVCCGC(context.Background(), db, MVCCGCSafePointPolicy{
		RequestedSafePoint: 100,
		SnapshotRetention: rootstate.SnapshotRetentionIndex{
			GlobalFloor: 50,
			MountFloors: map[string]uint64{
				"vol": 50,
			},
		},
	}, MVCCGCApplyOptions{})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(1), stats.AppliedDefaultDeletes)

	retainedVolWrite, err := db.GetInternalEntry(entrykv.CFWrite, volKey, 40)
	require.NoError(t, err)
	defer retainedVolWrite.DecrRef()
	require.Zero(t, retainedVolWrite.Meta&entrykv.BitDelete)

	droppedOtherWrite, err := db.GetInternalEntry(entrykv.CFWrite, otherKey, 40)
	require.NoError(t, err)
	defer droppedOtherWrite.DecrRef()
	require.NotZero(t, droppedOtherWrite.Meta&entrykv.BitDelete)
}

func TestApplyMVCCGCBatchesWithoutRescanningDeletedKeys(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	for i := range byte(4) {
		key, err := fsmeta.EncodeInodeKey("vol", fsmeta.InodeID(100+uint64(i)))
		require.NoError(t, err)
		applyMVCCGCPutVersion(t, db, key, 150, 140, "new")
		applyMVCCGCPutVersion(t, db, key, 90, 80, "anchor")
		applyMVCCGCPutVersion(t, db, key, 40, 30, "old")
	}

	stats, err := ApplyMVCCGC(
		context.Background(),
		db,
		MVCCGCSafePointPolicy{RequestedSafePoint: 100},
		MVCCGCApplyOptions{BatchEntries: 2},
	)
	require.NoError(t, err)
	require.Equal(t, uint64(4), stats.Keys)
	require.Equal(t, uint64(4), stats.AppliedWriteDeletes)
	require.Equal(t, uint64(4), stats.AppliedDefaultDeletes)
	require.Equal(t, uint64(4), stats.DroppableWrites)

	for i := range byte(4) {
		key, err := fsmeta.EncodeInodeKey("vol", fsmeta.InodeID(100+uint64(i)))
		require.NoError(t, err)
		droppedWrite, err := db.GetInternalEntry(entrykv.CFWrite, key, 40)
		require.NoError(t, err)
		require.NotZero(t, droppedWrite.Meta&entrykv.BitDelete)
		droppedWrite.DecrRef()
	}
}
