package mvccgc_test

import (
	"context"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	enginemvcc "github.com/feichai0017/NoKV/engine/mvcc"
	"github.com/feichai0017/NoKV/raftstore/mvccgc"
	"github.com/stretchr/testify/require"
)

func applyMVCCGCLock(t *testing.T, db *NoKV.DB, key []byte, startTs uint64) {
	t.Helper()
	lock := enginemvcc.EncodeLock(enginemvcc.Lock{
		Primary: key,
		Ts:      startTs,
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, key, entrykv.MaxVersion, lock, 0, 0)
}

func TestPlanMVCCGCTxnFloorScansActiveLocks(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	applyMVCCGCLock(t, db, []byte("a"), 80)
	applyMVCCGCLock(t, db, []byte("b"), 30)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, []byte("c"), entrykv.MaxVersion, nil, entrykv.BitDelete, 0)

	floor, err := mvccgc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.True(t, floor.Active())
	require.Equal(t, uint64(2), floor.ActiveLocks)
	require.Equal(t, uint64(30), floor.OldestStartTs)
	require.Equal(t, uint64(80), floor.MaxStartTs)
}

func TestPlanMVCCGCTxnFloorRejectsCorruptLock(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, []byte("bad"), entrykv.MaxVersion, []byte{0xff}, 0, 0)

	_, err := mvccgc.PlanTxnFloor(context.Background(), db)
	require.ErrorContains(t, err, "decode CFLock")
}

func TestPlanMVCCGCTxnFloorHonorsContextCancellation(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	applyMVCCGCLock(t, db, []byte("a"), 80)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := mvccgc.PlanTxnFloor(ctx, db)
	require.ErrorIs(t, err, context.Canceled)
}
