package mvcc_test

import (
	"context"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	txnmvcc "github.com/feichai0017/NoKV/percolator/mvcc"
	storemvcc "github.com/feichai0017/NoKV/raftstore/mvcc"
	"github.com/stretchr/testify/require"
)

func applyMVCCGCLockRecord(t *testing.T, db *NoKV.DB, key, primary []byte, startTs, ttl uint64, kind kvrpcpb.Mutation_Op) {
	t.Helper()
	lock := txnmvcc.EncodeLock(txnmvcc.Lock{
		Primary: primary,
		Ts:      startTs,
		TTL:     ttl,
		Kind:    kind,
	})
	applyVersionedEntryForApplyTest(t, db, entrykv.CFLock, key, entrykv.MaxVersion, lock, 0, 0)
}

func TestResolveExpiredLocksRollsBackExpiredPrimaryLock(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("primary")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, key, key, 10, 5, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ResolveExpiredLocks(context.Background(), db, storemvcc.ResolveLocksOptions{
		CurrentTs:  20,
		BatchLocks: 1,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedLocks)
	require.Equal(t, uint64(1), stats.ExpiredLocks)
	require.Equal(t, uint64(1), stats.ResolvedLocks)
	require.Equal(t, uint64(1), stats.RolledBackLocks)

	lock, err := db.GetInternalEntry(entrykv.CFLock, key, entrykv.MaxVersion)
	require.NoError(t, err)
	defer lock.DecrRef()
	require.NotZero(t, lock.Meta&entrykv.BitDelete)

	write, err := db.GetInternalEntry(entrykv.CFWrite, key, 10)
	require.NoError(t, err)
	defer write.DecrRef()
	decoded, err := txnmvcc.DecodeWrite(write.Value)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Mutation_Rollback, decoded.Kind)

	payload, err := db.GetInternalEntry(entrykv.CFDefault, key, 10)
	require.NoError(t, err)
	defer payload.DecrRef()
	require.NotZero(t, payload.Meta&entrykv.BitDelete)
}

func TestResolveExpiredLocksCommitsSecondaryFromPrimaryWrite(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	primary := []byte("primary")
	secondary := []byte("secondary")
	applyMVCCGCWrite(t, db, primary, 30, 10)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, secondary, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, secondary, primary, 10, 5, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ResolveExpiredLocks(context.Background(), db, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ResolvedLocks)
	require.Equal(t, uint64(1), stats.CommittedLocks)

	write, err := db.GetInternalEntry(entrykv.CFWrite, secondary, 30)
	require.NoError(t, err)
	defer write.DecrRef()
	decoded, err := txnmvcc.DecodeWrite(write.Value)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Mutation_Put, decoded.Kind)
	require.Equal(t, uint64(10), decoded.StartTs)

	lock, err := db.GetInternalEntry(entrykv.CFLock, secondary, entrykv.MaxVersion)
	require.NoError(t, err)
	defer lock.DecrRef()
	require.NotZero(t, lock.Meta&entrykv.BitDelete)
}

func TestResolveExpiredLocksRetainsLiveLock(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("live")
	applyMVCCGCLockRecord(t, db, key, key, 10, 100, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ResolveExpiredLocks(context.Background(), db, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedLocks)
	require.Equal(t, uint64(1), stats.RetainedLocks)
	require.Zero(t, stats.ResolvedLocks)

	floor, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor.ActiveLocks)
	require.Equal(t, uint64(10), floor.OldestStartTs)
}

func TestResolveExpiredLocksUnblocksTxnFloor(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("old")
	applyMVCCGCLockRecord(t, db, key, key, 10, 5, kvrpcpb.Mutation_Put)

	_, err := storemvcc.ResolveExpiredLocks(context.Background(), db, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)

	floor, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.False(t, floor.Active())
}
