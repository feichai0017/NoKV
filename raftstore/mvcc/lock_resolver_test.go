package mvcc_test

import (
	"context"
	"fmt"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	entrykv "github.com/feichai0017/NoKV/engine/kv"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator"
	"github.com/feichai0017/NoKV/percolator/latch"
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

	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, &testLockResolver{db: db}, storemvcc.ResolveLocksOptions{
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

	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, &testLockResolver{db: db}, storemvcc.ResolveLocksOptions{CurrentTs: 20})
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

func TestResolveExpiredLocksRollsBackSecondaryAfterPrimaryAuthorityRollback(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	primary := []byte("primary")
	secondary := []byte("secondary")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, secondary, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, secondary, primary, 10, 5, kvrpcpb.Mutation_Put)

	resolver := &testLockResolver{db: db}
	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, resolver, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedLocks)
	require.Equal(t, uint64(1), stats.ExpiredLocks)
	require.Zero(t, stats.RetainedLocks)
	require.Equal(t, uint64(1), stats.ResolvedLocks)
	require.Equal(t, uint64(1), stats.RolledBackLocks)
	require.Equal(t, 1, resolver.statusCalls)
	require.Equal(t, 1, resolver.calls)

	lock, err := db.GetInternalEntry(entrykv.CFLock, secondary, entrykv.MaxVersion)
	require.NoError(t, err)
	defer lock.DecrRef()
	require.NotZero(t, lock.Meta&entrykv.BitDelete)

	_, err = db.GetInternalEntry(entrykv.CFWrite, secondary, 10)
	require.NoError(t, err)
}

func TestResolveExpiredLocksReplicatedUsesResolveLockCommand(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	primary := []byte("primary")
	secondary := []byte("secondary")
	applyMVCCGCWrite(t, db, primary, 30, 10)
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, secondary, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, secondary, primary, 10, 5, kvrpcpb.Mutation_Put)

	proposer := &testLockResolver{db: db}
	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, proposer, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)
	require.Equal(t, 1, proposer.calls)
	require.Equal(t, uint64(1), stats.ResolvedLocks)
	require.Equal(t, uint64(1), stats.CommittedLocks)

	write, err := db.GetInternalEntry(entrykv.CFWrite, secondary, 30)
	require.NoError(t, err)
	defer write.DecrRef()
	decoded, err := txnmvcc.DecodeWrite(write.Value)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Mutation_Put, decoded.Kind)
	require.Equal(t, uint64(10), decoded.StartTs)
}

func TestResolveExpiredLocksReplicatedRollsBackExpiredPrimary(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("primary")
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, 10, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, key, key, 10, 5, kvrpcpb.Mutation_Put)

	proposer := &testLockResolver{db: db}
	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, proposer, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ResolvedLocks)
	require.Equal(t, uint64(1), stats.RolledBackLocks)

	write, err := db.GetInternalEntry(entrykv.CFWrite, key, 10)
	require.NoError(t, err)
	defer write.DecrRef()
	decoded, err := txnmvcc.DecodeWrite(write.Value)
	require.NoError(t, err)
	require.Equal(t, kvrpcpb.Mutation_Rollback, decoded.Kind)
}

func TestResolveExpiredLocksRetainsLiveLock(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("live")
	applyMVCCGCLockRecord(t, db, key, key, 10, 100, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, &testLockResolver{db: db}, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedLocks)
	require.Equal(t, uint64(1), stats.RetainedLocks)
	require.Zero(t, stats.ResolvedLocks)

	floor, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.Equal(t, uint64(1), floor.ActiveLocks)
	require.Equal(t, uint64(10), floor.OldestStartTs)
}

func TestResolveExpiredLocksRetainsTTLAcrossUint64Boundary(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("overflow-live")
	startTs := ^uint64(0) - 5
	applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, startTs, []byte("value"), 0, 0)
	applyMVCCGCLockRecord(t, db, key, key, startTs, 10, kvrpcpb.Mutation_Put)

	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, &testLockResolver{db: db}, storemvcc.ResolveLocksOptions{CurrentTs: startTs + 4})
	require.NoError(t, err)
	require.Equal(t, uint64(1), stats.ScannedLocks)
	require.Equal(t, uint64(1), stats.RetainedLocks)
	require.Zero(t, stats.ResolvedLocks)

	lock, err := db.GetInternalEntry(entrykv.CFLock, key, entrykv.MaxVersion)
	require.NoError(t, err)
	defer lock.DecrRef()
	require.Zero(t, lock.Meta&entrykv.BitDelete)
}

func TestResolveExpiredLocksUnblocksTxnFloor(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	key := []byte("old")
	applyMVCCGCLockRecord(t, db, key, key, 10, 5, kvrpcpb.Mutation_Put)

	_, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, &testLockResolver{db: db}, storemvcc.ResolveLocksOptions{CurrentTs: 20})
	require.NoError(t, err)

	floor, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.False(t, floor.Active())
}

func TestResolveExpiredLocksStopsAtMaxLocks(t *testing.T) {
	db := openMVCCGCPlanTestDB(t)
	for i := range 3 {
		key := fmt.Appendf(nil, "lock-%d", i)
		startTs := uint64(10 + i)
		applyVersionedEntryForApplyTest(t, db, entrykv.CFDefault, key, startTs, []byte("value"), 0, 0)
		applyMVCCGCLockRecord(t, db, key, key, startTs, 5, kvrpcpb.Mutation_Put)
	}

	stats, err := storemvcc.ResolveExpiredLocksReplicated(context.Background(), db, &testLockResolver{db: db}, storemvcc.ResolveLocksOptions{
		CurrentTs:  20,
		BatchLocks: 10,
		MaxLocks:   2,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), stats.ScannedLocks)
	require.Equal(t, uint64(2), stats.ExpiredLocks)
	require.Equal(t, uint64(2), stats.ResolvedLocks)

	floor, err := storemvcc.PlanTxnFloor(context.Background(), db)
	require.NoError(t, err)
	require.True(t, floor.Active())
	require.Equal(t, uint64(12), floor.OldestStartTs)
}

type testLockResolver struct {
	db          *NoKV.DB
	calls       int
	statusCalls int
}

func (p *testLockResolver) CheckTxnStatus(_ context.Context, primary []byte, lockTs, currentTs uint64) (*kvrpcpb.CheckTxnStatusResponse, error) {
	p.statusCalls++
	if p.db == nil {
		return &kvrpcpb.CheckTxnStatusResponse{}, nil
	}
	return percolator.CheckTxnStatus(p.db, latch.NewManager(32), &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         primary,
		LockTs:             lockTs,
		CurrentTs:          currentTs,
		CallerStartTs:      currentTs,
		RollbackIfNotExist: true,
	}), nil
}

func (p *testLockResolver) ResolveLocks(_ context.Context, startVersion, commitVersion uint64, keys [][]byte) (uint64, error) {
	p.calls++
	if p.db == nil {
		return uint64(len(keys)), nil
	}
	count, keyErr := percolator.ResolveLock(p.db, latch.NewManager(32), &kvrpcpb.ResolveLockRequest{
		StartVersion:  startVersion,
		CommitVersion: commitVersion,
		Keys:          keys,
	})
	if keyErr != nil {
		return 0, fmt.Errorf("resolve lock: %v", keyErr)
	}
	return count, nil
}
