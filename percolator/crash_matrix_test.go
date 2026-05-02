package percolator

import (
	"path/filepath"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestPercolatorCrashMatrixPrimaryCommittedSecondaryRecovered(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	primary := []byte("crash-primary-commit")
	secondary := []byte("crash-secondary-commit")
	startTs := uint64(100)
	commitTs := uint64(120)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{primary},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))

	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey: primary,
		LockTs:     startTs,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, commitTs, status.GetCommitVersion())

	resolved, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		StartVersion:  startTs,
		CommitVersion: status.GetCommitVersion(),
		Keys:          [][]byte{secondary},
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), resolved)

	reader := NewReader(db)
	value, _, err := reader.GetValue(primary, commitTs+10)
	require.NoError(t, err)
	require.Equal(t, []byte("primary-value"), value)
	value, _, err = reader.GetValue(secondary, commitTs+10)
	require.NoError(t, err)
	require.Equal(t, []byte("secondary-value"), value)
	lock, err := reader.GetLock(secondary)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestPercolatorCrashMatrixPrimaryRollbackSecondaryRecovered(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	primary := []byte("crash-primary-rollback")
	secondary := []byte("crash-secondary-rollback")
	startTs := uint64(200)

	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  primary,
		StartVersion: startTs,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: primary, Value: []byte("primary-value")},
			{Op: kvrpcpb.Mutation_Put, Key: secondary, Value: []byte("secondary-value")},
		},
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{primary},
		StartVersion: startTs,
	}))

	status := CheckTxnStatus(db, latches, &kvrpcpb.CheckTxnStatusRequest{
		PrimaryKey:         primary,
		LockTs:             startTs,
		RollbackIfNotExist: true,
	})
	require.Nil(t, status.GetError())
	require.Equal(t, kvrpcpb.CheckTxnStatusAction_CheckTxnStatusLockNotExistRollback, status.GetAction())

	resolved, keyErr := ResolveLock(db, latches, &kvrpcpb.ResolveLockRequest{
		StartVersion: startTs,
		Keys:         [][]byte{secondary},
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), resolved)

	reader := NewReader(db)
	_, _, err := reader.GetValue(primary, startTs+100)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	_, _, err = reader.GetValue(secondary, startTs+100)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	lock, err := reader.GetLock(secondary)
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestPercolatorCrashMatrixCommitAndRollbackIdempotentAfterRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "db")
	db, err := NoKV.Open(testOptionsForDir(dir))
	require.NoError(t, err)
	latches := latch.NewManager(32)

	committedKey := []byte("crash-restart-committed")
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  committedKey,
		StartVersion: 300,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: committedKey, Value: []byte("committed-value")},
		},
	}))
	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{committedKey},
		StartVersion:  300,
		CommitVersion: 320,
	}))

	rolledBackKey := []byte("crash-restart-rolled-back")
	require.Empty(t, Prewrite(db, latches, &kvrpcpb.PrewriteRequest{
		PrimaryLock:  rolledBackKey,
		StartVersion: 400,
		LockTtl:      3000,
		Mutations: []*kvrpcpb.Mutation{
			{Op: kvrpcpb.Mutation_Put, Key: rolledBackKey, Value: []byte("rolled-back-value")},
		},
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{rolledBackKey},
		StartVersion: 400,
	}))
	require.NoError(t, db.Close())

	db, err = NoKV.Open(testOptionsForDir(dir))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	require.Nil(t, Commit(db, latches, &kvrpcpb.CommitRequest{
		Keys:          [][]byte{committedKey},
		StartVersion:  300,
		CommitVersion: 320,
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{committedKey},
		StartVersion: 300,
	}))
	require.Nil(t, BatchRollback(db, latches, &kvrpcpb.BatchRollbackRequest{
		Keys:         [][]byte{rolledBackKey},
		StartVersion: 400,
	}))

	reader := NewReader(db)
	value, _, err := reader.GetValue(committedKey, 500)
	require.NoError(t, err)
	require.Equal(t, []byte("committed-value"), value)
	_, _, err = reader.GetValue(rolledBackKey, 500)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}
