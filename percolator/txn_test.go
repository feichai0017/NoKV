package percolator

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/utils"
)

func openTestDB(t *testing.T) *NoKV.DB {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = filepath.Join(t.TempDir(), "db")
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = utils.DefaultValueThreshold
	db := NoKV.Open(opt)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestPrewriteAndCommitPut(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	req := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   []byte("k1"),
			Value: []byte("value1"),
		}},
		PrimaryLock:  []byte("k1"),
		StartVersion: 10,
		LockTtl:      3000,
	}
	errs := Prewrite(db, latches, req)
	require.Empty(t, errs)

	reader := NewReader(db)
	lock, err := reader.GetLock([]byte("k1"))
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, req.StartVersion, lock.Ts)

	commitReq := &pb.CommitRequest{
		Keys:          [][]byte{[]byte("k1")},
		StartVersion:  req.StartVersion,
		CommitVersion: 20,
	}
	require.Nil(t, Commit(db, latches, commitReq))

	write, commitTs, err := reader.MostRecentWrite([]byte("k1"))
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, uint64(20), commitTs)
	require.Equal(t, req.StartVersion, write.StartTs)

	val, err := reader.GetValue([]byte("k1"), 30)
	require.NoError(t, err)
	require.Equal(t, []byte("value1"), val)
	lock, err = reader.GetLock([]byte("k1"))
	require.NoError(t, err)
	require.Nil(t, lock)
}

func TestPrewriteConflictingLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	first := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   []byte("conflict"),
			Value: []byte("v1"),
		}},
		PrimaryLock:  []byte("conflict"),
		StartVersion: 5,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, first))

	second := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   []byte("conflict"),
			Value: []byte("v2"),
		}},
		PrimaryLock:  []byte("conflict"),
		StartVersion: 8,
		LockTtl:      1000,
	}
	errs := Prewrite(db, latches, second)
	require.Len(t, errs, 1)
	require.NotNil(t, errs[0].GetLocked())
}

func TestCommitMissingLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	commit := &pb.CommitRequest{
		Keys:          [][]byte{[]byte("missing")},
		StartVersion:  3,
		CommitVersion: 6,
	}
	err := Commit(db, latches, commit)
	require.NotNil(t, err)
	require.Contains(t, err.GetAbort(), "lock not found")
}

func TestBatchRollbackRemovesLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	req := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   []byte("rk"),
			Value: []byte("v"),
		}},
		PrimaryLock:  []byte("rk"),
		StartVersion: 8,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, req))

	rollback := &pb.BatchRollbackRequest{Keys: [][]byte{[]byte("rk")}, StartVersion: 8}
	require.Nil(t, BatchRollback(db, latches, rollback))

	reader := NewReader(db)
	lock, err := reader.GetLock([]byte("rk"))
	require.NoError(t, err)
	require.Nil(t, lock)
	write, commitTs, err := reader.GetWriteByStartTs([]byte("rk"), 8)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, pb.Mutation_Rollback, write.Kind)
	require.Equal(t, uint64(8), commitTs)
}

func TestResolveLockCommit(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   []byte("res"),
			Value: []byte("val"),
		}},
		PrimaryLock:  []byte("res"),
		StartVersion: 40,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))
	count, keyErr := ResolveLock(db, latches, &pb.ResolveLockRequest{
		Keys:          [][]byte{[]byte("res")},
		StartVersion:  40,
		CommitVersion: 50,
	})
	require.Nil(t, keyErr)
	require.Equal(t, uint64(1), count)
	reader := NewReader(db)
	val, err := reader.GetValue([]byte("res"), 60)
	require.NoError(t, err)
	require.Equal(t, []byte("val"), val)
}

func TestCheckTxnStatusTTLExpire(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	startTs := uint64(100)
	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   []byte("primary"),
			Value: []byte("value"),
		}},
		PrimaryLock:  []byte("primary"),
		StartVersion: startTs,
		LockTtl:      5,
	}
	require.Empty(t, Prewrite(db, latches, pre))
	resp := CheckTxnStatus(db, latches, &pb.CheckTxnStatusRequest{
		PrimaryKey:         []byte("primary"),
		LockTs:             startTs,
		CurrentTs:          startTs + 10,
		RollbackIfNotExist: true,
	})
	require.Equal(t, pb.CheckTxnStatusAction_CheckTxnStatusTTLExpireRollback, resp.GetAction())
	reader := NewReader(db)
	lock, err := reader.GetLock([]byte("primary"))
	require.NoError(t, err)
	require.Nil(t, lock)
}
