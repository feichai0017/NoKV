package percolator

import (
	"errors"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
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

func TestReaderMostRecentWriteSkipsOtherCF(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, db.Set([]byte("b"), []byte("vb")))

	entry := EncodeWrite(Write{Kind: pb.Mutation_Put, StartTs: 1})
	require.NoError(t, db.SetVersionedEntry(kv.CFWrite, []byte("a"), 10, entry, 0))

	reader := NewReader(db)
	write, commitTs, err := reader.MostRecentWrite([]byte("a"))
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, uint64(10), commitTs)
	require.Equal(t, uint64(1), write.StartTs)
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

func TestEncodeDecodeLockRoundTrip(t *testing.T) {
	lock := Lock{
		Primary:     []byte("primary"),
		Ts:          10,
		TTL:         20,
		Kind:        pb.Mutation_Put,
		MinCommitTs: 30,
	}
	encoded := EncodeLock(lock)
	got, err := DecodeLock(encoded)
	require.NoError(t, err)
	require.Equal(t, lock.Primary, got.Primary)
	require.Equal(t, lock.Ts, got.Ts)
	require.Equal(t, lock.TTL, got.TTL)
	require.Equal(t, lock.Kind, got.Kind)
	require.Equal(t, lock.MinCommitTs, got.MinCommitTs)
}

func TestDecodeLockErrors(t *testing.T) {
	_, err := DecodeLock(nil)
	require.Error(t, err)

	_, err = DecodeLock([]byte{0x99})
	require.Error(t, err)

	_, err = DecodeLock([]byte{lockCodecVersion})
	require.Error(t, err)

	_, err = DecodeLock([]byte{lockCodecVersion, 0x05, 'a'})
	require.Error(t, err)
}

func TestEncodeDecodeWriteRoundTrip(t *testing.T) {
	write := Write{
		Kind:       pb.Mutation_Put,
		StartTs:    42,
		ShortValue: []byte("short"),
	}
	encoded := EncodeWrite(write)
	got, err := DecodeWrite(encoded)
	require.NoError(t, err)
	require.Equal(t, write.Kind, got.Kind)
	require.Equal(t, write.StartTs, got.StartTs)
	require.Equal(t, write.ShortValue, got.ShortValue)
}

func TestDecodeWriteErrors(t *testing.T) {
	_, err := DecodeWrite([]byte{writeCodecVersion})
	require.Error(t, err)

	_, err = DecodeWrite([]byte{0x99, 0x01, 0x01})
	require.Error(t, err)

	_, err = DecodeWrite([]byte{writeCodecVersion, byte(pb.Mutation_Put), 0x01, 0x01})
	require.Error(t, err)

	_, err = DecodeWrite([]byte{writeCodecVersion, byte(pb.Mutation_Put), 0x01, 0x01, 0x05})
	require.Error(t, err)
}

func TestKeyErrorHelpers(t *testing.T) {
	conflict := keyErrorWriteConflict([]byte("k"), []byte("p"), 3, 4, 5)
	require.NotNil(t, conflict.GetWriteConflict())
	require.Equal(t, uint64(3), conflict.GetWriteConflict().GetConflictTs())

	retry := keyErrorRetryable(errors.New("retry"))
	require.Equal(t, "retry", retry.GetRetryable())
}

func TestCommitKeyAlreadyRolledBack(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb")
	lock := &Lock{Primary: key, Ts: 10, Kind: pb.Mutation_Put}

	rollback := EncodeWrite(Write{Kind: pb.Mutation_Rollback, StartTs: lock.Ts})
	require.NoError(t, db.SetVersionedEntry(kv.CFWrite, key, 15, rollback, 0))

	err := commitKey(db, reader, key, lock, 20)
	require.NotNil(t, err)
	require.NotNil(t, err.GetAbort())
}

func TestCommitKeyWritesAndCleansLock(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("commit")
	lock := &Lock{Primary: key, Ts: 11, Kind: pb.Mutation_Put}

	require.NoError(t, db.SetVersionedEntry(kv.CFLock, key, lockColumnTs, EncodeLock(*lock), 0))
	commitErr := commitKey(db, reader, key, lock, 22)
	require.Nil(t, commitErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, lock.Ts)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, uint64(22), commitTs)
}

func TestCommitKeyAlreadyCommittedDifferentVersion(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("dup")
	lock := &Lock{Primary: key, Ts: 12, Kind: pb.Mutation_Put}

	require.NoError(t, db.SetVersionedEntry(kv.CFWrite, key, 30, EncodeWrite(Write{Kind: lock.Kind, StartTs: lock.Ts}), 0))
	require.NoError(t, db.SetVersionedEntry(kv.CFLock, key, lockColumnTs, EncodeLock(*lock), 0))

	commitErr := commitKey(db, reader, key, lock, 40)
	require.Nil(t, commitErr)
}

func TestRollbackKeyCreatesRollbackWrite(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb2")
	startTs := uint64(17)

	require.NoError(t, db.SetVersionedEntry(kv.CFLock, key, lockColumnTs, EncodeLock(Lock{
		Primary: key,
		Ts:      startTs,
		Kind:    pb.Mutation_Put,
	}), 0))
	require.NoError(t, db.SetVersionedEntry(kv.CFDefault, key, startTs, []byte("val"), 0))

	rollbackErr := rollbackKey(db, reader, key, startTs)
	require.Nil(t, rollbackErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, pb.Mutation_Rollback, write.Kind)
	require.Equal(t, startTs, commitTs)
}

func TestIsLockExpired(t *testing.T) {
	require.False(t, isLockExpired(nil, 10))
	require.False(t, isLockExpired(&Lock{Ts: 5, TTL: 0}, 10))
	require.True(t, isLockExpired(&Lock{Ts: 5, TTL: 5}, 10))
}

func TestCommitTsExpired(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("expired")

	prewriteReq := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("v1"),
		}},
		PrimaryLock:  key,
		StartVersion: 10,
		LockTtl:      1000,
		// TODO: Manually set a large MinCommitTs to simulate that a concurrent
		// reader has pushed the lock's min_commit_ts.
		// This hardcoded value should be removed once the Read
		// path for pushing min_commit_ts is implemented.
		MinCommitTs: 30,
	}
	require.Empty(t, Prewrite(db, latches, prewriteReq))

	commitReq := &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  10,
		CommitVersion: 20,
	}
	err := Commit(db, latches, commitReq)

	require.NotNil(t, err)
	require.NotNil(t, err.GetCommitTsExpired())
	require.Equal(t, uint64(20), err.GetCommitTsExpired().CommitTs)
	require.Equal(t, uint64(30), err.GetCommitTsExpired().MinCommitTs)

	// Mock retry commit with new commit ts
	commitReq = &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  10,
		CommitVersion: 30,
	}
	require.Nil(t, Commit(db, latches, commitReq))
}
