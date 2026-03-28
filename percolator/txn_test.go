package percolator

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/percolator/latch"
	"github.com/feichai0017/NoKV/utils"
)

func testOptionsForDir(dir string) *NoKV.Options {
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = dir
	opt.MemTableSize = 1 << 12
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = utils.DefaultValueThreshold
	return opt
}

func openTestDB(t *testing.T) *NoKV.DB {
	opt := testOptionsForDir(filepath.Join(t.TempDir(), "db"))
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func applyVersionedEntryForTxnTest(t *testing.T, db *NoKV.DB, cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := kv.NewInternalEntry(cf, key, version, kv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))
}

func latestWALPath(t *testing.T, dir string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, files)
	sort.Strings(files)
	return files[len(files)-1]
}

func truncateTail(t *testing.T, path string, trim int64) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Greater(t, info.Size(), trim)
	require.NoError(t, os.Truncate(path, info.Size()-trim))
}

type rollbackTestStore struct {
	applyInternalEntries func(entries []*kv.Entry) error
	getInternalEntry     func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
	newInternalIterator  func(opt *utils.Options) utils.Iterator
}

func (s rollbackTestStore) ApplyInternalEntries(entries []*kv.Entry) error {
	if s.applyInternalEntries != nil {
		return s.applyInternalEntries(entries)
	}
	return nil
}

func (s rollbackTestStore) GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
	if s.getInternalEntry != nil {
		return s.getInternalEntry(cf, key, version)
	}
	return nil, utils.ErrKeyNotFound
}

func (s rollbackTestStore) NewInternalIterator(opt *utils.Options) utils.Iterator {
	if s.newInternalIterator != nil {
		return s.newInternalIterator(opt)
	}
	return &testIterator{}
}

type testIterator struct {
	items []utils.Item
	idx   int
}

func (it *testIterator) Next() {
	it.idx++
}

func (it *testIterator) Valid() bool {
	return it != nil && it.idx < len(it.items)
}

func (it *testIterator) Rewind() {
	it.idx = 0
}

func (it *testIterator) Item() utils.Item {
	if !it.Valid() {
		return nil
	}
	return it.items[it.idx]
}

func (it *testIterator) Close() error {
	return nil
}

func (it *testIterator) Seek(key []byte) {
	it.idx = 0
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

	val, _, err := reader.GetValue([]byte("k1"), 30)
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

// TestCommitNilRequestReturnsNil verifies Commit ignores nil requests.
func TestCommitNilRequestReturnsNil(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	require.Nil(t, Commit(db, latches, nil))
}

// TestCommitRejectsEmptyKey verifies Commit rejects empty keys.
func TestCommitRejectsEmptyKey(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)

	err := Commit(db, latches, &pb.CommitRequest{
		Keys:          [][]byte{nil},
		StartVersion:  3,
		CommitVersion: 6,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetAbort(), "empty key in commit")
}

// TestCommitRejectsCommitVersionEarlierThanStartVersion preserves MVCC ordering.
func TestCommitRejectsCommitVersionEarlierThanStartVersion(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("mvcc-order")
	startTs := uint64(20)
	commitTs := uint64(10)
	readTs := uint64(15)

	prewrite := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      3000,
	}
	require.Empty(t, Prewrite(db, latches, prewrite))

	keyErr := Commit(db, latches, &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	if keyErr == nil {
		t.Errorf("Commit accepted commitVersion=%d earlier than startVersion=%d", commitTs, startTs)
	}

	reader := NewReader(db)
	val, _, err := reader.GetValue(key, readTs)
	if !errors.Is(err, utils.ErrKeyNotFound) {
		t.Fatalf("read at ts=%d unexpectedly observed value %q, err=%v", readTs, val, err)
	}
}

// TestCommitMissingLockWithRollbackWriteAborts rejects commits after rollback.
func TestCommitMissingLockWithRollbackWriteAborts(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("rolled-back")
	startTs := uint64(18)

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, startTs, EncodeWrite(Write{
		Kind:    pb.Mutation_Rollback,
		StartTs: startTs,
	}), 0)

	err := Commit(db, latches, &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: 30,
	})
	require.NotNil(t, err)
	require.NotEmpty(t, err.GetAbort())
}

// TestCommitReturnsRetryableOnLockLookupError surfaces lock read failures.
func TestCommitReturnsRetryableOnLockLookupError(t *testing.T) {
	latches := latch.NewManager(32)
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
			return nil, errors.New("lock lookup failed")
		},
	}

	err := Commit(store, latches, &pb.CommitRequest{
		Keys:          [][]byte{[]byte("retry-lock")},
		StartVersion:  9,
		CommitVersion: 18,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "lock lookup failed")
}

// TestCommitReturnsRetryableOnWriteLookupErrorWhenLockMissing surfaces write scan failures.
func TestCommitReturnsRetryableOnWriteLookupErrorWhenLockMissing(t *testing.T) {
	badEntry := kv.NewEntry([]byte("bad-key"), nil)
	t.Cleanup(badEntry.DecrRef)

	latches := latch.NewManager(32)
	store := rollbackTestStore{
		newInternalIterator: func(opt *utils.Options) utils.Iterator {
			return &testIterator{items: []utils.Item{badEntry}}
		},
	}

	err := Commit(store, latches, &pb.CommitRequest{
		Keys:          [][]byte{[]byte("retry-write")},
		StartVersion:  9,
		CommitVersion: 18,
	})
	require.NotNil(t, err)
	require.Contains(t, err.GetRetryable(), "scanWrites expects internal key")
}

// TestCommitMissingLockWithExistingWriteIsIdempotent preserves idempotent commits.
func TestCommitMissingLockWithExistingWriteIsIdempotent(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("already-committed")
	startTs := uint64(12)
	commitTs := uint64(22)

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, commitTs, EncodeWrite(Write{
		Kind:    pb.Mutation_Put,
		StartTs: startTs,
	}), 0)

	err := Commit(db, latches, &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.Nil(t, err)
}

// TestCommitReturnsLockedOnDifferentTransactionLock rejects foreign locks.
func TestCommitReturnsLockedOnDifferentTransactionLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(32)
	key := []byte("locked-by-other")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, EncodeLock(Lock{
		Primary: key,
		Ts:      30,
		Kind:    pb.Mutation_Put,
	}), 0)

	err := Commit(db, latches, &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  20,
		CommitVersion: 40,
	})
	require.NotNil(t, err)
	require.NotNil(t, err.GetLocked())
	require.Equal(t, uint64(30), err.GetLocked().GetLockVersion())
}

func TestReaderMostRecentWriteSkipsOtherCF(t *testing.T) {
	db := openTestDB(t)
	require.NoError(t, db.Set([]byte("b"), []byte("vb")))

	entry := EncodeWrite(Write{Kind: pb.Mutation_Put, StartTs: 1})
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, []byte("a"), 10, entry, 0)

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

func TestBatchRollbackDoesNotDeleteOtherTransactionLock(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("rk-other-lock")

	prewrite := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: 20,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, prewrite))

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(20), lock.Ts)

	rollback := &pb.BatchRollbackRequest{Keys: [][]byte{key}, StartVersion: 10}
	require.Nil(t, BatchRollback(db, latches, rollback))

	lock, err = reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, uint64(20), lock.Ts)

	write, commitTs, err := reader.GetWriteByStartTs(key, 10)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, pb.Mutation_Rollback, write.Kind)
	require.Equal(t, uint64(10), commitTs)

	commit := &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  20,
		CommitVersion: 30,
	}
	require.Nil(t, Commit(db, latches, commit))

	value, _, err := reader.GetValue(key, 40)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), value)
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
	val, _, err := reader.GetValue([]byte("res"), 60)
	require.NoError(t, err)
	require.Equal(t, []byte("val"), val)
}

// TestResolveLockNilRequest verifies ResolveLock ignores nil requests.
func TestResolveLockNilRequest(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)

	var (
		count  uint64
		keyErr *pb.KeyError
	)
	require.NotPanics(t, func() {
		count, keyErr = ResolveLock(db, latches, nil)
	})
	require.Zero(t, count)
	require.Nil(t, keyErr)
}

// TestResolveLockSkipsEmptyAndMismatchedKeys ignores non-matching locks.
func TestResolveLockSkipsEmptyAndMismatchedKeys(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("other-lock")

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, EncodeLock(Lock{
		Primary: key,
		Ts:      55,
		Kind:    pb.Mutation_Put,
	}), 0)

	count, keyErr := ResolveLock(db, latches, &pb.ResolveLockRequest{
		Keys:          [][]byte{nil, key},
		StartVersion:  40,
		CommitVersion: 60,
	})
	require.Zero(t, count)
	require.Nil(t, keyErr)
}

// TestResolveLockReturnsRetryableOnLockLookupError surfaces lock read failures.
func TestResolveLockReturnsRetryableOnLockLookupError(t *testing.T) {
	latches := latch.NewManager(16)
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
			return nil, errors.New("lock lookup failed")
		},
	}

	count, keyErr := ResolveLock(store, latches, &pb.ResolveLockRequest{
		Keys:          [][]byte{[]byte("retry-lock")},
		StartVersion:  40,
		CommitVersion: 50,
	})
	require.Zero(t, count)
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetRetryable(), "lock lookup failed")
}

// TestResolveLockRollback keeps commitVersion==0 on the rollback path.
func TestResolveLockRollback(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("resolve-rollback")
	startTs := uint64(40)

	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("val"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	count, keyErr := ResolveLock(db, latches, &pb.ResolveLockRequest{
		Keys:         [][]byte{key},
		StartVersion: startTs,
	})
	require.Equal(t, uint64(1), count)
	require.Nil(t, keyErr)

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock)

	write, commitTs, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.NotNil(t, write)
	require.Equal(t, pb.Mutation_Rollback, write.Kind)
	require.Equal(t, startTs, commitTs)
}

// TestResolveLockRollbackReturnsError propagates rollback apply failures.
func TestResolveLockRollbackReturnsError(t *testing.T) {
	key := []byte("resolve-rollback-error")
	startTs := uint64(40)
	latches := latch.NewManager(16)
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, gotKey []byte, version uint64) (*kv.Entry, error) {
			if cf == kv.CFLock && string(gotKey) == string(key) && version == lockColumnTs {
				entry := kv.NewInternalEntry(cf, gotKey, version, EncodeLock(Lock{
					Primary: key,
					Ts:      startTs,
					Kind:    pb.Mutation_Put,
				}), 0, 0)
				return entry, nil
			}
			return nil, utils.ErrKeyNotFound
		},
		applyInternalEntries: func(entries []*kv.Entry) error {
			return errors.New("apply failed")
		},
	}

	count, keyErr := ResolveLock(store, latches, &pb.ResolveLockRequest{
		Keys:         [][]byte{key},
		StartVersion: startTs,
	})
	require.Zero(t, count)
	require.NotNil(t, keyErr)
	require.Contains(t, keyErr.GetRetryable(), "apply failed")
}

// TestResolveLockRejectsCommitVersionEarlierThanStartVersion preserves MVCC ordering.
func TestResolveLockRejectsCommitVersionEarlierThanStartVersion(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("res-order")
	startTs := uint64(40)
	commitTs := uint64(30)
	readTs := uint64(35)

	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("val"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	count, keyErr := ResolveLock(db, latches, &pb.ResolveLockRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	if keyErr == nil {
		t.Errorf("ResolveLock accepted commitVersion=%d earlier than startVersion=%d", commitTs, startTs)
	}
	if count != 0 {
		t.Errorf("ResolveLock resolved %d locks with invalid commitVersion=%d", count, commitTs)
	}

	reader := NewReader(db)
	val, _, err := reader.GetValue(key, readTs)
	if !errors.Is(err, utils.ErrKeyNotFound) {
		t.Fatalf("read at ts=%d unexpectedly observed value %q, err=%v", readTs, val, err)
	}
}

func TestResolveLockCommitTsExpired(t *testing.T) {
	db := openTestDB(t)
	latches := latch.NewManager(16)
	key := []byte("res-expired")
	startTs := uint64(40)
	minCommitTs := uint64(60)
	commitTs := uint64(50)

	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("val"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
		MinCommitTs:  minCommitTs,
	}
	require.Empty(t, Prewrite(db, latches, pre))

	count, keyErr := ResolveLock(db, latches, &pb.ResolveLockRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	})
	require.Equal(t, uint64(0), count)
	require.NotNil(t, keyErr)
	require.NotNil(t, keyErr.GetCommitTsExpired())
	require.Equal(t, commitTs, keyErr.GetCommitTsExpired().GetCommitTs())
	require.Equal(t, minCommitTs, keyErr.GetCommitTsExpired().GetMinCommitTs())

	reader := NewReader(db)
	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	write, _, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.Nil(t, write)
}

func TestPrewriteRecoveryDropsCorruptedBatch(t *testing.T) {
	workDir := filepath.Join(t.TempDir(), "db")
	db, err := NoKV.Open(testOptionsForDir(workDir))
	require.NoError(t, err)
	latches := latch.NewManager(16)
	key := []byte("prewrite-corrupt")
	startTs := uint64(101)

	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))
	require.NoError(t, db.WAL().Sync())
	require.NoError(t, db.Close())

	walPath := latestWALPath(t, workDir)
	truncateTail(t, walPath, 2)

	db2, err := NoKV.Open(testOptionsForDir(workDir))
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	reader := NewReader(db2)

	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.Nil(t, lock)

	_, err = db2.GetInternalEntry(kv.CFDefault, key, startTs)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestCommitRecoveryDropsCorruptedCommitBatch(t *testing.T) {
	workDir := filepath.Join(t.TempDir(), "db")
	db, err := NoKV.Open(testOptionsForDir(workDir))
	require.NoError(t, err)
	latches := latch.NewManager(16)
	key := []byte("commit-corrupt")
	startTs := uint64(201)
	commitTs := uint64(301)

	pre := &pb.PrewriteRequest{
		Mutations: []*pb.Mutation{{
			Op:    pb.Mutation_Put,
			Key:   key,
			Value: []byte("value"),
		}},
		PrimaryLock:  key,
		StartVersion: startTs,
		LockTtl:      1000,
	}
	require.Empty(t, Prewrite(db, latches, pre))
	require.NoError(t, db.WAL().Sync())

	require.Nil(t, Commit(db, latches, &pb.CommitRequest{
		Keys:          [][]byte{key},
		StartVersion:  startTs,
		CommitVersion: commitTs,
	}))
	require.NoError(t, db.WAL().Sync())
	require.NoError(t, db.Close())

	walPath := latestWALPath(t, workDir)
	truncateTail(t, walPath, 2)

	db2, err := NoKV.Open(testOptionsForDir(workDir))
	require.NoError(t, err)
	defer func() { _ = db2.Close() }()
	reader := NewReader(db2)

	lock, err := reader.GetLock(key)
	require.NoError(t, err)
	require.NotNil(t, lock)
	require.Equal(t, startTs, lock.Ts)

	write, _, err := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, err)
	require.Nil(t, write)

	entry, err := db2.GetInternalEntry(kv.CFDefault, key, startTs)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), entry.Value)
	entry.DecrRef()
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
		ExpiresAt:  12345,
	}
	encoded := EncodeWrite(write)
	got, err := DecodeWrite(encoded)
	require.NoError(t, err)
	require.Equal(t, write.Kind, got.Kind)
	require.Equal(t, write.StartTs, got.StartTs)
	require.Equal(t, write.ShortValue, got.ShortValue)
	require.Equal(t, write.ExpiresAt, got.ExpiresAt)
}

func TestDecodeWriteBackwardCompatibleWithoutExpiresAt(t *testing.T) {
	// Old format: version, kind, startTs, hasShort, shortLen, shortValue.
	raw := make([]byte, 0, 32)
	raw = append(raw, writeCodecVersion, byte(pb.Mutation_Put))
	raw = binary.AppendUvarint(raw, 7)
	raw = append(raw, 1)
	raw = binary.AppendUvarint(raw, 5)
	raw = append(raw, []byte("short")...)

	got, err := DecodeWrite(raw)
	require.NoError(t, err)
	require.Equal(t, pb.Mutation_Put, got.Kind)
	require.Equal(t, uint64(7), got.StartTs)
	require.Equal(t, []byte("short"), got.ShortValue)
	require.Equal(t, uint64(0), got.ExpiresAt)
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

	// hasShort=0 with trailing truncated expires_at varint.
	_, err = DecodeWrite([]byte{writeCodecVersion, byte(pb.Mutation_Put), 0x01, 0x00, 0x80})
	require.Error(t, err)
}

func TestReaderGetValueFromShortValueWithExpiresAt(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("short-value")
	write := Write{
		Kind:       pb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("v-short"),
		ExpiresAt:  ^uint64(0),
	}
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 20, EncodeWrite(write), 0)

	val, expiresAt, err := reader.GetValue(key, 30)
	require.NoError(t, err)
	require.Equal(t, []byte("v-short"), val)
	require.Equal(t, write.ExpiresAt, expiresAt)
}

func TestReaderGetValueFromExpiredShortValue(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("short-expired")
	write := Write{
		Kind:       pb.Mutation_Put,
		StartTs:    11,
		ShortValue: []byte("v-short"),
		ExpiresAt:  1, // definitely expired
	}
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 20, EncodeWrite(write), 0)

	_, _, err := reader.GetValue(key, 30)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
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
	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 15, rollback, 0)

	err := commitKey(db, reader, key, lock, 20)
	require.NotNil(t, err)
	require.NotNil(t, err.GetAbort())
}

func TestCommitKeyWritesAndCleansLock(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("commit")
	lock := &Lock{Primary: key, Ts: 11, Kind: pb.Mutation_Put}

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, EncodeLock(*lock), 0)
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

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 30, EncodeWrite(Write{Kind: lock.Kind, StartTs: lock.Ts}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, EncodeLock(*lock), 0)

	commitErr := commitKey(db, reader, key, lock, 40)
	require.Nil(t, commitErr)
}

func TestRollbackKeyCreatesRollbackWrite(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb2")
	startTs := uint64(17)

	applyVersionedEntryForTxnTest(t, db, kv.CFLock, key, lockColumnTs, EncodeLock(Lock{
		Primary: key,
		Ts:      startTs,
		Kind:    pb.Mutation_Put,
	}), 0)
	applyVersionedEntryForTxnTest(t, db, kv.CFDefault, key, startTs, []byte("val"), 0)

	rollbackErr := rollbackKey(db, reader, key, startTs)
	require.Nil(t, rollbackErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, pb.Mutation_Rollback, write.Kind)
	require.Equal(t, startTs, commitTs)
}

func TestRollbackKeyReturnsNilWhenWriteAlreadyExists(t *testing.T) {
	db := openTestDB(t)
	reader := NewReader(db)
	key := []byte("rb-existing")
	startTs := uint64(17)

	applyVersionedEntryForTxnTest(t, db, kv.CFWrite, key, 25, EncodeWrite(Write{
		Kind:    pb.Mutation_Put,
		StartTs: startTs,
	}), 0)

	rollbackErr := rollbackKey(db, reader, key, startTs)
	require.Nil(t, rollbackErr)

	write, commitTs, readErr := reader.GetWriteByStartTs(key, startTs)
	require.NoError(t, readErr)
	require.NotNil(t, write)
	require.Equal(t, pb.Mutation_Put, write.Kind)
	require.Equal(t, uint64(25), commitTs)
}

func TestRollbackKeyReturnsRetryableWhenWriteLookupFails(t *testing.T) {
	badEntry := kv.NewEntry([]byte("bad-key"), nil)
	t.Cleanup(badEntry.DecrRef)

	store := rollbackTestStore{
		newInternalIterator: func(opt *utils.Options) utils.Iterator {
			return &testIterator{items: []utils.Item{badEntry}}
		},
	}

	rollbackErr := rollbackKey(store, NewReader(store), []byte("rb-write-err"), 9)
	require.NotNil(t, rollbackErr)
	require.Contains(t, rollbackErr.GetRetryable(), "scanWrites expects internal key")
}

func TestRollbackKeyReturnsRetryableWhenLockLookupFails(t *testing.T) {
	store := rollbackTestStore{
		getInternalEntry: func(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error) {
			return nil, errors.New("lock lookup failed")
		},
	}

	rollbackErr := rollbackKey(store, NewReader(store), []byte("rb-lock-err"), 9)
	require.NotNil(t, rollbackErr)
	require.Contains(t, rollbackErr.GetRetryable(), "lock lookup failed")
}

func TestRollbackKeyReturnsRetryableWhenApplyFails(t *testing.T) {
	store := rollbackTestStore{
		applyInternalEntries: func(entries []*kv.Entry) error {
			return errors.New("apply failed")
		},
	}

	rollbackErr := rollbackKey(store, NewReader(store), []byte("rb-apply-err"), 9)
	require.NotNil(t, rollbackErr)
	require.Contains(t, rollbackErr.GetRetryable(), "apply failed")
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
