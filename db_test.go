package NoKV

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	dbruntime "github.com/feichai0017/NoKV/internal/runtime"
	myraft "github.com/feichai0017/NoKV/raft"
	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/raftlog"
	"github.com/feichai0017/NoKV/thermos"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	// Write entries.
	for i := range 50 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		ttl := 1000 * time.Second
		if err := db.SetWithTTL([]byte(key), []byte(val), ttl); err != nil {
			t.Fatal(err)
		}
		// Read back.
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}

	for i := range 40 {
		key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		if err := db.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Iterator scan.
	iter := db.NewIterator(&index.Options{
		Prefix: []byte("hello"),
		IsAsc:  false,
	})
	defer func() { _ = iter.Close() }()
	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}
	t.Logf("db.Stats.Entries=%+v", db.Info().Snapshot().Entries)
	// Delete.
	if err := db.Del([]byte("hello")); err != nil {
		t.Fatal(err)
	}

	for i := range 10 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		ttl := 1000 * time.Second
		if err := db.SetWithTTL([]byte(key), []byte(val), ttl); err != nil {
			t.Fatal(err)
		}
		// Read back.
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
		}
	}
}

func openTestDB(t testing.TB, opt *Options) *DB {
	t.Helper()
	// Tests written before the EnableValueLog opt-in flag landed assumed
	// vlog was on by default. Auto-enable when the test fixture clearly
	// expects vlog to exist (explicit ValueLogFileSize, explicit "all
	// values to vlog" via ValueThreshold=0, or BucketCount > 0). New
	// tests that want metadata-only behavior must construct Options
	// without setting any of these fields and leave EnableValueLog at
	// its default false.
	if opt != nil && !opt.EnableValueLog {
		if opt.ValueLogFileSize > 0 || opt.ValueThreshold == 0 || opt.ValueLogBucketCount > 0 {
			opt.EnableValueLog = true
		}
	}
	db, err := Open(opt)
	require.NoError(t, err)
	return db
}

func TestColumnFamilies(t *testing.T) {
	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("user-key")
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, key, nonTxnMaxVersion, []byte("default"), 0, 0),
		kv.NewInternalEntry(kv.CFLock, key, nonTxnMaxVersion, []byte("lock"), 0, 0),
		kv.NewInternalEntry(kv.CFWrite, key, nonTxnMaxVersion, []byte("write"), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, db.ApplyInternalEntries(entries))

	e, err := db.GetInternalEntry(kv.CFDefault, key, nonTxnMaxVersion)
	require.NoError(t, err)
	gotCF, _, _, _ := kv.SplitInternalKey(e.Key)
	require.Equal(t, kv.CFDefault, gotCF)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()

	e, err = db.GetInternalEntry(kv.CFLock, key, nonTxnMaxVersion)
	require.NoError(t, err)
	gotCF, _, _, _ = kv.SplitInternalKey(e.Key)
	require.Equal(t, kv.CFLock, gotCF)
	require.Equal(t, []byte("lock"), e.Value)
	e.DecrRef()

	e, err = db.GetInternalEntry(kv.CFWrite, key, nonTxnMaxVersion)
	require.NoError(t, err)
	gotCF, _, _, _ = kv.SplitInternalKey(e.Key)
	require.Equal(t, kv.CFWrite, gotCF)
	require.Equal(t, []byte("write"), e.Value)
	e.DecrRef()

	// Default Get should read default CF.
	e, err = db.Get(key)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, e.CF)
	require.Equal(t, []byte("default"), e.Value)

	lockDelete := kv.NewInternalEntry(kv.CFLock, key, nonTxnMaxVersion, nil, kv.BitDelete, 0)
	defer lockDelete.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{lockDelete}))
	lock, err := db.GetInternalEntry(kv.CFLock, key, nonTxnMaxVersion)
	require.NoError(t, err)
	require.True(t, lock.Meta&kv.BitDelete > 0)
	lock.DecrRef()
	// Default CF should remain untouched.
	e, err = db.GetInternalEntry(kv.CFDefault, key, nonTxnMaxVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()
}

func TestSetBatch(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.SetBatch([]BatchSetItem{
		{Key: []byte("k1"), Value: []byte("v1")},
		{Key: []byte("k2"), Value: []byte("v2")},
	}))

	e, err := db.Get([]byte("k1"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), e.Value)

	e, err = db.Get([]byte("k2"))
	require.NoError(t, err)
	require.Equal(t, []byte("v2"), e.Value)
}

func TestSetBatchValidation(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NoError(t, db.SetBatch(nil))
	require.Equal(t, utils.ErrEmptyKey, db.SetBatch([]BatchSetItem{
		{Key: nil, Value: []byte("v")},
	}))
	require.Equal(t, utils.ErrNilValue, db.SetBatch([]BatchSetItem{
		{Key: []byte("k"), Value: nil},
	}))
}

func TestOpenNormalizesLegacyUnsetFieldsWithoutMutatingCaller(t *testing.T) {
	opt := newTestOptions(t)
	opt.WriteBatchMaxCount = 0
	opt.WriteBatchMaxSize = 0
	opt.MaxBatchCount = 0
	opt.MaxBatchSize = 0
	opt.WriteThrottleMinRate = 0
	opt.WriteThrottleMaxRate = 0
	opt.WALBufferSize = 0
	opt.NumCompactors = 0
	opt.NumLevelZeroTables = 0
	opt.L0SlowdownWritesTrigger = 0
	opt.L0StopWritesTrigger = 0
	opt.L0ResumeWritesTrigger = 0
	opt.CompactionSlowdownTrigger = 0
	opt.CompactionStopTrigger = 0
	opt.CompactionResumeTrigger = 0
	opt.IngestCompactBatchSize = 0
	opt.IngestBacklogMergeScore = 0
	opt.IngestShardParallelism = 0
	opt.CompactionValueWeight = 0
	opt.CompactionValueAlertThreshold = 0
	opt.ThermosTopK = 0

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.Zero(t, opt.WriteBatchMaxCount)
	require.Zero(t, opt.WriteThrottleMinRate)
	require.Zero(t, opt.NumCompactors)
	require.Zero(t, opt.L0StopWritesTrigger)
	require.Zero(t, opt.CompactionStopTrigger)
	require.Zero(t, opt.ThermosTopK)

	require.Greater(t, db.opt.WriteBatchMaxCount, 0)
	require.Greater(t, db.opt.WriteBatchMaxSize, int64(0))
	require.Greater(t, db.opt.MaxBatchCount, int64(0))
	require.Greater(t, db.opt.MaxBatchSize, int64(0))
	require.Greater(t, db.opt.WriteThrottleMinRate, int64(0))
	require.GreaterOrEqual(t, db.opt.WriteThrottleMaxRate, db.opt.WriteThrottleMinRate)
	require.Greater(t, db.opt.WALBufferSize, 0)
	require.Greater(t, db.opt.NumCompactors, 0)
	require.Greater(t, db.opt.NumLevelZeroTables, 0)
	require.Greater(t, db.opt.L0SlowdownWritesTrigger, 0)
	require.Greater(t, db.opt.L0StopWritesTrigger, db.opt.L0SlowdownWritesTrigger)
	require.Less(t, db.opt.L0ResumeWritesTrigger, db.opt.L0SlowdownWritesTrigger)
	require.Greater(t, db.opt.CompactionSlowdownTrigger, 0.0)
	require.GreaterOrEqual(t, db.opt.CompactionStopTrigger, db.opt.CompactionSlowdownTrigger)
	require.LessOrEqual(t, db.opt.CompactionResumeTrigger, db.opt.CompactionSlowdownTrigger)
	require.Greater(t, db.opt.IngestCompactBatchSize, 0)
	require.Greater(t, db.opt.IngestBacklogMergeScore, 0.0)
	require.Greater(t, db.opt.IngestShardParallelism, 0)
	require.Greater(t, db.opt.CompactionValueWeight, 0.0)
	require.Greater(t, db.opt.CompactionTombstoneWeight, 0.0)
	require.Greater(t, db.opt.CompactionValueAlertThreshold, 0.0)
	require.Greater(t, db.opt.ThermosTopK, 0)
}

func TestNewDefaultOptionsExposeConcreteCompactionDefaults(t *testing.T) {
	opt := NewDefaultOptions()

	require.Greater(t, opt.NumLevelZeroTables, 0)
	require.Greater(t, opt.L0SlowdownWritesTrigger, 0)
	require.Greater(t, opt.L0StopWritesTrigger, opt.L0SlowdownWritesTrigger)
	require.Less(t, opt.L0ResumeWritesTrigger, opt.L0SlowdownWritesTrigger)
	require.Greater(t, opt.CompactionSlowdownTrigger, 0.0)
	require.GreaterOrEqual(t, opt.CompactionStopTrigger, opt.CompactionSlowdownTrigger)
	require.LessOrEqual(t, opt.CompactionResumeTrigger, opt.CompactionSlowdownTrigger)
	require.Greater(t, opt.IngestCompactBatchSize, 0)
	require.Greater(t, opt.IngestBacklogMergeScore, 0.0)
	require.NotNil(t, opt.PrefixExtractor)
	require.Greater(t, opt.CompactionTombstoneWeight, 0.0)
}

func TestNewDefaultOptionsExposeConcreteBatchDefaults(t *testing.T) {
	opt := NewDefaultOptions()

	require.Greater(t, opt.WriteBatchMaxCount, 0)
	require.Greater(t, opt.WriteBatchMaxSize, int64(0))
	require.Equal(t, int64(opt.WriteBatchMaxCount), opt.MaxBatchCount)
	require.Equal(t, opt.WriteBatchMaxSize, opt.MaxBatchSize)
	require.Greater(t, opt.WriteThrottleMinRate, int64(0))
	require.GreaterOrEqual(t, opt.WriteThrottleMaxRate, opt.WriteThrottleMinRate)
	require.Greater(t, opt.WALBufferSize, 0)
}

func newTestOptions(t *testing.T) *Options {
	t.Helper()
	opt := NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.MemTableSize = 1 << 20
	opt.SSTableMaxSz = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.ValueThreshold = 1 << 20
	opt.DetectConflicts = true
	return opt
}

func applyVersionedEntryForTest(t *testing.T, db *DB, cf kv.ColumnFamily, key []byte, version uint64, value []byte, meta byte) {
	t.Helper()
	entry := kv.NewInternalEntry(cf, key, version, kv.SafeCopy(nil, value), meta, 0)
	defer entry.DecrRef()
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{entry}))
}

func TestVersionedEntryRoundTrip(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-key")
	version := uint64(42)
	value := []byte("value-42")

	applyVersionedEntryForTest(t, db, kv.CFDefault, key, version, value, 0)

	entry, err := db.GetInternalEntry(kv.CFDefault, key, version)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, entry.CF)
	require.Equal(t, version, entry.Version)
	_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, key, userKey)
	require.Equal(t, version, kv.Timestamp(entry.Key))
	require.Equal(t, value, entry.Value)
	entry.DecrRef()
}

func TestGetInternalEntryPopulatesInternalFieldsFromHitVersion(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-hit")
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 1, []byte("v1"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 3, []byte("v3"), 0)

	entry, err := db.GetInternalEntry(kv.CFDefault, key, 2)
	require.NoError(t, err)
	defer entry.DecrRef()

	cf, userKey, ts, ok := kv.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, kv.CFDefault, cf)
	require.Equal(t, key, userKey)
	require.Equal(t, uint64(1), ts)
	require.Equal(t, cf, entry.CF)
	require.Equal(t, ts, entry.Version)
	require.Equal(t, []byte("v1"), entry.Value)
}

func TestVersionedEntryDeleteTombstone(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-delete")
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 1, []byte("v1"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, key, 2, nil, kv.BitDelete)

	entry, err := db.GetInternalEntry(kv.CFDefault, key, 2)
	require.NoError(t, err)
	_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, key, userKey)
	require.Equal(t, uint64(2), kv.Timestamp(entry.Key))
	require.True(t, entry.Meta&kv.BitDelete > 0)
	entry.DecrRef()

	entry, err = db.GetInternalEntry(kv.CFDefault, key, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), entry.Value)
	require.Equal(t, uint64(1), kv.Timestamp(entry.Key))
	entry.DecrRef()
}

func TestApplyEntriesWritesBatch(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("batch-key")
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, kv.SafeCopy(nil, key), 11, []byte("value"), 0, 0),
		kv.NewInternalEntry(kv.CFLock, kv.SafeCopy(nil, key), kv.MaxVersion, []byte("lock"), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}

	require.NoError(t, db.ApplyInternalEntries(entries))

	valueEntry, err := db.GetInternalEntry(kv.CFDefault, key, 11)
	require.NoError(t, err)
	require.Equal(t, []byte("value"), valueEntry.Value)
	valueEntry.DecrRef()

	lockEntry, err := db.GetInternalEntry(kv.CFLock, key, kv.MaxVersion)
	require.NoError(t, err)
	require.Equal(t, []byte("lock"), lockEntry.Value)
	lockEntry.DecrRef()
}

func TestApplyEntriesRejectsEmptyKey(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kv.NewEntry(nil, []byte("value"))
	defer entry.DecrRef()
	entry.Key = nil

	err := db.ApplyInternalEntries([]*kv.Entry{entry})
	require.ErrorIs(t, err, utils.ErrEmptyKey)
}

func TestApplyEntriesRejectsNonInternalKey(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kv.NewEntry([]byte("plain-user-key"), []byte("value"))
	defer entry.DecrRef()

	err := db.ApplyInternalEntries([]*kv.Entry{entry})
	require.ErrorIs(t, err, utils.ErrInvalidRequest)
}

func TestSetRejectsNilValueAndAllowsEmptyValue(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	nilKey := []byte("nil-value")
	err := db.Set(nilKey, nil)
	require.ErrorIs(t, err, utils.ErrNilValue)
	_, err = db.Get(nilKey)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	nilTTLKey := []byte("nil-value-ttl")
	err = db.SetWithTTL(nilTTLKey, nil, time.Second)
	require.ErrorIs(t, err, utils.ErrNilValue)
	_, err = db.Get(nilTTLKey)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	emptyKey := []byte("empty-value")
	require.NoError(t, db.Set(emptyKey, []byte{}))
	entry, err := db.Get(emptyKey)
	require.NoError(t, err)
	require.Len(t, entry.Value, 0)
	require.Equal(t, byte(0), entry.Meta&kv.BitDelete)

	emptyTTLKey := []byte("empty-value-ttl")
	require.NoError(t, db.SetWithTTL(emptyTTLKey, []byte{}, time.Second))
	entry, err = db.Get(emptyTTLKey)
	require.NoError(t, err)
	require.Len(t, entry.Value, 0)
	require.Equal(t, byte(0), entry.Meta&kv.BitDelete)
}

func TestSetAfterCloseDoesNotPanic(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())

	var err error
	require.NotPanics(t, func() {
		err = db.Set([]byte("k"), []byte("v"))
	})
	require.ErrorIs(t, err, utils.ErrBlockedWrites)
}

func TestApplyEntriesAfterCloseDoesNotPanicAndCallerCanRelease(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("k"), 1, []byte("v"), 0, 0)
	var err error
	require.NotPanics(t, func() {
		err = db.ApplyInternalEntries([]*kv.Entry{entry})
		entry.DecrRef()
	})
	require.ErrorIs(t, err, utils.ErrBlockedWrites)
}

func TestApplyEntriesErrTxnTooBigDoesNotPanicAndCallerCanRelease(t *testing.T) {
	opt := newTestOptions(t)
	opt.MaxBatchCount = 1
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("k"), 1, []byte("v"), 0, 0)
	var err error
	require.NotPanics(t, func() {
		err = db.ApplyInternalEntries([]*kv.Entry{entry})
		entry.DecrRef()
	})
	require.ErrorIs(t, err, utils.ErrTxnTooBig)
}

func TestGetEntryIsDetachedFromPool(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("detached-key")
	require.NoError(t, db.Set(key, []byte("value-1")))

	entry, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), entry.Value)

	// Public read APIs return detached entries; DecrRef misuse should fail fast.
	require.Panics(t, func() {
		entry.DecrRef()
	})
	require.Equal(t, []byte("value-1"), entry.Value)

	entry.Value[0] = 'X'
	again, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), again.Value)
}

func TestGetValueLogEntryIsDetached(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value []byte
	}{
		{name: "small", value: bytes.Repeat([]byte("s"), 64)},
		{name: "large", value: bytes.Repeat([]byte("l"), valueLogSmallCopyThreshold+512)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			opt := newTestOptions(t)
			opt.ValueThreshold = 0
			db := openTestDB(t, opt)
			defer func() { _ = db.Close() }()

			key := []byte("vlog-detached-" + tc.name)
			require.NoError(t, db.Set(key, tc.value))

			entry, err := db.Get(key)
			require.NoError(t, err)
			require.Equal(t, tc.value, entry.Value)
			require.Zero(t, entry.Meta&kv.BitValuePointer)

			entry.Value[0] ^= 0x1
			again, err := db.Get(key)
			require.NoError(t, err)
			require.Equal(t, tc.value, again.Value)
		})
	}
}

func TestDBIteratorSeekAndValueCopy(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		require.NoError(t, db.Set([]byte("a"), []byte("va")))
		require.NoError(t, db.Set([]byte("b"), []byte("vb")))
		require.NoError(t, db.Set([]byte("c"), []byte("vc")))

		it := db.NewIterator(&index.Options{IsAsc: true})
		defer func() { _ = it.Close() }()
		it.Seek([]byte("b"))
		require.True(t, it.Valid())
		item := it.Item()
		require.Equal(t, []byte("b"), item.Entry().Key)
		val, err := item.(*Item).ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, []byte("vb"), val)
	})

	t.Run("value-pointer", func(t *testing.T) {
		opt := newTestOptions(t)
		opt.ValueThreshold = 0
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		value := bytes.Repeat([]byte("p"), 64)
		require.NoError(t, db.Set([]byte("k"), value))

		it := db.NewIterator(&index.Options{IsAsc: true, OnlyUseKey: true})
		defer func() { _ = it.Close() }()
		it.Seek([]byte("k"))
		require.True(t, it.Valid())
		item := it.Item()
		require.True(t, kv.IsValuePtr(item.Entry()))
		val, err := item.(*Item).ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, value, val)
	})
}

func TestDBIteratorUserView(t *testing.T) {
	t.Run("filters-non-default-cf", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("k1"), nonTxnMaxVersion, []byte("default"), 0)
		applyVersionedEntryForTest(t, db, kv.CFLock, []byte("k2"), nonTxnMaxVersion, []byte("lock"), 0)
		applyVersionedEntryForTest(t, db, kv.CFWrite, []byte("k3"), nonTxnMaxVersion, []byte("write"), 0)

		it := db.NewIterator(&index.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		var keys []string
		var cfs []kv.ColumnFamily
		for it.Rewind(); it.Valid(); it.Next() {
			entry := it.Item().Entry()
			keys = append(keys, string(entry.Key))
			cfs = append(cfs, entry.CF)
		}
		require.Equal(t, []string{"k1"}, keys)
		require.Equal(t, []kv.ColumnFamily{kv.CFDefault}, cfs)
	})

	t.Run("returns-latest-version-only", func(t *testing.T) {
		opt := newTestOptions(t)
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		key := []byte("k")
		applyVersionedEntryForTest(t, db, kv.CFDefault, key, 1, []byte("v1"), 0)
		applyVersionedEntryForTest(t, db, kv.CFDefault, key, 2, []byte("v2"), 0)

		it := db.NewIterator(&index.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		var versions []uint64
		var values []string
		for it.Rewind(); it.Valid(); it.Next() {
			entry := it.Item().Entry()
			versions = append(versions, entry.Version)
			values = append(values, string(entry.Value))
		}
		require.Equal(t, []uint64{2}, versions)
		require.Equal(t, []string{"v2"}, values)
	})
}

func TestDBIteratorReverseWithARTMemtable(t *testing.T) {
	opt := newTestOptions(t)
	opt.MemTableEngine = MemTableEngineART
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	for _, k := range []string{"a", "b", "c", "d"} {
		require.NoError(t, db.Set([]byte(k), []byte("v_"+k)))
	}

	it := db.NewIterator(&index.Options{IsAsc: false})
	defer func() { require.NoError(t, it.Close()) }()

	var keys []string
	for it.Rewind(); it.Valid(); it.Next() {
		keys = append(keys, string(it.Item().Entry().Key))
	}
	require.Equal(t, []string{"d", "c", "b", "a"}, keys)
}

func TestDBIteratorReverseLatestVersion(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("a"), 1, []byte("va"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("k"), 1, []byte("v1"), 0)
	applyVersionedEntryForTest(t, db, kv.CFDefault, []byte("k"), 2, []byte("v2"), 0)

	it := db.NewIterator(&index.Options{IsAsc: false})
	defer func() { require.NoError(t, it.Close()) }()

	var keys []string
	var versions []uint64
	var values []string
	for it.Rewind(); it.Valid(); it.Next() {
		entry := it.Item().Entry()
		keys = append(keys, string(entry.Key))
		versions = append(versions, entry.Version)
		values = append(values, string(entry.Value))
	}
	require.Equal(t, []string{"k", "a"}, keys)
	require.Equal(t, []uint64{2, 1}, versions)
	require.Equal(t, []string{"v2", "va"}, values)
}

func TestDBIteratorCloseIdempotentAcrossMemtableEngines(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		opt := newTestOptions(t)
		opt.MemTableEngine = engine
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		require.NoError(t, db.Set([]byte("k"), []byte("v")))
		it := db.NewIterator(&index.Options{IsAsc: true})
		it.Rewind()
		require.NoError(t, it.Close())
		require.NoError(t, it.Close())
	})
}

func TestRequestLoadEntriesCopiesSlice(t *testing.T) {
	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	defer func() {
		req.Entries = nil
		req.Ptrs = nil
		dbruntime.RequestPool.Put(req)
	}()

	e1 := &kv.Entry{Key: []byte("a")}
	e2 := &kv.Entry{Key: []byte("b")}
	src := []*kv.Entry{e1, e2}
	req.LoadEntries(src)

	if len(req.Entries) != len(src) {
		t.Fatalf("expected %d entries, got %d", len(src), len(req.Entries))
	}
	if &req.Entries[0] == &src[0] {
		t.Fatalf("request reused caller backing array")
	}
	src[0] = &kv.Entry{Key: []byte("z")}
	if string(req.Entries[0].Key) != "a" {
		t.Fatalf("entry data mutated with caller slice")
	}
}

func TestDirectoryLockPreventsConcurrentOpen(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:          dir,
		ValueThreshold:   1 << 10,
		MemTableSize:     1 << 12,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 18,
		MaxBatchCount:    16,
		MaxBatchSize:     1 << 20,
	}

	db := openTestDB(t, opt)
	_, err := Open(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), "already held")

	require.NoError(t, db.Close())

	db2 := openTestDB(t, opt)
	require.NoError(t, db2.Close())
}

func TestOpenRejectsSeededWorkdirByDefault(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())
	require.NoError(t, raftmode.Write(opt.WorkDir, raftmode.State{
		Mode:     raftmode.ModeSeeded,
		StoreID:  1,
		RegionID: 2,
		PeerID:   3,
	}))

	_, err := Open(opt)
	require.Error(t, err)
	require.Contains(t, err.Error(), `workdir mode "seeded"`)
}

func TestOpenAllowsSeededWorkdirWhenExplicitlyRequested(t *testing.T) {
	opt := newTestOptions(t)
	db := openTestDB(t, opt)
	require.NoError(t, db.Close())
	require.NoError(t, raftmode.Write(opt.WorkDir, raftmode.State{
		Mode:     raftmode.ModeSeeded,
		StoreID:  1,
		RegionID: 2,
		PeerID:   3,
	}))

	opt.AllowedModes = []raftmode.Mode{raftmode.ModeSeeded}
	db, err := Open(opt)
	require.NoError(t, err)
	require.NoError(t, db.Close())
}

func TestWriteHotKeyThrottleBlocksDB(t *testing.T) {
	clearDir()
	prev := opt.WriteHotKeyLimit
	opt.WriteHotKeyLimit = 3
	defer func() {
		opt.WriteHotKeyLimit = prev
	}()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("throttle-key")
	require.NoError(t, db.Set(key, []byte("v1")))
	require.NoError(t, db.Set(key, []byte("v2")))
	err := db.Set(key, []byte("v3"))
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), db.hotWriteLimited.Load())
}

// -------------------------------------------------------------------------- //
// Recovery and WAL/value log tests (merged from db_recovery_test.go)

func logRecoveryMetric(t *testing.T, name string, payload any) {
	if os.Getenv("RECOVERY_TRACE_METRICS") == "" {
		return
	}
	t.Helper()
	data, err := json.Marshal(payload)
	if err != nil {
		t.Logf("RECOVERY_METRIC %s marshal_error=%v payload=%+v", name, err, payload)
		return
	}
	t.Logf("RECOVERY_METRIC %s=%s", name, data)
}

func TestRecoveryRemovesStaleValueLogSegment(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:             dir,
		ValueThreshold:      0,
		MemTableSize:        1 << 12,
		SSTableMaxSz:        1 << 20,
		ValueLogFileSize:    1 << 14,
		ValueLogBucketCount: 1,
		MaxBatchCount:       100,
		MaxBatchSize:        1 << 20,
	}

	db := openTestDB(t, opt)

	for i := range 48 {
		val := make([]byte, 512)
		key := fmt.Appendf(nil, "key-%03d", i)
		require.NoError(t, db.Set(key, val))
	}
	fids := db.vlog.managers[0].ListFIDs()
	require.GreaterOrEqual(t, len(fids), 2)
	staleFID := fids[0]

	require.NoError(t, db.lsm.LogValueLogDelete(0, staleFID))

	stalePath := filepath.Join(dir, "vlog", "bucket-000", fmt.Sprintf("%05d.vlog", staleFID))
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("expected stale value log file %s to exist: %v", stalePath, err)
	}

	require.NoError(t, db.Close())

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()

	_, err := os.Stat(stalePath)
	require.Error(t, err)
	removed := os.IsNotExist(err)
	require.True(t, removed, "expected stale value log file to be deleted on recovery")

	status := db2.valueLogStatusSnapshot()
	meta, ok := status[manifest.ValueLogID{Bucket: 0, FileID: staleFID}]
	if ok {
		require.False(t, meta.Valid)
	}
	logRecoveryMetric(t, "value_log_gc", map[string]any{
		"stale_fid":         staleFID,
		"stale_path":        stalePath,
		"file_removed":      removed,
		"status_has_entry":  ok,
		"status_valid_flag": meta.Valid,
		"status_len":        len(status),
	})
}

func TestRecoveryRemovesOrphanValueLogSegment(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:             dir,
		ValueThreshold:      0,
		MemTableSize:        1 << 12,
		SSTableMaxSz:        1 << 20,
		ValueLogFileSize:    1 << 14,
		ValueLogBucketCount: 1,
		MaxBatchCount:       100,
		MaxBatchSize:        1 << 20,
	}

	db := openTestDB(t, opt)
	key := []byte("orphan-key")
	val := make([]byte, 512)
	require.NoError(t, db.Set(key, val))

	headPtr := db.vlog.managers[0].Head()
	require.False(t, headPtr.IsZero(), "expected value log head to be initialized")
	headCopy := headPtr
	require.NoError(t, db.lsm.LogValueLogHead(&headCopy))
	before := db.valueLogStatusSnapshot()
	beforeInfo := make(map[manifest.ValueLogID]bool, len(before))
	for id, meta := range before {
		beforeInfo[id] = meta.Valid
	}
	require.NoError(t, db.Close())

	orphanFID := uint32(123)
	orphanPath := filepath.Join(dir, "vlog", "bucket-000", fmt.Sprintf("%05d.vlog", orphanFID))
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), 0o666))

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()

	heads := db2.getHeads()
	headMeta, hasHead := heads[0]
	status := db2.valueLogStatusSnapshot()
	statusInfo := make(map[manifest.ValueLogID]bool, len(status))
	for id, meta := range status {
		statusInfo[id] = meta.Valid
	}
	remainingFIDs := db2.vlog.managers[0].ListFIDs()

	_, err := os.Stat(orphanPath)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err), "expected orphan value log file to be deleted on recovery")

	for _, fid := range remainingFIDs {
		require.NotEqual(t, orphanFID, fid)
	}

	logRecoveryMetric(t, "value_log_orphan_cleanup", map[string]any{
		"orphan_fid":        orphanFID,
		"orphan_path":       orphanPath,
		"pre_status_valid":  beforeInfo,
		"post_status_valid": statusInfo,
		"head_meta":         headMeta,
		"head_present":      hasHead,
		"fids_remaining":    remainingFIDs,
	})
}

func TestRecoveryFailsOnMissingSST(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:          dir,
		ValueThreshold:   1 << 20,
		MemTableSize:     1 << 10,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 20,
		MaxBatchCount:    100,
		MaxBatchSize:     1 << 20,
	}

	db := openTestDB(t, opt)
	for i := range 256 {
		key := fmt.Appendf(nil, "sst-crash-%03d", i)
		val := make([]byte, 128)
		require.NoError(t, db.Set(key, val))
	}
	require.NoError(t, db.Close())

	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	removed := files[0]
	require.NoError(t, os.Remove(removed))
	removedFID := vfs.FID(removed)

	db2, openErr := Open(opt)
	require.Error(t, openErr)
	require.Nil(t, db2)
	require.Contains(t, openErr.Error(), "missing sstable")

	mgr, err := manifest.Open(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close()) }()

	version := mgr.Current()
	var found bool
	for _, metas := range version.Levels {
		for _, meta := range metas {
			if meta.FileID == removedFID {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	require.True(t, found, "expected missing sstable to remain referenced in manifest after failed startup")

	logRecoveryMetric(t, "sst_missing_startup_failure", map[string]any{
		"removed_path": removed,
		"removed_fid":  removedFID,
		"open_error":   openErr.Error(),
	})
}

func TestRecoveryFailsOnCorruptSST(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:          dir,
		ValueThreshold:   1 << 20,
		MemTableSize:     1 << 10,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 20,
		MaxBatchCount:    100,
		MaxBatchSize:     1 << 20,
	}

	db := openTestDB(t, opt)
	for i := range 256 {
		key := fmt.Appendf(nil, "sst-corrupt-%03d", i)
		val := make([]byte, 128)
		require.NoError(t, db.Set(key, val))
	}
	require.NoError(t, db.Close())

	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	corruptPath := files[0]
	corruptFID := vfs.FID(corruptPath)
	require.NoError(t, os.WriteFile(corruptPath, []byte("bad-sst"), 0o666))

	db2, openErr := Open(opt)
	require.Error(t, openErr)
	require.Nil(t, db2)
	require.Contains(t, openErr.Error(), "open sstable")

	mgr, err := manifest.Open(dir, nil)
	require.NoError(t, err)
	defer func() { require.NoError(t, mgr.Close()) }()

	version := mgr.Current()
	var found bool
	for _, metas := range version.Levels {
		for _, meta := range metas {
			if meta.FileID == corruptFID {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	require.True(t, found, "expected corrupt sstable to remain referenced in manifest after failed startup")

	logRecoveryMetric(t, "sst_corrupt_startup_failure", map[string]any{
		"corrupt_path": corruptPath,
		"corrupt_fid":  corruptFID,
		"levels":       len(version.Levels),
		"open_error":   openErr.Error(),
	})
}

func TestRecoveryManifestRewriteCrash(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:          dir,
		ValueThreshold:   1 << 20,
		MemTableSize:     1 << 10,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 20,
		MaxBatchCount:    100,
		MaxBatchSize:     1 << 20,
	}

	db := openTestDB(t, opt)
	require.NoError(t, db.Set([]byte("rewrite-key"), []byte("rewrite-val")))
	require.NoError(t, db.Close())

	current := filepath.Join(dir, "CURRENT")
	data, err := os.ReadFile(current)
	require.NoError(t, err)
	manifestName := string(data)

	tmp := filepath.Join(dir, "CURRENT.tmp")
	require.NoError(t, os.WriteFile(tmp, []byte("MANIFEST-999999"), 0o666))

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()

	name, err := os.ReadFile(current)
	require.NoError(t, err)
	require.Equal(t, manifestName, string(name))

	tmpExists := false
	item, err := db2.Get([]byte("rewrite-key"))
	require.NoError(t, err)
	require.Equal(t, []byte("rewrite-val"), item.Value)

	_, err = os.Stat(tmp)
	if err == nil {
		tmpExists = true
		require.NoError(t, os.Remove(tmp))
	}
	logRecoveryMetric(t, "manifest_rewrite", map[string]any{
		"current_manifest": manifestName,
		"current_path":     current,
		"tmp_path":         tmp,
		"tmp_exists":       tmpExists,
	})
}

func TestRecoverySnapshotExportRoundTrip(t *testing.T) {
	dir := t.TempDir()
	walDir := filepath.Join(dir, "wal")
	manifestDir := filepath.Join(dir, "manifest")

	walMgr, err := wal.Open(wal.Config{Dir: walDir})
	require.NoError(t, err)
	defer func() { _ = walMgr.Close() }()

	localMeta, err := localmeta.OpenLocalStore(manifestDir, nil)
	require.NoError(t, err)
	defer func() { _ = localMeta.Close() }()

	ws, err := raftlog.OpenWALStorage(raftlog.WALStorageConfig{
		GroupID:   1,
		WAL:       walMgr,
		LocalMeta: localMeta,
	})
	require.NoError(t, err)

	snapshot := myraft.Snapshot{
		Metadata: raftpb.SnapshotMetadata{
			Index:     7,
			Term:      2,
			ConfState: raftpb.ConfState{Voters: []uint64{1}},
		},
		Data: []byte("raft-recovery-snapshot"),
	}
	require.NoError(t, ws.ApplySnapshot(snapshot))

	exportPath := filepath.Join(dir, "raft.snapshot")
	require.NoError(t, raftlog.ExportSnapshot(ws, exportPath, nil))
	logRecoveryMetric(t, "raft_snapshot_export", map[string]any{
		"group_id":        1,
		"snapshot_index":  snapshot.Metadata.Index,
		"snapshot_term":   snapshot.Metadata.Term,
		"export_path":     exportPath,
		"manifest_dir":    manifestDir,
		"wal_dir":         walDir,
		"snapshot_length": len(snapshot.Data),
	})

	restoreWalDir := filepath.Join(dir, "restore", "wal")
	restoreManifestDir := filepath.Join(dir, "restore", "manifest")
	walMgrRestore, err := wal.Open(wal.Config{Dir: restoreWalDir})
	require.NoError(t, err)
	defer func() { _ = walMgrRestore.Close() }()

	localMetaRestore, err := localmeta.OpenLocalStore(restoreManifestDir, nil)
	require.NoError(t, err)
	defer func() { _ = localMetaRestore.Close() }()

	wsRestore, err := raftlog.OpenWALStorage(raftlog.WALStorageConfig{
		GroupID:   1,
		WAL:       walMgrRestore,
		LocalMeta: localMetaRestore,
	})
	require.NoError(t, err)

	require.NoError(t, raftlog.ImportSnapshot(wsRestore, exportPath, nil))

	ptr, ok := localMetaRestore.RaftPointer(1)
	require.True(t, ok)
	require.Equal(t, snapshot.Metadata.Index, ptr.SnapshotIndex)
	require.Equal(t, snapshot.Metadata.Term, ptr.SnapshotTerm)

	logRecoveryMetric(t, "raft_snapshot_import", map[string]any{
		"group_id":       1,
		"snapshot_index": ptr.SnapshotIndex,
		"snapshot_term":  ptr.SnapshotTerm,
		"manifest_dir":   restoreManifestDir,
		"wal_dir":        restoreWalDir,
	})
}

func TestRecoveryWALReplayRestoresData(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		WorkDir:             dir,
		ValueThreshold:      1 << 20,
		MemTableSize:        1 << 16,
		SSTableMaxSz:        1 << 20,
		ValueLogFileSize:    1 << 20,
		ValueLogBucketCount: 1,
		MaxBatchCount:       100,
		MaxBatchSize:        1 << 20,
	}

	db := openTestDB(t, opt)
	key := []byte("wal-crash-key")
	val := []byte("wal-crash-value")
	require.NoError(t, db.Set(key, val))

	// Simulate crash: close WAL/ValueLog handles without flushing LSM.
	drSimulateCrash(t, db)

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()

	item, err := db2.Get(key)
	require.NoError(t, err)
	require.Equal(t, val, item.Value)
	logRecoveryMetric(t, "wal_replay", map[string]any{
		"key":           string(key),
		"value_base64":  item.Value,
		"wal_dir":       filepath.Join(opt.WorkDir, "wal"),
		"recovered_len": len(item.Value),
	})
}

func TestRecoverySlowFollowerSnapshotBacklog(t *testing.T) {
	root := t.TempDir()
	opt := &Options{
		WorkDir:             root,
		ValueThreshold:      1 << 20,
		MemTableSize:        1 << 12,
		SSTableMaxSz:        1 << 20,
		ValueLogFileSize:    1 << 20,
		ValueLogBucketCount: 1,
		MaxBatchCount:       32,
		MaxBatchSize:        1 << 20,
	}
	localMeta, err := localmeta.OpenLocalStore(root, nil)
	require.NoError(t, err)
	defer func() { _ = localMeta.Close() }()
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	walMgr, err := db.raftWALFor(1)
	require.NoError(t, err)

	appendRaft := func(data string) {
		_, err := walMgr.AppendRecords(wal.DurabilityBuffered, wal.Record{Type: wal.RecordTypeRaftEntry, Payload: []byte(data)})
		require.NoError(t, err)
		require.NoError(t, walMgr.Sync())
	}

	appendRaft("group1-seg1")
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{GroupID: 1, Segment: walMgr.ActiveSegment(), AppliedIndex: 10, AppliedTerm: 1}))
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{GroupID: 9, Segment: walMgr.ActiveSegment(), AppliedIndex: 9, AppliedTerm: 1}))

	snapBefore := db.Info().Snapshot()
	logRecoveryMetric(t, "raft_wal_backlog_pre", map[string]any{
		"wal_segments_with_raft": snapBefore.WAL.SegmentsWithRaftRecords,
		"wal_removable_segments": snapBefore.WAL.RemovableRaftSegments,
	})

	require.NoError(t, walMgr.SwitchSegment(2, true))
	appendRaft("group1-seg2")
	require.NoError(t, walMgr.SwitchSegment(3, true))
	appendRaft("group1-seg3")

	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{
		GroupID:        1,
		Segment:        3,
		AppliedIndex:   30,
		AppliedTerm:    4,
		TruncatedIndex: 30,
		TruncatedTerm:  4,
		SegmentIndex:   3,
	}))
	require.NoError(t, localMeta.SaveRaftPointer(localmeta.RaftLogPointer{
		GroupID:        9,
		Segment:        3,
		AppliedIndex:   28,
		AppliedTerm:    4,
		TruncatedIndex: 28,
		TruncatedTerm:  4,
		SegmentIndex:   3,
	}))

	snapAfter := db.Info().Snapshot()
	require.Greater(t, snapAfter.WAL.SegmentsWithRaftRecords, 0, "expected raft segments to be tracked")
	require.Greater(t, snapAfter.WAL.RemovableRaftSegments, 0, "expected removable raft backlog once followers catch up")
	logRecoveryMetric(t, "raft_wal_backlog_post", map[string]any{
		"wal_segments_with_raft": snapAfter.WAL.SegmentsWithRaftRecords,
		"wal_removable_segments": snapAfter.WAL.RemovableRaftSegments,
	})
}

func TestRecoverySkipsValueLogReplay(t *testing.T) {
	dir := t.TempDir()
	opt := NewDefaultOptions()
	opt.WorkDir = dir
	opt.ValueLogFileSize = 1 << 16
	opt.ValueThreshold = 1 << 20
	opt.ValueLogBucketCount = 1
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0

	db := openTestDB(t, opt)

	userKey := []byte("vlog-replay-key")
	internalKey := kv.InternalKey(kv.CFDefault, userKey, math.MaxUint64)
	entry := kv.NewEntry(internalKey, []byte("payload"))
	_, err := db.vlog.managers[0].AppendEntry(entry)
	require.NoError(t, err)
	entry.DecrRef()
	require.NoError(t, db.vlog.managers[0].SyncActive())
	require.NoError(t, db.Close())

	db2 := openTestDB(t, opt)
	defer func() { _ = db2.Close() }()

	_, err = db2.Get(userKey)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestWriteHotKeyThrottleBlocksSet(t *testing.T) {
	clearDir()
	prev := opt.WriteHotKeyLimit
	opt.WriteHotKeyLimit = 3
	defer func() {
		opt.WriteHotKeyLimit = prev
	}()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("txn-hot-key")
	require.NoError(t, db.Set(key, []byte("a")))
	require.NoError(t, db.Set(key, []byte("b")))
	err := db.Set(key, []byte("c"))
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), db.hotWriteLimited.Load())
}

func TestHotWriteAndThrottle(t *testing.T) {
	db := &DB{
		opt: &Options{
			WriteHotKeyLimit: 1,
		},
		hotWrite: thermos.NewRotatingThermos(8, nil),
	}

	userKey := []byte("hot")
	err := db.maybeThrottleWrite(kv.CFDefault, userKey)
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), db.hotWriteLimited.Load())
}

func TestApplyRequestsFailureIndex(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.ValueLogGCInterval = 0
	local.WriteBatchWait = 0

	db := openTestDB(t, local)
	defer func() { _ = db.Close() }()

	good := kv.NewInternalEntry(kv.CFDefault, []byte("good"), nonTxnMaxVersion, []byte("v1"), 0, 0)
	bad := kv.NewEntry([]byte{}, []byte("v2"))
	defer good.DecrRef()
	defer bad.DecrRef()

	reqs := []*dbruntime.Request{
		{
			Entries: []*kv.Entry{good},
			Ptrs:    []kv.ValuePtr{{}},
		},
		{
			Entries: []*kv.Entry{bad},
			Ptrs:    []kv.ValuePtr{{}},
		},
	}

	failedAt, err := db.applyRequests(reqs, 0)
	require.Equal(t, 1, failedAt)
	require.Error(t, err)

	got, getErr := db.lsm.Get(good.Key)
	require.NoError(t, getErr)
	require.Equal(t, []byte("v1"), got.Value)
	got.DecrRef()
}

func TestApplyRequestsInlineRequestWithoutPtrs(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.ValueLogGCInterval = 0
	local.WriteBatchWait = 0
	local.ValueThreshold = 1 << 20

	db := openTestDB(t, local)
	defer func() { _ = db.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("inline-fast-path"), nonTxnMaxVersion, []byte("v1"), 0, 0)
	defer entry.DecrRef()

	reqs := []*dbruntime.Request{
		{
			Entries: []*kv.Entry{entry},
		},
	}

	failedAt, err := db.applyRequests(reqs, 0)
	require.Equal(t, -1, failedAt)
	require.NoError(t, err)

	got, getErr := db.lsm.Get(entry.Key)
	require.NoError(t, getErr)
	require.Equal(t, []byte("v1"), got.Value)
	got.DecrRef()
}

func TestApplyRequestsCoalescesCommitBatchIntoOneLSMRecord(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.ValueLogGCInterval = 0
	local.WriteBatchWait = 0
	local.ValueThreshold = 1 << 20

	db := openTestDB(t, local)
	defer func() { _ = db.Close() }()

	first := kv.NewInternalEntry(kv.CFDefault, []byte("coalesce-a"), nonTxnMaxVersion, []byte("v1"), 0, 0)
	second := kv.NewInternalEntry(kv.CFDefault, []byte("coalesce-b"), nonTxnMaxVersion-1, []byte("v2"), 0, 0)
	defer first.DecrRef()
	defer second.DecrRef()

	reqs := []*dbruntime.Request{
		{Entries: []*kv.Entry{first}},
		{Entries: []*kv.Entry{second}},
	}

	failedAt, err := db.applyRequests(reqs, 0)
	require.Equal(t, -1, failedAt)
	require.NoError(t, err)

	var batchRecords int
	var decoded int
	err = db.lsmWALs[0].Replay(func(info wal.EntryInfo, payload []byte) error {
		if info.Type != wal.RecordTypeEntryBatch {
			return nil
		}
		batchRecords++
		entries, err := wal.DecodeEntryBatch(payload)
		if err != nil {
			return err
		}
		decoded += len(entries)
		for _, entry := range entries {
			entry.DecrRef()
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, batchRecords)
	require.Equal(t, 2, decoded)
}

func TestFinishCommitRequestsPerRequestErrors(t *testing.T) {
	db := &DB{}
	req1 := &dbruntime.Request{}
	req2 := &dbruntime.Request{}
	req1.WG.Add(1)
	req2.WG.Add(1)
	reqErr := errors.New("request failed")

	batch := []*dbruntime.CommitRequest{
		{Req: req1},
		{Req: req2},
	}
	perReq := map[*dbruntime.Request]error{
		req2: reqErr,
	}

	db.finishCommitRequests(batch, nil, perReq)
	req1.WG.Wait()
	req2.WG.Wait()

	require.NoError(t, req1.Err)
	require.ErrorIs(t, req2.Err, reqErr)
}

func TestCloseWithErrors(t *testing.T) {
	local := *opt
	local.WorkDir = t.TempDir()
	dirLockErr := errors.New("dir lock release error")

	db := openTestDB(t, &local)
	realLock := db.dirLock
	db.dirLock = closeFunc(func() error {
		if realLock != nil {
			_ = realLock.Close()
		}
		return dirLockErr
	})
	err := db.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, dirLockErr)
	require.True(t, db.IsClosed())

	err2 := db.Close()
	require.Error(t, err2)
	require.ErrorIs(t, err2, dirLockErr)
}

type closeFunc func() error

func (fn closeFunc) Close() error {
	return fn()
}

func TestCloseConcurrent(t *testing.T) {
	local := *opt
	local.WorkDir = t.TempDir()
	db := openTestDB(t, &local)

	var wg sync.WaitGroup
	const workers = 16
	errs := make(chan error, workers)
	for range workers {
		wg.Go(func() {
			errs <- db.Close()
		})
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
	require.True(t, db.IsClosed())
}

func drTestOptions(dir string) *Options {
	cfg := *opt
	cfg.WorkDir = dir
	return &cfg
}

func drMustSet(t *testing.T, db *DB, key, value []byte) {
	t.Helper()
	if err := db.Set(key, value); err != nil {
		t.Fatal(err)
	}
}

func drMustDeleteRange(t *testing.T, db *DB, start, end []byte) {
	t.Helper()
	if err := db.DeleteRange(start, end); err != nil {
		t.Fatal(err)
	}
}

func drMustDel(t *testing.T, db *DB, key []byte) {
	t.Helper()
	if err := db.Del(key); err != nil {
		t.Fatal(err)
	}
}

func drMustClose(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
}

func drWaitForFlushedSST(t *testing.T, db *DB) {
	t.Helper()
	deadline := time.Now().Add(8 * time.Second)
	for time.Now().Before(deadline) {
		snap := db.Info().Snapshot()
		hasSST := false
		for _, lvl := range snap.LSM.Levels {
			if lvl.TableCount > 0 || lvl.IngestTables > 0 {
				hasSST = true
				break
			}
		}
		if hasSST && snap.Flush.Pending == 0 && snap.Flush.Active == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for flushed sst")
}

func drForEachMemTableEngine(t *testing.T, fn func(t *testing.T, engine MemTableEngine)) {
	t.Helper()
	for _, engine := range []MemTableEngine{MemTableEngineART, MemTableEngineSkiplist} {
		t.Run(string(engine), func(t *testing.T) {
			fn(t, engine)
		})
	}
}

func drSimulateCrash(t *testing.T, db *DB) {
	t.Helper()
	_ = db.Info().close()
	for _, mgr := range db.vlog.managers {
		if mgr != nil {
			_ = mgr.Close()
		}
	}
	// Close each WAL Manager but do not nil the slot — commit processor
	// goroutines still hold the pointer (cached at startup) and the race
	// detector flags the slot rewrite even though the goroutine never
	// re-reads the slice. A closed Manager is enough to fail subsequent
	// writes; nilling is unnecessary.
	for _, mgr := range db.lsmWALs {
		if mgr != nil {
			_ = mgr.Close()
		}
	}
	if db.dirLock != nil {
		_ = db.dirLock.Close()
		db.dirLock = nil
	}
}

func drRequireValue(t *testing.T, db *DB, key, expected []byte) {
	t.Helper()
	entry, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, expected, entry.Value)
}

func drRequireNotFound(t *testing.T, db *DB, key []byte) {
	t.Helper()
	_, err := db.Get(key)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func drFirstWALSegmentPath(t *testing.T, dir string) string {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(dir, "lsm-wal-*", "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, files, "expected at least one wal segment")
	return files[0]
}

func drAppendPartialWALTail(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
	require.NoError(t, err)
	defer func() { _ = f.Close() }()

	var recordLen uint32 = 32
	buf := make([]byte, 4)
	buf[0] = byte(recordLen >> 24)
	buf[1] = byte(recordLen >> 16)
	buf[2] = byte(recordLen >> 8)
	buf[3] = byte(recordLen)
	_, err = f.Write(buf)
	require.NoError(t, err)
	_, err = f.Write([]byte("partial"))
	require.NoError(t, err)
}

// TestDeleteRangeCore tests basic functionality, boundaries, lexicographic ordering,
// empty ranges, and write-after-delete scenarios.
func TestDeleteRangeCore(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// Test 1: Basic deletion with [start, end) semantics
	drMustSet(t, db, []byte("a"), []byte("1"))
	drMustSet(t, db, []byte("b"), []byte("2"))
	drMustSet(t, db, []byte("c"), []byte("3"))

	if err := db.DeleteRange([]byte("a"), []byte("c")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("a")); err != utils.ErrKeyNotFound {
		t.Error("start key should be deleted")
	}
	if _, err := db.Get([]byte("b")); err != utils.ErrKeyNotFound {
		t.Error("middle key should be deleted")
	}
	if e, err := db.Get([]byte("c")); err != nil || !bytes.Equal(e.Value, []byte("3")) {
		t.Error("end key should not be deleted (primacy)")
	}

	// Test 2: Lexicographic ordering
	drMustSet(t, db, []byte("key1"), []byte("v1"))
	drMustSet(t, db, []byte("key10"), []byte("v10"))
	drMustSet(t, db, []byte("key2"), []byte("v2"))

	drMustDeleteRange(t, db, []byte("key1"), []byte("key2"))

	if _, err := db.Get([]byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("key1 should be deleted")
	}
	if _, err := db.Get([]byte("key10")); err != utils.ErrKeyNotFound {
		t.Error("key10 should be deleted (lexicographically between key1 and key2)")
	}
	if _, err := db.Get([]byte("key2")); err != nil {
		t.Error("key2 should exist (primacy end)")
	}

	// Test 3: Empty range (no keys in range)
	drMustSet(t, db, []byte("x"), []byte("1"))
	drMustSet(t, db, []byte("z"), []byte("2"))

	if err := db.DeleteRange([]byte("xa"), []byte("xz")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("x")); err != nil {
		t.Error("key before range should exist")
	}
	if _, err := db.Get([]byte("z")); err != nil {
		t.Error("key after range should exist")
	}

	// Test 4: Write after delete
	drMustSet(t, db, []byte("rewrite"), []byte("old"))
	drMustDeleteRange(t, db, []byte("rewrite"), []byte("rewritf"))

	if _, err := db.Get([]byte("rewrite")); err != utils.ErrKeyNotFound {
		t.Error("key should be deleted")
	}

	drMustSet(t, db, []byte("rewrite"), []byte("new"))
	if e, err := db.Get([]byte("rewrite")); err != nil || !bytes.Equal(e.Value, []byte("new")) {
		t.Error("key should have new value after rewrite")
	}
}

// TestDeleteRangeValidation tests error handling for invalid inputs.
func TestDeleteRangeValidation(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// Inverted range
	if err := db.DeleteRange([]byte("z"), []byte("a")); err != utils.ErrInvalidRequest {
		t.Errorf("expected invalid request for inverted range, got %v", err)
	}

	// Equal keys
	if err := db.DeleteRange([]byte("a"), []byte("a")); err != utils.ErrInvalidRequest {
		t.Errorf("expected invalid request for equal keys, got %v", err)
	}

	// Empty key
	if err := db.DeleteRange([]byte(""), []byte("a")); err != utils.ErrEmptyKey {
		t.Errorf("expected empty key error, got %v", err)
	}
}

// TestDeleteRangeIsolation tests that default-CF DeleteRange does not affect other CFs.
func TestDeleteRangeIsolation(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	defaultEntry := kv.NewInternalEntry(kv.CFDefault, []byte("key1"), db.nextNonTxnVersion(), []byte("val1"), 0, 0)
	lockEntry := kv.NewInternalEntry(kv.CFLock, []byte("key1"), db.nextNonTxnVersion(), []byte("lock1"), 0, 0)
	defer defaultEntry.DecrRef()
	defer lockEntry.DecrRef()
	if err := db.ApplyInternalEntries([]*kv.Entry{defaultEntry, lockEntry}); err != nil {
		t.Fatal(err)
	}

	if err := db.DeleteRange([]byte("key1"), []byte("key2")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.GetInternalEntry(kv.CFDefault, []byte("key1"), nonTxnMaxVersion); err != utils.ErrKeyNotFound {
		t.Error("default CF key should be deleted")
	}
	entry, err := db.GetInternalEntry(kv.CFLock, []byte("key1"), nonTxnMaxVersion)
	if err != nil {
		t.Error("lock CF key should still exist")
	} else {
		entry.DecrRef()
	}
}

// TestDeleteRangeComplex tests overlapping ranges and interaction with point deletes.
func TestDeleteRangeComplex(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// Test 1: Overlapping ranges
	drMustSet(t, db, []byte("a"), []byte("1"))
	drMustSet(t, db, []byte("b"), []byte("2"))
	drMustSet(t, db, []byte("c"), []byte("3"))
	drMustSet(t, db, []byte("d"), []byte("4"))

	drMustDeleteRange(t, db, []byte("a"), []byte("c"))
	drMustDeleteRange(t, db, []byte("b"), []byte("d"))

	if _, err := db.Get([]byte("a")); err != utils.ErrKeyNotFound {
		t.Error("a should be deleted")
	}
	if _, err := db.Get([]byte("b")); err != utils.ErrKeyNotFound {
		t.Error("b should be deleted")
	}
	if _, err := db.Get([]byte("c")); err != utils.ErrKeyNotFound {
		t.Error("c should be deleted")
	}
	if _, err := db.Get([]byte("d")); err != nil {
		t.Error("d should exist")
	}

	// Test 2: Range delete over already deleted keys
	drMustSet(t, db, []byte("x"), []byte("1"))
	drMustSet(t, db, []byte("y"), []byte("2"))
	drMustSet(t, db, []byte("z"), []byte("3"))

	drMustDel(t, db, []byte("y"))

	if err := db.DeleteRange([]byte("x"), []byte("zz")); err != nil {
		t.Fatal(err)
	}

	if _, err := db.Get([]byte("x")); err != utils.ErrKeyNotFound {
		t.Error("x should be deleted")
	}
	if _, err := db.Get([]byte("y")); err != utils.ErrKeyNotFound {
		t.Error("y should remain deleted")
	}
	if _, err := db.Get([]byte("z")); err != utils.ErrKeyNotFound {
		t.Error("z should be deleted")
	}
}

// TestDeleteRangeWithCompaction tests range deletion behavior during compaction.
func TestDeleteRangeWithCompaction(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	opt.MemTableSize = 1024
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	for i := range 100 {
		key := []byte{byte('a' + i%26), byte(i)}
		drMustSet(t, db, key, []byte("value"))
	}

	drMustDeleteRange(t, db, []byte{byte('a')}, []byte{byte('m')})

	for i := range 100 {
		key := []byte{byte('a' + i%26), byte(i)}
		_, err := db.Get(key)
		if key[0] < 'm' {
			if err != utils.ErrKeyNotFound {
				t.Errorf("key %v should be deleted", key)
			}
		} else {
			if err != nil {
				t.Errorf("key %v should exist", key)
			}
		}
	}
}

// TestDeleteRangeWALRecovery tests that range tombstones are correctly recovered from WAL.
func TestDeleteRangeWALRecovery(t *testing.T) {
	dir := t.TempDir()
	opt := drTestOptions(dir)

	db := openTestDB(t, opt)
	drMustSet(t, db, []byte("key1"), []byte("val1"))
	drMustSet(t, db, []byte("key2"), []byte("val2"))
	drMustSet(t, db, []byte("key3"), []byte("val3"))
	drMustDeleteRange(t, db, []byte("key1"), []byte("key3"))
	drMustClose(t, db)

	db = openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	if _, err := db.Get([]byte("key1")); err != utils.ErrKeyNotFound {
		t.Error("key1 should be deleted after recovery")
	}
	if _, err := db.Get([]byte("key2")); err != utils.ErrKeyNotFound {
		t.Error("key2 should be deleted after recovery")
	}
	if _, err := db.Get([]byte("key3")); err != nil {
		t.Error("key3 should exist after recovery")
	}
}

// TestDeleteRangeVisibilityBug ensures a newer point write remains visible after
// an earlier range tombstone.
func TestDeleteRangeVisibilityBug(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	drMustSet(t, db, []byte("a1"), []byte("old"))
	drMustDeleteRange(t, db, []byte("a0"), []byte("a9"))
	drMustSet(t, db, []byte("a1"), []byte("new"))

	e, err := db.Get([]byte("a1"))
	if err != nil {
		t.Fatalf("expected key a1 to exist with value 'new', got error: %v", err)
	}
	if !bytes.Equal(e.Value, []byte("new")) {
		t.Errorf("expected value 'new', got '%s'", e.Value)
	}
}

func TestDeleteRangePersistsAfterFlushAndReopen(t *testing.T) {
	dir := t.TempDir()
	opt := drTestOptions(dir)
	opt.MemTableSize = 512
	opt.NumLevelZeroTables = 1000

	db := openTestDB(t, opt)
	drMustSet(t, db, []byte("b"), []byte("old"))
	drMustDeleteRange(t, db, []byte("a"), []byte("z"))
	drMustSet(t, db, []byte("y"), []byte("new"))

	padding := bytes.Repeat([]byte("x"), 192)
	for i := range 64 {
		key := fmt.Appendf(nil, "pad-%03d", i)
		drMustSet(t, db, key, padding)
	}
	drWaitForFlushedSST(t, db)
	drMustClose(t, db)

	db = openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	_, err := db.Get([]byte("b"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	entry, err := db.Get([]byte("y"))
	require.NoError(t, err)
	require.Equal(t, []byte("new"), entry.Value)
}

func TestDeleteRangeBatchOrdering(t *testing.T) {
	opt := drTestOptions(t.TempDir())
	db := openTestDB(t, opt)
	defer func() { drMustClose(t, db) }()

	// point then range tombstone in one batch: point should be hidden.
	setV := db.nextNonTxnVersion()
	rtV := db.nextNonTxnVersion()
	setEntry := kv.NewInternalEntry(kv.CFDefault, []byte("b"), setV, []byte("old"), 0, 0)
	rtEntry := kv.NewInternalEntry(kv.CFDefault, []byte("a"), rtV, []byte("z"), kv.BitRangeDelete, 0)
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{setEntry, rtEntry}))
	setEntry.DecrRef()
	rtEntry.DecrRef()
	_, err := db.Get([]byte("b"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	// range tombstone then point in one batch: point should remain visible.
	rtV2 := db.nextNonTxnVersion()
	setV2 := db.nextNonTxnVersion()
	rtEntry2 := kv.NewInternalEntry(kv.CFDefault, []byte("a"), rtV2, []byte("z"), kv.BitRangeDelete, 0)
	setEntry2 := kv.NewInternalEntry(kv.CFDefault, []byte("c"), setV2, []byte("new"), 0, 0)
	require.NoError(t, db.ApplyInternalEntries([]*kv.Entry{rtEntry2, setEntry2}))
	rtEntry2.DecrRef()
	setEntry2.DecrRef()
	entry, err := db.Get([]byte("c"))
	require.NoError(t, err)
	require.Equal(t, []byte("new"), entry.Value)
}

func TestDBIteratorBoundsAndOutOfRangeSeekContract(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		opt := newTestOptions(t)
		opt.MemTableEngine = engine
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		for _, k := range []string{"a", "b", "c", "d"} {
			require.NoError(t, db.Set([]byte(k), []byte("v_"+k)))
		}

		t.Run("forward", func(t *testing.T) {
			it := db.NewIterator(&index.Options{
				IsAsc:      true,
				LowerBound: []byte("b"),
				UpperBound: []byte("d"),
			})
			defer func() { require.NoError(t, it.Close()) }()

			var keys []string
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, string(it.Item().Entry().Key))
			}
			require.Equal(t, []string{"b", "c"}, keys)

			it.Seek([]byte("a"))
			require.True(t, it.Valid())
			require.Equal(t, "b", string(it.Item().Entry().Key))

			it.Seek([]byte("z"))
			require.False(t, it.Valid())
			it.Next()
			require.False(t, it.Valid(), "Next must not resurrect validity after out-of-range seek")

			it.Rewind()
			require.True(t, it.Valid())
			require.Equal(t, "b", string(it.Item().Entry().Key))
		})

		t.Run("reverse", func(t *testing.T) {
			it := db.NewIterator(&index.Options{
				IsAsc:      false,
				LowerBound: []byte("b"),
				UpperBound: []byte("d"),
			})
			defer func() { require.NoError(t, it.Close()) }()

			var keys []string
			for it.Rewind(); it.Valid(); it.Next() {
				keys = append(keys, string(it.Item().Entry().Key))
			}
			require.Equal(t, []string{"c", "b"}, keys)

			it.Seek([]byte("a"))
			require.False(t, it.Valid())
			it.Next()
			require.False(t, it.Valid(), "Next must not resurrect validity after out-of-range seek")

			it.Seek([]byte("z"))
			require.True(t, it.Valid())
			require.Equal(t, "c", string(it.Item().Entry().Key))
		})
	})
}

func TestAPIMixedOpsPersistAcrossFlushCompactionAndReopen(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		dir := t.TempDir()
		opt := drTestOptions(dir)
		opt.MemTableEngine = engine
		opt.MemTableSize = 512
		opt.NumLevelZeroTables = 2

		db := openTestDB(t, opt)
		require.NoError(t, db.SetBatch([]BatchSetItem{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
			{Key: []byte("k3"), Value: []byte("v3")},
		}))
		require.NoError(t, db.Del([]byte("k1")))
		require.NoError(t, db.DeleteRange([]byte("k2"), []byte("k4")))
		require.NoError(t, db.Set([]byte("k3"), []byte("v3-new")))
		require.NoError(t, db.SetBatch([]BatchSetItem{
			{Key: []byte("k4"), Value: []byte("v4")},
			{Key: []byte("k5"), Value: []byte("v5")},
		}))
		require.NoError(t, db.DeleteRange([]byte("k5"), []byte("k6")))

		padding := bytes.Repeat([]byte("p"), 160)
		for i := range 48 {
			key := fmt.Appendf(nil, "pad-%03d", i)
			require.NoError(t, db.Set(key, padding))
		}
		drWaitForFlushedSST(t, db)

		drMustClose(t, db)
		db = openTestDB(t, opt)
		defer func() { drMustClose(t, db) }()

		drRequireNotFound(t, db, []byte("k1"))
		drRequireNotFound(t, db, []byte("k2"))
		drRequireValue(t, db, []byte("k3"), []byte("v3-new"))
		drRequireValue(t, db, []byte("k4"), []byte("v4"))
		drRequireNotFound(t, db, []byte("k5"))
	})
}

func TestRecoveryWALReplayMixedBatchDeleteAndRangeDelete(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		dir := t.TempDir()
		opt := newTestOptions(t)
		opt.WorkDir = dir
		opt.MemTableEngine = engine
		opt.MemTableSize = 1 << 16
		opt.SSTableMaxSz = 1 << 20
		opt.ValueLogFileSize = 1 << 20
		opt.ValueThreshold = 1 << 20

		db := openTestDB(t, opt)
		require.NoError(t, db.SetBatch([]BatchSetItem{
			{Key: []byte("a"), Value: []byte("va")},
			{Key: []byte("b"), Value: []byte("vb")},
			{Key: []byte("c"), Value: []byte("vc")},
		}))
		require.NoError(t, db.DeleteRange([]byte("b"), []byte("d")))
		require.NoError(t, db.Set([]byte("c"), []byte("vc-new")))
		require.NoError(t, db.Del([]byte("a")))
		require.NoError(t, db.SetBatch([]BatchSetItem{
			{Key: []byte("d"), Value: []byte("vd")},
			{Key: []byte("e"), Value: []byte("ve")},
		}))

		drSimulateCrash(t, db)

		db2 := openTestDB(t, opt)
		defer func() { _ = db2.Close() }()
		drRequireNotFound(t, db2, []byte("a"))
		drRequireNotFound(t, db2, []byte("b"))
		drRequireValue(t, db2, []byte("c"), []byte("vc-new"))
		drRequireValue(t, db2, []byte("d"), []byte("vd"))
		drRequireValue(t, db2, []byte("e"), []byte("ve"))
	})
}

func TestRecoveryWALReplayIdempotentAcrossRepeatedReopen(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		dir := t.TempDir()
		opt := newTestOptions(t)
		opt.WorkDir = dir
		opt.MemTableEngine = engine
		opt.MemTableSize = 1 << 16

		db := openTestDB(t, opt)
		require.NoError(t, db.SetBatch([]BatchSetItem{
			{Key: []byte("k1"), Value: []byte("v1")},
			{Key: []byte("k2"), Value: []byte("v2")},
		}))
		require.NoError(t, db.DeleteRange([]byte("k2"), []byte("k3")))
		require.NoError(t, db.Set([]byte("k2"), []byte("v2-new")))
		drSimulateCrash(t, db)

		db2 := openTestDB(t, opt)
		drRequireValue(t, db2, []byte("k1"), []byte("v1"))
		drRequireValue(t, db2, []byte("k2"), []byte("v2-new"))
		// Replay same WAL one more time (without clean close) and verify no semantic drift.
		drSimulateCrash(t, db2)

		db3 := openTestDB(t, opt)
		defer func() { _ = db3.Close() }()
		drRequireValue(t, db3, []byte("k1"), []byte("v1"))
		drRequireValue(t, db3, []byte("k2"), []byte("v2-new"))
	})
}

func TestRecoveryWALReplayTruncatedTailBatchIsNotPartiallyApplied(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		dir := t.TempDir()
		opt := newTestOptions(t)
		opt.WorkDir = dir
		opt.MemTableEngine = engine
		// Both writes need to land in one WAL so the truncation hits the
		// batch tail rather than orphaning the anchor on a peer shard.
		// (anchor and the batch's first key hash to different shards
		// under N=4.)
		opt.LSMShardCount = 1

		db := openTestDB(t, opt)
		require.NoError(t, db.Set([]byte("anchor"), []byte("ok")))
		require.NoError(t, db.SetBatch([]BatchSetItem{
			{Key: []byte("b1"), Value: []byte("v1")},
			{Key: []byte("b2"), Value: []byte("v2")},
			{Key: []byte("b3"), Value: []byte("v3")},
		}))
		require.NoError(t, db.Close())

		seg := drFirstWALSegmentPath(t, dir)
		fi, err := os.Stat(seg)
		require.NoError(t, err)
		require.Greater(t, fi.Size(), int64(8))
		require.NoError(t, os.Truncate(seg, fi.Size()-3))

		db2 := openTestDB(t, opt)
		defer func() { _ = db2.Close() }()
		drRequireValue(t, db2, []byte("anchor"), []byte("ok"))

		found := 0
		for _, k := range []string{"b1", "b2", "b3"} {
			if _, err := db2.Get([]byte(k)); err == nil {
				found++
			}
		}
		require.True(t, found == 0 || found == 3, "batch replay must be atomic, found=%d", found)
	})
}

func TestRecoveryVlogPointerRoundTripAfterReopen(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		dir := t.TempDir()
		opt := newTestOptions(t)
		opt.WorkDir = dir
		opt.MemTableEngine = engine
		opt.ValueThreshold = 0
		opt.ValueLogFileSize = 1 << 16

		v1 := bytes.Repeat([]byte("A"), 1024)
		v2 := bytes.Repeat([]byte("B"), 1536)
		db := openTestDB(t, opt)
		require.NoError(t, db.Set([]byte("vp-1"), v1))
		require.NoError(t, db.Set([]byte("vp-2"), v2))
		require.NoError(t, db.Close())

		db = openTestDB(t, opt)
		defer func() { _ = db.Close() }()
		drRequireValue(t, db, []byte("vp-1"), v1)
		drRequireValue(t, db, []byte("vp-2"), v2)

		it := db.NewIterator(&index.Options{IsAsc: true, OnlyUseKey: true})
		defer func() { _ = it.Close() }()
		it.Seek([]byte("vp-1"))
		require.True(t, it.Valid())
		item, ok := it.Item().(*Item)
		require.True(t, ok)
		val, err := item.ValueCopy(nil)
		require.NoError(t, err)
		require.Equal(t, v1, val)
	})
}

func TestCloseAggregatesWalAndDirLockErrors(t *testing.T) {
	dir := t.TempDir()
	// Per-key affinity routes Set("k") to one of N shards; arm the fault
	// on every shard's wal-00001 second sync — only the routed shard fires.
	walSyncErr := errors.New("wal sync close error")
	dirCloseErr := errors.New("dir lock close error")
	rules := make([]vfs.FaultRule, 0, 8)
	for shard := range 8 {
		rules = append(rules, vfs.FailOnNthRule(
			vfs.OpFileSync,
			filepath.Join(dir, fmt.Sprintf("lsm-wal-%02d", shard), "00001.wal"),
			2, walSyncErr,
		))
	}
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(rules...))

	opt := newTestOptions(t)
	opt.WorkDir = dir
	opt.FS = fs
	db := openTestDB(t, opt)
	require.NoError(t, db.Set([]byte("k"), []byte("v")))

	realLock := db.dirLock
	db.dirLock = closeFunc(func() error {
		if realLock != nil {
			_ = realLock.Close()
		}
		return dirCloseErr
	})

	err := db.Close()
	require.Error(t, err)
	require.ErrorIs(t, err, walSyncErr)
	require.ErrorIs(t, err, dirCloseErr)
	require.True(t, db.IsClosed())
}

func TestFaultFSWriteFailureThenRecoverableReopen(t *testing.T) {
	dir := t.TempDir()
	// Per-key affinity routes the write to one of N shards; arm a fault
	// on every shard's wal-00001 — only one fires.
	injected := errors.New("wal write injected")
	rules := make([]vfs.FaultRule, 0, 8)
	for shard := range 8 {
		rules = append(rules, vfs.FailOnceRule(
			vfs.OpFileWrite,
			filepath.Join(dir, fmt.Sprintf("lsm-wal-%02d", shard), "00001.wal"),
			injected,
		))
	}
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(rules...))

	opt := newTestOptions(t)
	opt.WorkDir = dir
	opt.FS = fs
	opt.WALBufferSize = 256 << 10 // Force large write to hit injected error.

	db := openTestDB(t, opt)
	// Use a large payload to force bufio flush and hit underlying file Write.
	big := bytes.Repeat([]byte("w"), 512<<10)
	err := db.Set([]byte("first"), big)
	require.ErrorIs(t, err, injected)
	// A write-path IO error can poison the current writer state; verify restart
	// recovery instead of requiring same-process follow-up writes to succeed.
	err = db.Close()
	require.ErrorIs(t, err, injected)

	db = openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	_, err = db.Get([]byte("first"))
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.NoError(t, db.Set([]byte("second"), []byte("v2")))
	drRequireValue(t, db, []byte("second"), []byte("v2"))
}

func TestRecoveryTruncateFailureThenSucceedsWithHealthyFS(t *testing.T) {
	dir := t.TempDir()
	opt := newTestOptions(t)
	opt.WorkDir = dir

	db := openTestDB(t, opt)
	require.NoError(t, db.Set([]byte("anchor"), []byte("ok")))
	require.NoError(t, db.Close())

	seg := drFirstWALSegmentPath(t, dir)
	drAppendPartialWALTail(t, seg)

	truncErr := errors.New("truncate injected")
	faultFS := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpFileTrunc, seg, truncErr),
	))
	err := wal.VerifyDir(filepath.Dir(seg), faultFS)
	require.ErrorIs(t, err, truncErr)

	db = openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	drRequireValue(t, db, []byte("anchor"), []byte("ok"))
}

func TestConcurrentReadWriteFlushCompactionStress(t *testing.T) {
	drForEachMemTableEngine(t, func(t *testing.T, engine MemTableEngine) {
		opt := newTestOptions(t)
		opt.MemTableEngine = engine
		opt.MemTableSize = 4 << 10
		opt.NumLevelZeroTables = 8
		opt.NumCompactors = 2
		opt.WriteHotKeyLimit = 0
		db := openTestDB(t, opt)
		defer func() { _ = db.Close() }()

		const (
			writers = 4
			readers = 3
			ops     = 180
		)
		var wg sync.WaitGroup
		var writeErr atomic.Int64
		var readErr atomic.Int64
		for i := range writers {
			wg.Go(func() {
				rng := rand.New(rand.NewSource(int64(1000 + i)))
				for j := range ops {
					kid := rng.Intn(128)
					key := fmt.Appendf(nil, "k-%03d", kid)
					val := fmt.Appendf(nil, "v-%d-%d", i, j)
					if err := db.Set(key, val); err != nil {
						writeErr.Add(1)
					}
					if j%120 == 0 {
						start := fmt.Appendf(nil, "k-%03d", rng.Intn(96))
						end := fmt.Appendf(nil, "k-%03d", rng.Intn(31)+97)
						if bytes.Compare(start, end) < 0 {
							_ = db.DeleteRange(start, end)
						}
					}
				}
			})
		}
		for i := range readers {
			wg.Go(func() {
				rng := rand.New(rand.NewSource(int64(2000 + i)))
				for range ops {
					key := fmt.Appendf(nil, "k-%03d", rng.Intn(128))
					_, err := db.Get(key)
					if err != nil && !errors.Is(err, utils.ErrKeyNotFound) {
						readErr.Add(1)
					}
				}
			})
		}
		wg.Wait()
		require.EqualValues(t, 0, writeErr.Load())
		require.EqualValues(t, 0, readErr.Load())

		require.NoError(t, db.Set([]byte("tail"), []byte("ok")))
		drRequireValue(t, db, []byte("tail"), []byte("ok"))
	})
}

func TestValueSeparationPolicyDecisionLogic(t *testing.T) {
	var err error

	workDir, err := os.MkdirTemp("", "nokv-value-separation-test")
	require.NoError(t, err)
	defer func() {
		err = os.RemoveAll(workDir)
		require.NoError(t, err)
	}()

	inlinePolicy, err := kv.NewAlwaysInlinePolicy(kv.CFDefault, "meta_")
	require.NoError(t, err)
	require.NotNil(t, inlinePolicy)
	offloadPolicy, err := kv.NewAlwaysOffloadPolicy(kv.CFDefault, "large_")
	require.NoError(t, err)
	require.NotNil(t, offloadPolicy)
	thresholdPolicy, err := kv.NewThresholdBasedPolicy(kv.CFDefault, "medium_", 32)
	require.NoError(t, err)
	require.NotNil(t, thresholdPolicy)
	policies := []*kv.ValueSeparationPolicy{
		inlinePolicy,
		offloadPolicy,
		thresholdPolicy,
	}
	opt := &Options{
		WorkDir:                 workDir,
		MaxBatchCount:           3,
		MaxBatchSize:            1024,
		ValueThreshold:          32, // Global fallback threshold
		ValueSeparationPolicies: policies,
	}

	db, err := Open(opt)
	require.NoError(t, err)
	require.NotNil(t, db)
	defer func() {
		err = db.Close()
		require.NoError(t, err)
	}()

	largeValue := make([]byte, 128) // Larger than both thresholds
	for i := range largeValue {
		largeValue[i] = byte(i % 256)
	}

	// Test meta_ prefix (should be inlined regardless of size)
	entry := kv.NewInternalEntry(kv.CFDefault, []byte("meta_test"), nonTxnMaxVersion, largeValue, 0, 0)
	require.True(t, db.shouldWriteValueToLSM(entry))
	entry.DecrRef()

	// Test large_ prefix (should be offloaded regardless of size)
	entry = kv.NewInternalEntry(kv.CFDefault, []byte("large_test"), nonTxnMaxVersion, []byte("small"), 0, 0)
	require.False(t, db.shouldWriteValueToLSM(entry))
	entry.DecrRef()

	// Test medium_ prefix with small value (should be inlined due to threshold)
	entry = kv.NewInternalEntry(kv.CFDefault, []byte("medium_test1"), nonTxnMaxVersion, []byte("small"), 0, 0)
	require.True(t, db.shouldWriteValueToLSM(entry))
	entry.DecrRef()

	// Test medium_ prefix with large value (should be offloaded due to threshold)
	entry = kv.NewInternalEntry(kv.CFDefault, []byte("medium_test2"), nonTxnMaxVersion, largeValue, 0, 0)
	require.False(t, db.shouldWriteValueToLSM(entry))
	entry.DecrRef()

	// Test unmatched key with small value (should use global threshold)
	entry = kv.NewInternalEntry(kv.CFDefault, []byte("regular_test1"), nonTxnMaxVersion, []byte("small"), 0, 0)
	require.True(t, db.shouldWriteValueToLSM(entry))
	entry.DecrRef()

	// Test unmatched key with large value (should use global threshold)
	entry = kv.NewInternalEntry(kv.CFDefault, []byte("regular_test2"), nonTxnMaxVersion, largeValue, 0, 0)
	require.False(t, db.shouldWriteValueToLSM(entry))
	entry.DecrRef()
}

// TestSyncPipelineWALConsistency opens two DBs (one with SyncPipeline off, one
// with SyncPipeline on), writes the same keys, closes them, then compares the
// raw WAL file bytes to make sure they are identical.
func TestSyncPipelineWALConsistency(t *testing.T) {
	const numKeys = 10
	value := []byte("hello-sync-pipeline")

	readWALFiles := func(dir string) []byte {
		matches, err := filepath.Glob(filepath.Join(dir, "lsm-wal-*", "*.wal"))
		require.NoError(t, err)
		var all []byte
		for _, f := range matches {
			data, err := os.ReadFile(f)
			require.NoError(t, err)
			all = append(all, data...)
		}
		return all
	}

	writeAndClose := func(dir string, pipeline bool) {
		opts := NewDefaultOptions()
		opts.WorkDir = dir
		opts.SyncWrites = true
		opts.SyncPipeline = pipeline
		opts.EnableWALWatchdog = false
		opts.ValueLogGCInterval = 0
		opts.ManifestSync = false
		opts.ValueThreshold = 1 << 20
		opts.WriteBatchWait = 0

		db := openTestDB(t, opts)
		for i := range numKeys {
			key := fmt.Appendf(nil, "key-%04d", i)
			require.NoError(t, db.Set(key, value))
		}
		require.NoError(t, db.Close())
	}

	dirInline := t.TempDir()
	dirPipeline := t.TempDir()

	writeAndClose(dirInline, false)
	writeAndClose(dirPipeline, true)

	walInline := readWALFiles(dirInline)
	walPipeline := readWALFiles(dirPipeline)

	require.NotEmpty(t, walInline, "inline WAL should not be empty")
	require.NotEmpty(t, walPipeline, "pipeline WAL should not be empty")
	require.Equal(t, walInline, walPipeline, "WAL file contents should be identical between SyncPipeline=false and SyncPipeline=true")
}

func TestSendToWriteChWaitsForThrottleClear(t *testing.T) {
	opts := newTestOptions(t)
	opts.WriteBatchWait = 0
	db := openTestDB(t, opts)
	defer func() { _ = db.Close() }()

	db.applyThrottle(lsm.WriteThrottleStop)
	defer db.applyThrottle(lsm.WriteThrottleNone)

	done := make(chan error, 1)
	go func() {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("throttle-clear"), 1, []byte("value"), 0, 0)
		req, err := db.sendToWriteCh([]*kv.Entry{entry}, true)
		if err != nil {
			entry.DecrRef()
			done <- err
			return
		}
		done <- req.Wait()
	}()

	select {
	case err := <-done:
		t.Fatalf("write finished before throttle cleared: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	db.applyThrottle(lsm.WriteThrottleNone)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("write did not resume after throttle cleared")
	}
}

func TestSendToWriteChReturnsBlockedWritesWhenClosedWhileThrottled(t *testing.T) {
	opts := newTestOptions(t)
	opts.WriteBatchWait = 0
	db := openTestDB(t, opts)

	db.applyThrottle(lsm.WriteThrottleStop)

	done := make(chan error, 1)
	go func() {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("throttle-close"), 1, []byte("value"), 0, 0)
		_, err := db.sendToWriteCh([]*kv.Entry{entry}, true)
		if err != nil {
			entry.DecrRef()
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("write finished before db close: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, db.Close())

	select {
	case err := <-done:
		require.ErrorIs(t, err, utils.ErrBlockedWrites)
	case <-time.After(2 * time.Second):
		t.Fatal("throttled write did not return after db close")
	}
}

func TestDBWrapperNilAndOpenGuards(t *testing.T) {
	var nilDB *DB

	require.Nil(t, nilDB.ExternalSSTOptions())
	_, err := nilDB.ImportExternalSST([]string{"x.sst"})
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	require.ErrorContains(t, nilDB.RollbackExternalSST([]uint64{1}), "snapshot bridge requires open db")
	_, err = nilDB.ExportSnapshotDir(t.TempDir(), localmeta.RegionMeta{})
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	_, err = nilDB.ImportSnapshotDir(t.TempDir())
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	_, err = nilDB.ExportSnapshot(localmeta.RegionMeta{})
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	_, err = nilDB.ExportSnapshotTo(bytes.NewBuffer(nil), localmeta.RegionMeta{})
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	_, err = nilDB.ImportSnapshot([]byte("payload"))
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	_, err = nilDB.ImportSnapshotFrom(bytes.NewReader(nil))
	require.ErrorContains(t, err, "snapshot bridge requires open db")

	require.Nil(t, nilDB.RaftLog())
	require.ErrorContains(t, nilDB.SyncWAL(), "wal is unavailable")
	require.ErrorContains(t, nilDB.ReplayWAL(nil), "wal is unavailable")

	_, err = nilDB.MaterializeInternalEntry(nil)
	require.EqualError(t, err, "db is nil")

	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	require.NotNil(t, db.ExternalSSTOptions())
	require.NotNil(t, db.RaftLog())
	require.Nil(t, db.GetValueSeparationPolicyStats())

	_, err = db.MaterializeInternalEntry(nil)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	db.isClosed.Store(1)
	require.Nil(t, db.ExternalSSTOptions())
	_, err = db.ImportExternalSST([]string{"x.sst"})
	require.ErrorContains(t, err, "snapshot bridge requires open db")
	require.ErrorContains(t, db.RollbackExternalSST([]uint64{1}), "snapshot bridge requires open db")
	_, err = db.ExportSnapshot(localmeta.RegionMeta{})
	require.ErrorContains(t, err, "snapshot bridge requires open db")
}

func TestRaftLogUsesShardedWAL(t *testing.T) {
	dir := t.TempDir()
	localMeta, err := localmeta.OpenLocalStore(filepath.Join(dir, "raftmeta"), nil)
	require.NoError(t, err)
	defer func() { _ = localMeta.Close() }()

	opt := NewDefaultOptions()
	opt.WorkDir = dir
	opt.EnableWALWatchdog = false
	opt.ValueLogGCInterval = 0
	opt.RaftPointerSnapshot = localMeta.RaftPointerSnapshot
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	storage, err := db.RaftLog().Open(9, localMeta)
	require.NoError(t, err)
	require.NoError(t, storage.Append([]myraft.Entry{{Index: 1, Term: 1, Data: []byte("raft")}}))

	for _, mgr := range db.lsmWALs {
		require.Equal(t, uint64(0), mgr.Metrics().RecordCounts.RaftEntries)
	}
	shard := raftWALShard(9)
	matches, err := filepath.Glob(filepath.Join(dir, fmt.Sprintf("raft-wal-%02d", shard), "*.wal"))
	require.NoError(t, err)
	require.NotEmpty(t, matches)
}

// TestDBValueLogDisabledByDefault locks the new opt-in semantics: with
// EnableValueLog left at its default false, NoKV opens without spinning
// up vlog managers, never creates the WorkDir/vlog directory, and
// inlines every value regardless of size. This is the metadata-first
// configuration the slab-substrate redesign promises (see
// docs/notes/2026-04-27-slab-substrate.md).
func TestDBValueLogDisabledByDefault(t *testing.T) {
	dir := t.TempDir()
	cfg := NewDefaultOptions()
	cfg.WorkDir = dir
	require.False(t, cfg.EnableValueLog, "NewDefaultOptions must default EnableValueLog to false")

	db, err := Open(cfg)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.Nil(t, db.vlog, "vlog must not be initialized when EnableValueLog is false")

	// A value larger than ValueThreshold would otherwise be sent to vlog;
	// with vlog disabled the engine inlines it.
	bigValue := make([]byte, 8<<10) // 8 KiB > default 2 KiB threshold
	for i := range bigValue {
		bigValue[i] = byte(i & 0xff)
	}
	require.NoError(t, db.Set([]byte("big"), bigValue))
	got, err := db.Get([]byte("big"))
	require.NoError(t, err)
	require.Equal(t, bigValue, got.Value)

	// vlog directory must not exist on disk.
	_, err = os.Stat(filepath.Join(dir, "vlog"))
	require.ErrorIs(t, err, os.ErrNotExist, "vlog dir must not be created when EnableValueLog is false")

	// RunValueLogGC must be a no-op rather than panicking.
	require.NoError(t, db.RunValueLogGC(0.5))
}

// TestDBValueLogEnabledRoundTrip verifies that explicitly enabling vlog
// still produces a working value-separation path: a >threshold value is
// stored and read back identically, and the vlog directory exists on
// disk. This is the migration-friendly opt-in flow.
func TestDBValueLogEnabledRoundTrip(t *testing.T) {
	dir := t.TempDir()
	cfg := NewDefaultOptions()
	cfg.WorkDir = dir
	cfg.EnableValueLog = true
	cfg.ValueThreshold = 64
	cfg.ValueLogFileSize = 1 << 20
	cfg.ValueLogBucketCount = 1

	db, err := Open(cfg)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	require.NotNil(t, db.vlog, "vlog must be initialized when EnableValueLog is true")

	bigValue := make([]byte, 4<<10)
	for i := range bigValue {
		bigValue[i] = byte(i)
	}
	require.NoError(t, db.Set([]byte("big-vlog"), bigValue))
	got, err := db.Get([]byte("big-vlog"))
	require.NoError(t, err)
	require.Equal(t, bigValue, got.Value)

	_, err = os.Stat(filepath.Join(dir, "vlog"))
	require.NoError(t, err, "vlog dir must exist when EnableValueLog is true")
}
