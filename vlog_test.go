package NoKV

import (
	"bytes"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	kvpkg "github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	vlogpkg "github.com/feichai0017/NoKV/engine/vlog"
	dbruntime "github.com/feichai0017/NoKV/internal/runtime"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var (
	// Test options for value log tests.
	opt = &Options{
		WorkDir:             "./work_test",
		SSTableMaxSz:        1 << 10,
		MemTableSize:        1 << 10,
		EnableValueLog:      true, // these tests exercise the vlog path
		ValueLogFileSize:    1 << 20,
		ValueThreshold:      0,
		ValueLogBucketCount: 1,
		MaxBatchCount:       10,
		MaxBatchSize:        1 << 20,
		ThermosEnabled:      true,
		ThermosBits:         8,
		ThermosTopK:         8,
	}
)

func newPointerEntry(userKey, value []byte) *kvpkg.Entry {
	return kvpkg.NewInternalEntry(kvpkg.CFDefault, userKey, nonTxnMaxVersion, value, kvpkg.BitValuePointer, 0)
}

func TestVlogBase(t *testing.T) {
	// Clean work directory.
	clearDir()
	// Open DB.
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	log := db.vlog
	var err error
	// Create a simple key/value entry.
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, int64(len(val1)) >= db.opt.ValueThreshold)

	e1 := newPointerEntry([]byte("samplekey"), []byte(val1))
	e2 := newPointerEntry([]byte("samplekeyb"), []byte(val2))

	// Build a batched request.
	b := new(dbruntime.Request)
	b.Entries = []*kvpkg.Entry{e1, e2}

	// Write directly into the value log.
	require.NoError(t, log.write([]*dbruntime.Request{b}))
	e1.DecrRef()
	e2.DecrRef()
	require.Len(t, b.Ptrs, 2)
	t.Logf("Pointer written: %+v %+v\n", b.Ptrs[0], b.Ptrs[1])

	// Read back the value log entries via value pointers.
	mgr1, err := log.managerFor(b.Ptrs[0].Bucket)
	require.NoError(t, err)
	mgr2, err := log.managerFor(b.Ptrs[1].Bucket)
	require.NoError(t, err)
	payload1, unlock1, err1 := mgr1.Read(&b.Ptrs[0])
	payload2, unlock2, err2 := mgr2.Read(&b.Ptrs[1])
	require.NoError(t, err1)
	require.NoError(t, err2)
	// Release callbacks.
	if unlock1 != nil {
		defer unlock1()
	}
	if unlock2 != nil {
		defer unlock2()
	}
	entry1, err := kvpkg.DecodeEntry(payload1)
	require.NoError(t, err)
	defer entry1.DecrRef()
	entry2, err := kvpkg.DecodeEntry(payload2)
	require.NoError(t, err)
	defer entry2.DecrRef()

	// Compare the fields we care about.
	_, key1, ts1, ok := kvpkg.SplitInternalKey(entry1.Key)
	require.True(t, ok)
	require.Equal(t, []byte("samplekey"), key1)
	require.Equal(t, nonTxnMaxVersion, ts1)
	require.Equal(t, []byte(val1), entry1.Value)
	require.Equal(t, kvpkg.BitValuePointer, entry1.Meta)

	_, key2, ts2, ok := kvpkg.SplitInternalKey(entry2.Key)
	require.True(t, ok)
	require.Equal(t, []byte("samplekeyb"), key2)
	require.Equal(t, nonTxnMaxVersion, ts2)
	require.Equal(t, []byte(val2), entry2.Value)
	require.Equal(t, kvpkg.BitValuePointer, entry2.Meta)
}

func TestVersionedEntryValueLogPointer(t *testing.T) {
	clearDir()
	prevThreshold := opt.ValueThreshold
	prevFileSize := opt.ValueLogFileSize
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 1 << 20
	defer func() {
		opt.ValueThreshold = prevThreshold
		opt.ValueLogFileSize = prevFileSize
	}()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-vlog")
	version := uint64(7)
	value := bytes.Repeat([]byte("v"), 64)

	applyVersionedEntryForTest(t, db, kvpkg.CFDefault, key, version, value, 0)
	entry, err := db.GetInternalEntry(kvpkg.CFDefault, key, version)
	require.NoError(t, err)
	require.Equal(t, kvpkg.CFDefault, entry.CF)
	_, userKey, _, ok := kvpkg.SplitInternalKey(entry.Key)
	require.True(t, ok)
	require.Equal(t, key, userKey)
	require.Equal(t, version, kvpkg.Timestamp(entry.Key))
	require.Equal(t, value, entry.Value)
	entry.DecrRef()
}

func TestVlogSyncWritesCoversAllSegments(t *testing.T) {
	clearDir()

	prevSync := opt.SyncWrites
	prevThreshold := opt.ValueThreshold
	prevFileSize := opt.ValueLogFileSize
	opt.SyncWrites = true
	opt.ValueThreshold = 0
	opt.ValueLogFileSize = 256
	defer func() {
		opt.SyncWrites = prevSync
		opt.ValueThreshold = prevThreshold
		opt.ValueLogFileSize = prevFileSize
	}()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	log := db.vlog

	payload := bytes.Repeat([]byte("v"), 180)
	e1 := newPointerEntry([]byte("sync-key-1"), payload)
	e2 := newPointerEntry([]byte("sync-key-2"), payload)
	req := &dbruntime.Request{Entries: []*kvpkg.Entry{e1, e2}}

	require.NoError(t, log.write([]*dbruntime.Request{req}))
	e1.DecrRef()
	e2.DecrRef()

	if len(req.Ptrs) != 2 {
		t.Fatalf("expected 2 value pointers, got %d", len(req.Ptrs))
	}
	if req.Ptrs[0].Fid == req.Ptrs[1].Fid && req.Ptrs[0].Bucket == req.Ptrs[1].Bucket {
		t.Fatalf("expected pointers in different vlog segments or buckets, got fid=%d bucket=%d", req.Ptrs[0].Fid, req.Ptrs[0].Bucket)
	}
}

func clearDir() {
	if opt == nil {
		return
	}
	if opt.WorkDir != "" {
		_ = os.RemoveAll(opt.WorkDir)
	}
	dir, err := os.MkdirTemp("", "nokv-vlog-test-")
	if err != nil {
		panic(err)
	}
	opt.WorkDir = dir
}

func keyForBucket(t *testing.T, bucket int, buckets int) []byte {
	t.Helper()
	for i := range 10000 {
		userKey := fmt.Appendf(nil, "gc-bucket-key-%d", i)
		internal := kvpkg.InternalKey(kvpkg.CFDefault, userKey, 1)
		if kvpkg.ValueLogBucket(internal, uint32(buckets)) == uint32(bucket) {
			return userKey
		}
	}
	t.Fatalf("unable to find key for bucket %d", bucket)
	return nil
}

func TestValueGC(t *testing.T) {
	clearDir()
	opt.ValueLogFileSize = 1 << 20
	origCompactors := opt.NumCompactors
	origMemTableSize := opt.MemTableSize
	origSSTableMaxSz := opt.SSTableMaxSz
	opt.NumCompactors = 0
	opt.MemTableSize = 8 << 20
	opt.SSTableMaxSz = 8 << 20
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()
	defer func() {
		opt.NumCompactors = origCompactors
		opt.MemTableSize = origMemTableSize
		opt.SSTableMaxSz = origSSTableMaxSz
	}()
	sz := 32 << 10
	kvList := make([]*kvpkg.Entry, 0, 100)
	defer func() {
		for _, e := range kvList {
			e.DecrRef()
		}
	}()

	for range 100 {
		e := newRandEntry(sz)
		eCopy := kvpkg.NewEntry(e.Key, e.Value)
		eCopy.Meta = e.Meta
		eCopy.ExpiresAt = e.ExpiresAt
		kvList = append(kvList, eCopy)

		if e.ExpiresAt > 0 {
			ttl := time.Until(time.Unix(int64(e.ExpiresAt), 0))
			require.NoError(t, db.SetWithTTL(e.Key, e.Value, ttl))
		} else {
			require.NoError(t, db.Set(e.Key, e.Value))
		}
		e.DecrRef()
	}
	if err := db.RunValueLogGC(0.9); err != nil && !errors.Is(err, utils.ErrNoRewrite) {
		require.NoError(t, err)
	}
	for _, e := range kvList {
		item, err := db.Get(e.Key)
		require.NoErrorf(t, err, "missing key after gc user_key=%q raw=%x", string(e.Key), e.Key)
		val := getItemValue(t, item)
		require.NotNil(t, val)
		require.True(t, bytes.Equal(item.Key, e.Key), "key not equal: e:%s, v:%s", e.Key, item.Key)
		require.True(t, bytes.Equal(item.Value, e.Value), "value not equal: e:%s, v:%s", e.Value, item.Key)
	}
}

func TestValueLogGCParallelScheduling(t *testing.T) {
	cfg := *opt
	cfg.WorkDir = t.TempDir()
	cfg.ValueThreshold = 0
	cfg.ValueLogBucketCount = 2
	cfg.ValueLogFileSize = 4 << 10
	cfg.ValueLogGCParallelism = 2
	cfg.ValueLogGCInterval = 0
	cfg.NumCompactors = 2

	db := openTestDB(t, &cfg)
	defer func() { _ = db.Close() }()

	key0 := keyForBucket(t, 0, cfg.ValueLogBucketCount)
	key1 := keyForBucket(t, 1, cfg.ValueLogBucketCount)
	payload := bytes.Repeat([]byte("x"), 512)

	hasSealed := func() bool {
		for bucket := 0; bucket < cfg.ValueLogBucketCount; bucket++ {
			mgr, err := db.vlog.managerFor(uint32(bucket))
			require.NoError(t, err)
			if len(mgr.ListFIDs()) < 2 {
				return false
			}
		}
		return true
	}

	for i := 0; i < 200 && !hasSealed(); i++ {
		require.NoError(t, db.Set(key0, payload))
		require.NoError(t, db.Set(key1, payload))
	}
	require.True(t, hasSealed(), "expected sealed vlog segments in each bucket")

	metrics.ResetValueLogGCMetricsForTesting()
	before := metrics.DefaultValueLogGCCollector().Snapshot().GCScheduled

	_ = db.RunValueLogGC(0.99)

	after := metrics.DefaultValueLogGCCollector().Snapshot().GCScheduled
	if after-before < 2 {
		t.Fatalf("expected parallel GC scheduling, delta=%d", after-before)
	}
}

func TestValueLogIterateReleasesEntries(t *testing.T) {
	clearDir()
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	val := bytes.Repeat([]byte("x"), 128)
	entry := kvpkg.NewEntry([]byte("iter-key"), val)
	require.NoError(t, db.Set(entry.Key, entry.Value))
	entry.DecrRef()

	vlog := db.vlog
	active := vlog.managers[0].ActiveFID()

	var captured []*kvpkg.Entry
	_, err := vlog.managers[0].Iterate(active, kvpkg.ValueLogHeaderSize, func(e *kvpkg.Entry, vp *kvpkg.ValuePtr) error {
		captured = append(captured, e)
		return nil
	})
	require.NoError(t, err)
	require.NotZero(t, len(captured), "expected to capture at least one entry")

	for _, e := range captured {
		if len(e.Key) != 0 || len(e.Value) != 0 {
			t.Fatalf("expected entry to be reset after DecrRef")
		}
	}
}

func TestDecodeWalEntryReleasesEntries(t *testing.T) {
	orig := kvpkg.NewEntry([]byte("decode-key"), []byte("decode-val"))
	buf := &bytes.Buffer{}
	payload, err := kvpkg.EncodeEntry(buf, orig)
	require.NoError(t, err)
	orig.DecrRef()

	entry, err := kvpkg.DecodeEntry(payload)
	require.NoError(t, err)
	entry.DecrRef()

	if len(entry.Key) != 0 || len(entry.Value) != 0 {
		t.Fatalf("expected decoded entry to reset after DecrRef")
	}
}

func TestValueLogWriteAppendFailureRewinds(t *testing.T) {
	clearDir()
	cfg := *opt
	cfg.ValueLogFileSize = 256
	injected := errors.New("append failure")
	rotatePath := filepath.Join(cfg.WorkDir, "vlog", "bucket-000", "00001.vlog")
	cfg.FS = vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpOpenFile, rotatePath, injected),
	))
	db := openTestDB(t, &cfg)
	defer func() { _ = db.Close() }()

	head := db.vlog.managers[0].Head()

	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	entries := []*kvpkg.Entry{
		newPointerEntry([]byte("afail"), bytes.Repeat([]byte("a"), 512)),
		newPointerEntry([]byte("bfail"), bytes.Repeat([]byte("b"), 512)),
	}
	req.LoadEntries(entries)
	req.IncrRef()
	defer req.DecrRef()

	err := db.vlog.write([]*dbruntime.Request{req})
	require.Error(t, err)
	require.ErrorIs(t, err, injected)
	require.Equal(t, head, db.vlog.managers[0].Head())
	require.Len(t, req.Ptrs, 0)
}

func TestValueLogWriteRotateFailureRewinds(t *testing.T) {
	clearDir()
	cfg := *opt
	cfg.ValueLogFileSize = 256
	injected := errors.New("rotate failure")
	rotatePath := filepath.Join(cfg.WorkDir, "vlog", "bucket-000", "00001.vlog")
	cfg.FS = vfs.NewFaultFSWithPolicy(vfs.OSFS{}, vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpOpenFile, rotatePath, injected),
	))
	db := openTestDB(t, &cfg)
	defer func() { _ = db.Close() }()

	head := db.vlog.managers[0].Head()

	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	entries := []*kvpkg.Entry{
		newPointerEntry([]byte("rfail1"), bytes.Repeat([]byte("x"), 512)),
		newPointerEntry([]byte("rfail2"), bytes.Repeat([]byte("y"), 512)),
	}
	req.LoadEntries(entries)
	req.IncrRef()
	defer req.DecrRef()

	err := db.vlog.write([]*dbruntime.Request{req})
	require.Error(t, err)
	require.ErrorIs(t, err, injected)
	require.Equal(t, head, db.vlog.managers[0].Head())
	require.Len(t, req.Ptrs, 0)
}

func TestValueLogWriteInlineRequestSkipsPtrs(t *testing.T) {
	clearDir()
	prevThreshold := opt.ValueThreshold
	opt.ValueThreshold = 1 << 20
	defer func() { opt.ValueThreshold = prevThreshold }()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	req := dbruntime.RequestPool.Get().(*dbruntime.Request)
	req.Reset()
	entry := kvpkg.NewInternalEntry(kvpkg.CFDefault, []byte("inline-vlog"), nonTxnMaxVersion, []byte("v"), 0, 0)
	req.LoadEntries([]*kvpkg.Entry{entry})
	req.IncrRef()
	defer req.DecrRef()

	require.NoError(t, db.vlog.write([]*dbruntime.Request{req}))
	require.Len(t, req.Ptrs, 0)
	require.Len(t, req.PtrIdxs, 0)
	require.Len(t, req.PtrBuckets, 0)
}

func TestValueLogReadCopiesSmallValue(t *testing.T) {
	clearDir()
	prevThreshold := opt.ValueThreshold
	opt.ValueThreshold = 0
	defer func() { opt.ValueThreshold = prevThreshold }()

	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kvpkg.NewInternalEntry(kvpkg.CFDefault, []byte("small-read"), nonTxnMaxVersion, []byte("v"), 0, 0)
	vp, err := db.vlog.newValuePtr(entry)
	entry.DecrRef()
	require.NoError(t, err)

	val, cb, err := db.vlog.read(vp)
	require.NoError(t, err)
	require.Nil(t, cb)
	require.Equal(t, []byte("v"), val)
}

func newRandEntry(sz int) *kvpkg.Entry {
	v := make([]byte, sz)
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	_, _ = rng.Read(v[:rng.Intn(sz)])
	key := fmt.Appendf(nil, "vlog-rand-%d-%d", time.Now().UnixNano(), rng.Uint64())
	e := kvpkg.NewEntry(key, v)
	e.ExpiresAt = uint64(time.Now().Add(12 * time.Hour).Unix())
	return e
}
func getItemValue(t *testing.T, item *kvpkg.Entry) (val []byte) {
	t.Helper()
	if item == nil {
		return nil
	}
	var v []byte
	v = append(v, item.Value...)
	if v == nil {
		return nil
	}
	return v
}

func TestManifestHeadMatchesValueLogHead(t *testing.T) {
	clearDir()
	opt.ValueThreshold = 0
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	entry := kvpkg.NewEntry([]byte("manifest-head"), []byte("value"))
	entry.Key = kvpkg.InternalKey(kvpkg.CFDefault, entry.Key, math.MaxUint32)
	if err := db.batchSet([]*kvpkg.Entry{entry}); err != nil {
		t.Fatalf("batchSet: %v", err)
	}

	head := db.vlog.managers[0].Head()
	heads := db.getHeads()
	meta, ok := heads[0]
	if !ok {
		t.Fatalf("expected manifest head")
	}
	if meta.Fid != head.Fid {
		t.Fatalf("manifest fid %d does not match manager %d", meta.Fid, head.Fid)
	}
	if meta.Offset != head.Offset {
		t.Fatalf("manifest offset %d does not match manager %d", meta.Offset, head.Offset)
	}
}

func TestValueLogReconcileManifestRemovesInvalid(t *testing.T) {
	tmp := t.TempDir()
	mgr, err := vlogpkg.Open(vlogpkg.Config{Dir: tmp, FileMode: utils.DefaultFileMode, MaxSize: 1 << 20, Bucket: 0})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	require.NoError(t, mgr.Rotate())

	vlog := &valueLog{
		dirPath:     tmp,
		bucketCount: 1,
		managers:    []*vlogpkg.Manager{mgr},
	}

	vlog.reconcileManifest(map[manifest.ValueLogID]manifest.ValueLogMeta{
		{Bucket: 0, FileID: 0}: {Bucket: 0, FileID: 0, Valid: false},
	})

	_, err = os.Stat(filepath.Join(tmp, "00000.vlog"))
	require.Error(t, err)
}

func TestValueLogGCSkipBlocked(t *testing.T) {
	clearDir()
	opt := NewDefaultOptions()
	opt.ValueLogFileSize = 1 << 20
	opt.NumCompactors = 0
	db := openTestDB(t, opt)
	defer func() { _ = db.Close() }()

	e := kvpkg.NewEntry([]byte("gc-skip"), []byte("v"))
	require.NoError(t, db.Set(e.Key, e.Value))
	e.DecrRef()

	db.applyThrottle(lsm.WriteThrottleStop)
	defer db.applyThrottle(lsm.WriteThrottleNone)

	if err := db.RunValueLogGC(0.5); err != nil && !errors.Is(err, utils.ErrNoRewrite) {
		t.Fatalf("expected ErrNoRewrite when writes blocked, got %v", err)
	}
}
