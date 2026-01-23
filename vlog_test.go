package NoKV

import (
	"bytes"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	kvpkg "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var (
	// Test options for value log tests.
	opt = &Options{
		WorkDir:          "./work_test",
		SSTableMaxSz:     1 << 10,
		MemTableSize:     1 << 10,
		ValueLogFileSize: 1 << 20,
		ValueThreshold:   0,
		MaxBatchCount:    10,
		MaxBatchSize:     1 << 20,
		HotRingEnabled:   true,
		HotRingBits:      8,
		HotRingTopK:      8,
	}
)

func TestVlogBase(t *testing.T) {
	// Clean work directory.
	clearDir()
	// Open DB.
	db := Open(opt)
	defer db.Close()
	log := db.vlog
	var err error
	// Create a simple key/value entry.
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, int64(len(val1)) >= db.opt.ValueThreshold)

	e1 := kvpkg.NewEntry([]byte("samplekey"), []byte(val1))
	e1.Meta = kvpkg.BitValuePointer

	e2 := kvpkg.NewEntry([]byte("samplekeyb"), []byte(val2))
	e2.Meta = kvpkg.BitValuePointer

	// Build a batched request.
	b := new(request)
	b.Entries = []*kvpkg.Entry{e1, e2}

	// Write directly into the value log.
	require.NoError(t, log.write([]*request{b}))
	e1.DecrRef()
	e2.DecrRef()
	require.Len(t, b.Ptrs, 2)
	t.Logf("Pointer written: %+v %+v\n", b.Ptrs[0], b.Ptrs[1])

	// Read back the value log entries via value pointers.
	payload1, unlock1, err1 := log.manager.Read(&b.Ptrs[0])
	payload2, unlock2, err2 := log.manager.Read(&b.Ptrs[1])
	require.NoError(t, err1)
	require.NoError(t, err2)
	// Release callbacks.
	defer kvpkg.RunCallback(unlock1)
	defer kvpkg.RunCallback(unlock2)
	entry1, err := kvpkg.DecodeEntry(payload1)
	require.NoError(t, err)
	defer entry1.DecrRef()
	entry2, err := kvpkg.DecodeEntry(payload2)
	require.NoError(t, err)
	defer entry2.DecrRef()

	// Compare the fields we care about.
	require.Equal(t, []byte("samplekey"), entry1.Key)
	require.Equal(t, []byte(val1), entry1.Value)
	require.Equal(t, kvpkg.BitValuePointer, entry1.Meta)

	require.Equal(t, []byte("samplekeyb"), entry2.Key)
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

	db := Open(opt)
	defer db.Close()

	key := []byte("versioned-vlog")
	version := uint64(7)
	value := bytes.Repeat([]byte("v"), 64)

	require.NoError(t, db.SetVersionedEntry(kvpkg.CFDefault, key, version, value, 0))
	entry, err := db.GetVersionedEntry(kvpkg.CFDefault, key, version)
	require.NoError(t, err)
	require.Equal(t, kvpkg.CFDefault, entry.CF)
	require.Equal(t, key, entry.Key)
	require.Equal(t, version, entry.Version)
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

	db := Open(opt)
	defer db.Close()
	log := db.vlog

	var mu sync.Mutex
	synced := make(map[uint32]int)
	log.manager.SetTestingHooks(vlogpkg.ManagerTestingHooks{
		BeforeSync: func(_ *vlogpkg.Manager, fid uint32) error {
			mu.Lock()
			synced[fid]++
			mu.Unlock()
			return nil
		},
	})

	payload := bytes.Repeat([]byte("v"), 180)
	e1 := kvpkg.NewEntry([]byte("sync-key-1"), payload)
	e2 := kvpkg.NewEntry([]byte("sync-key-2"), payload)
	req := &request{Entries: []*kvpkg.Entry{e1, e2}}

	require.NoError(t, log.write([]*request{req}))
	e1.DecrRef()
	e2.DecrRef()

	if len(req.Ptrs) != 2 {
		t.Fatalf("expected 2 value pointers, got %d", len(req.Ptrs))
	}
	if req.Ptrs[0].Fid == req.Ptrs[1].Fid {
		t.Fatalf("expected pointers in different vlog segments, got fid=%d", req.Ptrs[0].Fid)
	}

	mu.Lock()
	syncedCount := len(synced)
	mu.Unlock()
	if syncedCount < 2 {
		t.Fatalf("expected sync for multiple segments, got %d", syncedCount)
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

func TestValueGC(t *testing.T) {
	clearDir()
	opt.ValueLogFileSize = 1 << 20
	origCompactors := opt.NumCompactors
	opt.NumCompactors = 0
	db := Open(opt)
	defer db.Close()
	defer func() { opt.NumCompactors = origCompactors }()
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

		require.NoError(t, db.setEntry(e))
		e.DecrRef()
	}
	require.NoError(t, db.RunValueLogGC(0.9))
	for _, e := range kvList {
		item, err := db.Get(e.Key)
		require.NoError(t, err)
		val := getItemValue(t, item)
		require.NotNil(t, val)
		require.True(t, bytes.Equal(item.Key, e.Key), "key not equal: e:%s, v:%s", e.Key, item.Key)
		require.True(t, bytes.Equal(item.Value, e.Value), "value not equal: e:%s, v:%s", e.Value, item.Key)
		item.DecrRef()
	}
}

func TestValueLogIterateReleasesEntries(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer db.Close()

	txn := db.NewTransaction(true)
	defer txn.Discard()
	val := bytes.Repeat([]byte("x"), 128)
	require.NoError(t, txn.SetEntry(kvpkg.NewEntry([]byte("iter-key"), val)))
	require.NoError(t, txn.Commit())

	vlog := db.vlog
	active := vlog.manager.ActiveFID()

	var captured []*kvpkg.Entry
	_, err := vlog.manager.Iterate(active, kvpkg.ValueLogHeaderSize, func(e *kvpkg.Entry, vp *kvpkg.ValuePtr) error {
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
	db := Open(&cfg)
	defer db.Close()

	head := db.vlog.manager.Head()
	var calls int
	db.vlog.manager.SetTestingHooks(vlogpkg.ManagerTestingHooks{
		BeforeAppend: func(m *vlogpkg.Manager, data []byte) error {
			calls++
			if calls == 2 {
				return errors.New("append failure")
			}
			return nil
		},
	})
	defer db.vlog.manager.SetTestingHooks(vlogpkg.ManagerTestingHooks{})

	req := requestPool.Get().(*request)
	req.reset()
	entries := []*kvpkg.Entry{
		kvpkg.NewEntry([]byte("afail"), bytes.Repeat([]byte("a"), 64)),
		kvpkg.NewEntry([]byte("bfail"), bytes.Repeat([]byte("b"), 64)),
	}
	req.loadEntries(entries)
	req.IncrRef()
	defer req.DecrRef()

	err := db.vlog.write([]*request{req})
	require.Error(t, err)
	require.Equal(t, head, db.vlog.manager.Head())
	require.Len(t, req.Ptrs, 0)
}

func TestValueLogWriteRotateFailureRewinds(t *testing.T) {
	clearDir()
	cfg := *opt
	db := Open(&cfg)
	defer db.Close()

	head := db.vlog.manager.Head()
	db.vlog.setValueLogFileSize(256)
	var rotates int
	db.vlog.manager.SetTestingHooks(vlogpkg.ManagerTestingHooks{
		BeforeRotate: func(m *vlogpkg.Manager) error {
			rotates++
			if rotates == 1 {
				return errors.New("rotate failure")
			}
			return nil
		},
	})
	defer db.vlog.manager.SetTestingHooks(vlogpkg.ManagerTestingHooks{})

	req := requestPool.Get().(*request)
	req.reset()
	entries := []*kvpkg.Entry{
		kvpkg.NewEntry([]byte("rfail1"), bytes.Repeat([]byte("x"), 512)),
		kvpkg.NewEntry([]byte("rfail2"), bytes.Repeat([]byte("y"), 512)),
	}
	req.loadEntries(entries)
	req.IncrRef()
	defer req.DecrRef()

	err := db.vlog.write([]*request{req})
	require.Error(t, err)
	require.Equal(t, head, db.vlog.manager.Head())
	require.Len(t, req.Ptrs, 0)
	require.Equal(t, 1, rotates)
}

func TestValueLogReadCopiesSmallValue(t *testing.T) {
	clearDir()
	prevThreshold := opt.ValueThreshold
	opt.ValueThreshold = 0
	defer func() { opt.ValueThreshold = prevThreshold }()

	db := Open(opt)
	defer db.Close()

	entry := kvpkg.NewEntry([]byte("small-read"), []byte("v"))
	entry.Key = kvpkg.InternalKey(kvpkg.CFDefault, entry.Key, nonTxnMaxVersion)
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
	e := utils.BuildEntry()
	e.Value = v
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
	db := Open(opt)
	defer func() { _ = db.Close() }()

	entry := kvpkg.NewEntry([]byte("manifest-head"), []byte("value"))
	entry.Key = kvpkg.KeyWithTs(entry.Key, math.MaxUint32)
	if err := db.batchSet([]*kvpkg.Entry{entry}); err != nil {
		t.Fatalf("batchSet: %v", err)
	}

	head := db.vlog.manager.Head()
	if meta, ok := db.lsm.ValueLogHead(); !ok {
		t.Fatalf("expected manifest head")
	} else {
		if meta.Fid != head.Fid {
			t.Fatalf("manifest fid %d does not match manager %d", meta.Fid, head.Fid)
		}
		if meta.Offset != head.Offset {
			t.Fatalf("manifest offset %d does not match manager %d", meta.Offset, head.Offset)
		}
	}
}

func TestValueLogReconcileManifestRemovesInvalid(t *testing.T) {
	tmp := t.TempDir()
	mgr, err := vlogpkg.Open(vlogpkg.Config{Dir: tmp, FileMode: utils.DefaultFileMode, MaxSize: 1 << 20})
	require.NoError(t, err)
	defer mgr.Close()

	require.NoError(t, mgr.Rotate())

	vlog := &valueLog{
		dirPath: tmp,
		manager: mgr,
	}

	vlog.reconcileManifest(map[uint32]manifest.ValueLogMeta{
		0: {FileID: 0, Valid: false},
	})

	_, err = os.Stat(filepath.Join(tmp, "00000.vlog"))
	require.Error(t, err)
}

func TestValueLogGCSkipBlocked(t *testing.T) {
	clearDir()
	opt := NewDefaultOptions()
	opt.ValueLogFileSize = 1 << 20
	opt.NumCompactors = 0
	db := Open(opt)
	defer db.Close()

	e := kvpkg.NewEntry([]byte("gc-skip"), []byte("v"))
	require.NoError(t, db.Set(e.Key, e.Value))
	e.DecrRef()

	db.applyThrottle(true)
	defer db.applyThrottle(false)

	if err := db.RunValueLogGC(0.5); err != nil && !errors.Is(err, utils.ErrNoRewrite) {
		t.Fatalf("expected ErrNoRewrite when writes blocked, got %v", err)
	}
}
