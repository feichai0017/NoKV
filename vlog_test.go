package NoKV

import (
	"bytes"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	kvpkg "github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	vlogpkg "github.com/feichai0017/NoKV/vlog"
	"github.com/feichai0017/NoKV/wal"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

var (
	// 初始化opt
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
	// 清理目录
	clearDir()
	// 打开DB
	db := Open(opt)
	defer db.Close()
	log := db.vlog
	var err error
	// 创建一个简单的kv entry对象
	const val1 = "sampleval012345678901234567890123"
	const val2 = "samplevalb012345678901234567890123"
	require.True(t, int64(len(val1)) >= db.opt.ValueThreshold)

	e1 := kvpkg.NewEntry([]byte("samplekey"), []byte(val1))
	e1.Meta = kvpkg.BitValuePointer

	e2 := kvpkg.NewEntry([]byte("samplekeyb"), []byte(val2))
	e2.Meta = kvpkg.BitValuePointer

	// 构建一个批量请求的request
	b := new(request)
	b.Entries = []*kvpkg.Entry{e1, e2}

	// 直接写入vlog中
	log.write([]*request{b})
	e1.DecrRef()
	e2.DecrRef()
	require.Len(t, b.Ptrs, 2)
	t.Logf("Pointer written: %+v %+v\n", b.Ptrs[0], b.Ptrs[1])

	// 从vlog中使用 value ptr指针中查询写入的分段vlog文件
	payload1, unlock1, err1 := log.manager.Read(&b.Ptrs[0])
	payload2, unlock2, err2 := log.manager.Read(&b.Ptrs[1])
	require.NoError(t, err1)
	require.NoError(t, err2)
	// 关闭会调的锁
	defer kvpkg.RunCallback(unlock1)
	defer kvpkg.RunCallback(unlock2)
	entry1, err := wal.DecodeEntry(payload1)
	require.NoError(t, err)
	defer entry1.DecrRef()
	entry2, err := wal.DecodeEntry(payload2)
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
	db := Open(opt)
	defer db.Close()
	sz := 32 << 10
	kvList := make([]*kvpkg.Entry, 0, 100)
	defer func() {
		for _, e := range kvList {
			e.DecrRef()
		}
	}()

	for i := 0; i < 100; i++ {
		e := newRandEntry(sz)
		eCopy := kvpkg.NewEntry(e.Key, e.Value)
		eCopy.Meta = e.Meta
		eCopy.ExpiresAt = e.ExpiresAt
		kvList = append(kvList, eCopy)

		require.NoError(t, db.Set(e))
		e.DecrRef()
	}
	db.RunValueLogGC(0.9)
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
	lf, ok := vlog.manager.LogFile(active)
	require.True(t, ok, "active log file missing")

	var captured []*kvpkg.Entry
	_, err := vlog.iterate(lf, kvpkg.ValueLogHeaderSize, func(e *kvpkg.Entry, vp *kvpkg.ValuePtr) error {
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
	payload := wal.EncodeEntry(buf, orig)
	orig.DecrRef()

	entry, _, _, err := decodeWalEntry(payload)
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
	db.vlog.opt.ValueLogFileSize = 256
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

func newRandEntry(sz int) *kvpkg.Entry {
	v := make([]byte, sz)
	rand.Read(v[:rand.Intn(sz)])
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
