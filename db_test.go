package NoKV

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/feichai0017/hotring"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/raft/v3/raftpb"
)

func TestAPI(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()
	// Write entries.
	for i := range 50 {
		key, val := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		e := kv.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.setEntry(e); err != nil {
			t.Fatal(err)
		}
		e.DecrRef()
		// Read back.
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
			entry.DecrRef()
		}
	}

	for i := range 40 {
		key, _ := fmt.Sprintf("key%d", i), fmt.Sprintf("val%d", i)
		if err := db.Del([]byte(key)); err != nil {
			t.Fatal(err)
		}
	}

	// Iterator scan.
	iter := db.NewIterator(&utils.Options{
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
		e := kv.NewEntry([]byte(key), []byte(val)).WithTTL(1000 * time.Second)
		if err := db.setEntry(e); err != nil {
			t.Fatal(err)
		}
		e.DecrRef()
		// Read back.
		if entry, err := db.Get([]byte(key)); err != nil {
			t.Fatal(err)
		} else {
			t.Logf("db.Get key=%s, value=%s, expiresAt=%d", entry.Key, entry.Value, entry.ExpiresAt)
			entry.DecrRef()
		}
	}
}

func TestColumnFamilies(t *testing.T) {
	clearDir()
	db := Open(opt)
	defer func() { _ = db.Close() }()

	key := []byte("user-key")
	require.NoError(t, db.SetCF(kv.CFDefault, key, []byte("default")))
	require.NoError(t, db.SetCF(kv.CFLock, key, []byte("lock")))
	require.NoError(t, db.SetCF(kv.CFWrite, key, []byte("write")))

	e, err := db.GetCF(kv.CFDefault, key)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, e.CF)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()

	e, err = db.GetCF(kv.CFLock, key)
	require.NoError(t, err)
	require.Equal(t, kv.CFLock, e.CF)
	require.Equal(t, []byte("lock"), e.Value)
	e.DecrRef()

	e, err = db.GetCF(kv.CFWrite, key)
	require.NoError(t, err)
	require.Equal(t, kv.CFWrite, e.CF)
	require.Equal(t, []byte("write"), e.Value)
	e.DecrRef()

	// Default Get should read default CF.
	e, err = db.Get(key)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, e.CF)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()

	require.NoError(t, db.DelCF(kv.CFLock, key))
	_, err = db.GetCF(kv.CFLock, key)
	require.Error(t, err)
	// Default CF should remain untouched.
	e, err = db.GetCF(kv.CFDefault, key)
	require.NoError(t, err)
	require.Equal(t, []byte("default"), e.Value)
	e.DecrRef()
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

func TestVersionedEntryRoundTrip(t *testing.T) {
	opt := newTestOptions(t)
	db := Open(opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-key")
	version := uint64(42)
	value := []byte("value-42")

	require.NoError(t, db.SetVersionedEntry(kv.CFDefault, key, version, value, 0))

	entry, err := db.GetVersionedEntry(kv.CFDefault, key, version)
	require.NoError(t, err)
	require.Equal(t, kv.CFDefault, entry.CF)
	require.Equal(t, key, entry.Key)
	require.Equal(t, version, entry.Version)
	require.Equal(t, value, entry.Value)
	entry.DecrRef()
}

func TestVersionedEntryDeleteTombstone(t *testing.T) {
	opt := newTestOptions(t)
	db := Open(opt)
	defer func() { _ = db.Close() }()

	key := []byte("versioned-delete")
	require.NoError(t, db.SetVersionedEntry(kv.CFDefault, key, 1, []byte("v1"), 0))
	require.NoError(t, db.DeleteVersionedEntry(kv.CFDefault, key, 2))

	entry, err := db.GetVersionedEntry(kv.CFDefault, key, 2)
	require.NoError(t, err)
	require.Equal(t, key, entry.Key)
	require.Equal(t, uint64(2), entry.Version)
	require.True(t, entry.Meta&kv.BitDelete > 0)
	entry.DecrRef()

	entry, err = db.GetVersionedEntry(kv.CFDefault, key, 1)
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), entry.Value)
	require.Equal(t, uint64(1), entry.Version)
	entry.DecrRef()
}

func TestGetEntryIsDetachedFromPool(t *testing.T) {
	opt := newTestOptions(t)
	db := Open(opt)
	defer func() { _ = db.Close() }()

	key := []byte("detached-key")
	require.NoError(t, db.Set(key, []byte("value-1")))

	entry, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), entry.Value)

	// Public read APIs now return detached entries; DecrRef should be a no-op.
	entry.DecrRef()
	require.Equal(t, []byte("value-1"), entry.Value)

	entry.Value[0] = 'X'
	again, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, []byte("value-1"), again.Value)
}

func TestDBIteratorSeekAndValueCopy(t *testing.T) {
	t.Run("inline", func(t *testing.T) {
		opt := newTestOptions(t)
		db := Open(opt)
		defer func() { _ = db.Close() }()

		require.NoError(t, db.Set([]byte("a"), []byte("va")))
		require.NoError(t, db.Set([]byte("b"), []byte("vb")))
		require.NoError(t, db.Set([]byte("c"), []byte("vc")))

		it := db.NewIterator(&utils.Options{IsAsc: true})
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
		db := Open(opt)
		defer func() { _ = db.Close() }()

		value := bytes.Repeat([]byte("p"), 64)
		require.NoError(t, db.Set([]byte("k"), value))

		it := db.NewIterator(&utils.Options{IsAsc: true, OnlyUseKey: true})
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

func TestCFHotKeyEncoding(t *testing.T) {
	key := []byte("hot-key")
	require.Equal(t, string(key), cfHotKey(kv.CFDefault, key))
	require.Equal(t, string(key), cfHotKey(kv.ColumnFamily(0), key))

	encoded := cfHotKey(kv.CFLock, key)
	require.Len(t, encoded, len(key)+1)
	require.Equal(t, byte(kv.CFLock), encoded[0])
	require.Equal(t, string(key), encoded[1:])
}

func TestRequestLoadEntriesCopiesSlice(t *testing.T) {
	req := requestPool.Get().(*request)
	req.reset()
	defer func() {
		req.Entries = nil
		req.Ptrs = nil
		requestPool.Put(req)
	}()

	e1 := &kv.Entry{Key: []byte("a")}
	e2 := &kv.Entry{Key: []byte("b")}
	src := []*kv.Entry{e1, e2}
	req.loadEntries(src)

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

	db := Open(opt)

	didPanic := false
	func() {
		defer func() {
			if r := recover(); r != nil {
				didPanic = true
			}
		}()
		Open(opt)
	}()

	require.True(t, didPanic, "expected second Open to panic due to directory lock")

	require.NoError(t, db.Close())

	db2 := Open(opt)
	require.NoError(t, db2.Close())
}

func TestWriteHotKeyThrottleBlocksDB(t *testing.T) {
	clearDir()
	prev := opt.WriteHotKeyLimit
	opt.WriteHotKeyLimit = 3
	defer func() {
		opt.WriteHotKeyLimit = prev
	}()

	db := Open(opt)
	defer func() { _ = db.Close() }()

	key := []byte("throttle-key")
	require.NoError(t, db.SetCF(kv.CFDefault, key, []byte("v1")))
	require.NoError(t, db.SetCF(kv.CFDefault, key, []byte("v2")))
	err := db.SetCF(kv.CFDefault, key, []byte("v3"))
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.Equal(t, uint64(1), atomic.LoadUint64(&db.hotWriteLimited))
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

	db := Open(opt)

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

	db2 := Open(opt)
	defer func() { _ = db2.Close() }()

	_, err := os.Stat(stalePath)
	require.Error(t, err)
	removed := os.IsNotExist(err)
	require.True(t, removed, "expected stale value log file to be deleted on recovery")

	status := db2.lsm.ValueLogStatus()
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

	db := Open(opt)
	key := []byte("orphan-key")
	val := make([]byte, 512)
	require.NoError(t, db.Set(key, val))

	headPtr := db.vlog.managers[0].Head()
	require.False(t, headPtr.IsZero(), "expected value log head to be initialized")
	headCopy := headPtr
	require.NoError(t, db.lsm.LogValueLogHead(&headCopy))
	before := db.lsm.ValueLogStatus()
	beforeInfo := make(map[manifest.ValueLogID]bool, len(before))
	for id, meta := range before {
		beforeInfo[id] = meta.Valid
	}
	require.NoError(t, db.Close())

	orphanFID := uint32(123)
	orphanPath := filepath.Join(dir, "vlog", "bucket-000", fmt.Sprintf("%05d.vlog", orphanFID))
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), 0o666))

	db2 := Open(opt)
	defer func() { _ = db2.Close() }()

	headMeta, hasHead := db2.lsm.ValueLogHead()[0]
	status := db2.lsm.ValueLogStatus()
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

func TestRecoveryCleansMissingSSTFromManifest(t *testing.T) {
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

	db := Open(opt)
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

	db2 := Open(opt)
	defer func() { _ = db2.Close() }()

	version := db2.lsm.CurrentVersion()
	levelFiles := version.Levels[0]
	fileIDs := make([]uint64, 0, len(levelFiles))
	for _, meta := range levelFiles {
		fileIDs = append(fileIDs, meta.FileID)
		path := utils.FileNameSSTable(opt.WorkDir, meta.FileID)
		require.NotEqual(t, removed, path)
		_, err := os.Stat(path)
		require.NoError(t, err)
	}
	logRecoveryMetric(t, "sst_manifest_cleanup", map[string]any{
		"removed_path":      removed,
		"level0_file_count": len(levelFiles),
		"level0_file_ids":   fileIDs,
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

	db := Open(opt)
	require.NoError(t, db.Set([]byte("rewrite-key"), []byte("rewrite-val")))
	require.NoError(t, db.Close())

	current := filepath.Join(dir, "CURRENT")
	data, err := os.ReadFile(current)
	require.NoError(t, err)
	manifestName := string(data)

	tmp := filepath.Join(dir, "CURRENT.tmp")
	require.NoError(t, os.WriteFile(tmp, []byte("MANIFEST-999999"), 0o666))

	db2 := Open(opt)
	defer func() { _ = db2.Close() }()

	name, err := os.ReadFile(current)
	require.NoError(t, err)
	require.Equal(t, manifestName, string(name))

	tmpExists := false
	item, err := db2.Get([]byte("rewrite-key"))
	require.NoError(t, err)
	defer item.DecrRef()
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

	manifestMgr, err := manifest.Open(manifestDir)
	require.NoError(t, err)
	defer func() { _ = manifestMgr.Close() }()

	ws, err := engine.OpenWALStorage(engine.WALStorageConfig{
		GroupID:  1,
		WAL:      walMgr,
		Manifest: manifestMgr,
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
	require.NoError(t, engine.ExportSnapshot(ws, exportPath))
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

	manifestMgrRestore, err := manifest.Open(restoreManifestDir)
	require.NoError(t, err)
	defer func() { _ = manifestMgrRestore.Close() }()

	wsRestore, err := engine.OpenWALStorage(engine.WALStorageConfig{
		GroupID:  1,
		WAL:      walMgrRestore,
		Manifest: manifestMgrRestore,
	})
	require.NoError(t, err)

	require.NoError(t, engine.ImportSnapshot(wsRestore, exportPath))

	ptr, ok := manifestMgrRestore.RaftPointer(1)
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

	db := Open(opt)
	key := []byte("wal-crash-key")
	val := []byte("wal-crash-value")
	require.NoError(t, db.Set(key, val))

	// Simulate crash: close WAL/ValueLog handles without flushing LSM.
	_ = db.stats.close()
	for _, mgr := range db.vlog.managers {
		if mgr != nil {
			_ = mgr.Close()
		}
	}
	_ = db.wal.Close()
	if db.dirLock != nil {
		_ = db.dirLock.Release()
		db.dirLock = nil
	}

	db2 := Open(opt)
	defer func() { _ = db2.Close() }()

	item, err := db2.Get(key)
	require.NoError(t, err)
	defer item.DecrRef()
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

	db := Open(opt)
	defer func() { _ = db.Close() }()

	walMgr := db.WAL()
	manifestMgr := db.Manifest()

	appendRaft := func(data string) {
		_, err := walMgr.AppendRecords(wal.Record{Type: wal.RecordTypeRaftEntry, Payload: []byte(data)})
		require.NoError(t, err)
		require.NoError(t, walMgr.Sync())
	}

	appendRaft("group1-seg1")
	require.NoError(t, manifestMgr.LogRaftPointer(manifest.RaftLogPointer{GroupID: 1, Segment: walMgr.ActiveSegment(), AppliedIndex: 10, AppliedTerm: 1}))
	require.NoError(t, manifestMgr.LogRaftPointer(manifest.RaftLogPointer{GroupID: 2, Segment: walMgr.ActiveSegment(), AppliedIndex: 9, AppliedTerm: 1}))

	snapBefore := db.Info().Snapshot()
	logRecoveryMetric(t, "raft_wal_backlog_pre", map[string]any{
		"wal_segments_with_raft": snapBefore.WAL.SegmentsWithRaftRecords,
		"wal_removable_segments": snapBefore.WAL.RemovableRaftSegments,
	})

	require.NoError(t, walMgr.SwitchSegment(2, true))
	appendRaft("group1-seg2")
	require.NoError(t, walMgr.SwitchSegment(3, true))
	appendRaft("group1-seg3")

	require.NoError(t, manifestMgr.LogRaftPointer(manifest.RaftLogPointer{GroupID: 1, Segment: 3, AppliedIndex: 30, AppliedTerm: 4}))
	require.NoError(t, manifestMgr.LogRaftPointer(manifest.RaftLogPointer{GroupID: 2, Segment: 3, AppliedIndex: 28, AppliedTerm: 4}))
	require.NoError(t, manifestMgr.LogRaftTruncate(1, 30, 4, 3, 0))
	require.NoError(t, manifestMgr.LogRaftTruncate(2, 28, 4, 3, 0))

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

	db := Open(opt)

	userKey := []byte("vlog-replay-key")
	internalKey := kv.InternalKey(kv.CFDefault, userKey, math.MaxUint64)
	entry := kv.NewEntry(internalKey, []byte("payload"))
	_, err := db.vlog.managers[0].AppendEntry(entry)
	require.NoError(t, err)
	entry.DecrRef()
	require.NoError(t, db.vlog.managers[0].SyncActive())
	require.NoError(t, db.Close())

	db2 := Open(opt)
	defer func() { _ = db2.Close() }()

	_, err = db2.Get(userKey)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
}

func TestWriteHotKeyThrottleBlocksTxn(t *testing.T) {
	clearDir()
	prev := opt.WriteHotKeyLimit
	opt.WriteHotKeyLimit = 3
	defer func() {
		opt.WriteHotKeyLimit = prev
	}()

	db := Open(opt)
	defer func() { _ = db.Close() }()

	txn := db.NewTransaction(true)
	key := []byte("txn-hot-key")
	require.NoError(t, txn.Set(key, []byte("a")))
	require.NoError(t, txn.Set(key, []byte("b")))
	err := txn.Set(key, []byte("c"))
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	txn.Discard()
	require.Equal(t, uint64(1), atomic.LoadUint64(&db.hotWriteLimited))
}

func TestCFHotKey(t *testing.T) {
	key := []byte("k")
	require.Equal(t, "k", cfHotKey(kv.CFDefault, key))
	require.Equal(t, string(key), cfHotKey(kv.ColumnFamily(0), key))

	expected := append([]byte{byte(kv.CFWrite)}, key...)
	require.Equal(t, string(expected), cfHotKey(kv.CFWrite, key))
}

func TestHotWriteAndThrottle(t *testing.T) {
	db := &DB{
		opt: &Options{
			HotWriteBurstThreshold: 1,
			WriteHotKeyLimit:       1,
		},
		hotWrite: hotring.NewHotRing(8, nil),
	}

	entry := kv.NewEntry([]byte("hot"), []byte("v"))
	err := db.maybeThrottleWrite(kv.CFDefault, entry.Key)
	require.ErrorIs(t, err, utils.ErrHotKeyWriteThrottle)
	require.True(t, db.isHotWrite([]*kv.Entry{entry}))
	require.Equal(t, uint64(1), atomic.LoadUint64(&db.hotWriteLimited))
}

func TestIsHotWriteUsesUserKeyForInternalEntries(t *testing.T) {
	db := &DB{
		opt: &Options{
			HotWriteBurstThreshold: 1,
			WriteHotKeyLimit:       0,
		},
		hotWrite: hotring.NewHotRing(8, nil),
	}

	userKey := []byte("hot-user-key")
	require.NoError(t, db.maybeThrottleWrite(kv.CFDefault, userKey))

	entry := kv.NewEntry(kv.InternalKey(kv.CFDefault, userKey, nonTxnMaxVersion), []byte("v"))
	defer entry.DecrRef()
	require.True(t, db.isHotWrite([]*kv.Entry{entry}))
}

func TestApplyRequestsFailureIndex(t *testing.T) {
	local := NewDefaultOptions()
	local.WorkDir = t.TempDir()
	local.EnableWALWatchdog = false
	local.ValueLogGCInterval = 0
	local.WriteBatchWait = 0

	db := Open(local)
	defer func() { _ = db.Close() }()

	good := kv.NewEntryWithCF(kv.CFDefault, kv.InternalKey(kv.CFDefault, []byte("good"), nonTxnMaxVersion), []byte("v1"))
	bad := kv.NewEntryWithCF(kv.CFDefault, []byte{}, []byte("v2"))
	defer good.DecrRef()
	defer bad.DecrRef()

	reqs := []*request{
		{
			Entries: []*kv.Entry{good},
			Ptrs:    []kv.ValuePtr{{}},
		},
		{
			Entries: []*kv.Entry{bad},
			Ptrs:    []kv.ValuePtr{{}},
		},
	}

	failedAt, err := db.applyRequests(reqs)
	require.Equal(t, 1, failedAt)
	require.Error(t, err)

	got, getErr := db.lsm.Get(good.Key)
	require.NoError(t, getErr)
	require.Equal(t, []byte("v1"), got.Value)
	got.DecrRef()
}

func TestFinishCommitRequestsPerRequestErrors(t *testing.T) {
	db := &DB{}
	req1 := &request{}
	req2 := &request{}
	req1.wg.Add(1)
	req2.wg.Add(1)
	reqErr := errors.New("request failed")

	batch := []*commitRequest{
		{req: req1},
		{req: req2},
	}
	perReq := map[*request]error{
		req2: reqErr,
	}

	db.finishCommitRequests(batch, nil, perReq)
	req1.wg.Wait()
	req2.wg.Wait()

	require.NoError(t, req1.Err)
	require.ErrorIs(t, req2.Err, reqErr)
}

func TestRecordReadEnqueuesPrefetch(t *testing.T) {
	db := &DB{
		opt:          &Options{},
		hotRead:      hotring.NewHotRing(8, nil),
		prefetchRing: utils.NewRing[prefetchRequest](2),
		prefetchHot:  1,
	}
	db.prefetchState.Store(&prefetchState{
		pend:       make(map[string]struct{}),
		prefetched: make(map[string]time.Time),
	})

	db.recordRead([]byte("k1"))
	req, ok := db.prefetchRing.Pop()
	require.True(t, ok)
	require.Equal(t, "k1", req.key)
}

func TestEnqueuePrefetchRingFull(t *testing.T) {
	db := &DB{
		opt:          &Options{},
		prefetchRing: utils.NewRing[prefetchRequest](2),
	}
	db.prefetchState.Store(&prefetchState{
		pend:       make(map[string]struct{}),
		prefetched: make(map[string]time.Time),
	})

	require.True(t, db.prefetchRing.Push(prefetchRequest{key: "filled-1"}))
	require.True(t, db.prefetchRing.Push(prefetchRequest{key: "filled-2"}))

	db.enqueuePrefetch("blocked")
	state := db.prefetchState.Load()
	_, pending := state.pend["blocked"]
	require.False(t, pending)
}

func TestExecutePrefetchUpdatesState(t *testing.T) {
	db := &DB{
		opt:              &Options{},
		prefetchCooldown: 10 * time.Millisecond,
	}
	state := &prefetchState{
		pend:       map[string]struct{}{"k1": {}},
		prefetched: make(map[string]time.Time),
	}
	db.prefetchState.Store(state)

	db.executePrefetch(prefetchRequest{key: "k1"})
	next := db.prefetchState.Load()
	_, pending := next.pend["k1"]
	require.False(t, pending)
	expiry, ok := next.prefetched["k1"]
	require.True(t, ok)
	require.True(t, expiry.After(time.Now().Add(-time.Millisecond)))

	db.prefetchCooldown = 0
	next.pend["k2"] = struct{}{}
	db.prefetchState.Store(next)
	db.executePrefetch(prefetchRequest{key: "k2"})
	final := db.prefetchState.Load()
	_, pending = final.pend["k2"]
	require.False(t, pending)
	_, ok = final.prefetched["k2"]
	require.False(t, ok)
}

func TestPrefetchLoopDrainsRing(t *testing.T) {
	db := &DB{
		opt:           &Options{},
		prefetchRing:  utils.NewRing[prefetchRequest](2),
		prefetchItems: make(chan struct{}, 2),
	}
	db.prefetchState.Store(&prefetchState{
		pend:       make(map[string]struct{}),
		prefetched: make(map[string]time.Time),
	})

	db.prefetchWG.Add(1)
	go db.prefetchLoop()

	db.enqueuePrefetch("k")
	require.Eventually(t, func() bool {
		state := db.prefetchState.Load()
		_, pending := state.pend["k"]
		return !pending
	}, time.Second, 5*time.Millisecond)
	db.prefetchRing.Close()
	select {
	case db.prefetchItems <- struct{}{}:
	default:
	}
	db.prefetchWG.Wait()

	state := db.prefetchState.Load()
	_, pending := state.pend["k"]
	require.False(t, pending)
}

func TestCloseWithErrors(t *testing.T) {
	clearDir()
	db := Open(opt)
	lsmErr := errors.New("lsm close error")
	vlogErr := errors.New("vlog close error")
	walErr := errors.New("wal close error")
	dirLockErr := errors.New("dir lock release error")
	db.testCloseHooks = &testCloseHooks{
		lsmClose: func() error {
			_ = db.lsm.Close()
			return lsmErr
		},
		vlogClose: func() error {
			_ = db.vlog.close()
			return vlogErr
		},
		walClose: func() error {
			_ = db.wal.Close()
			return walErr
		},
		dirLockRelease: func() error {
			if db.dirLock != nil {
				_ = db.dirLock.Release()
			}
			return dirLockErr
		},
		calls: []string{},
	}
	err := db.Close()

	var gotErrs []error
	if err != nil {
		if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
			gotErrs = multiErr.Unwrap()
		}
	}
	require.Len(t, gotErrs, 4)
	require.True(t, errors.Is(gotErrs[0], lsmErr))
	require.True(t, errors.Is(gotErrs[1], vlogErr))
	require.True(t, errors.Is(gotErrs[2], walErr))
	require.True(t, errors.Is(gotErrs[3], dirLockErr))
	expectCalls := []string{"lsm close", "vlog close", "wal close", "dir lock release"}
	require.Equal(t, expectCalls, db.testCloseHooks.calls)
}
