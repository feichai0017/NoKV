package NoKV

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/manifest"
	myraft "github.com/feichai0017/NoKV/raft"
	"github.com/feichai0017/NoKV/raftstore/engine"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
	raftpb "go.etcd.io/etcd/raft/v3/raftpb"
)

var recoveryTraceEnabled = os.Getenv("RECOVERY_TRACE_METRICS") != ""

func logRecoveryMetric(t *testing.T, name string, payload any) {
	if !recoveryTraceEnabled {
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
		WorkDir:          dir,
		ValueThreshold:   0,
		MemTableSize:     1 << 12,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 14,
		MaxBatchCount:    100,
		MaxBatchSize:     1 << 20,
	}

	db := Open(opt)

	for i := range 48 {
		val := make([]byte, 512)
		key := fmt.Appendf(nil, "key-%03d", i)
		e := utils.NewEntry(key, val)
		require.NoError(t, db.Set(e))
		e.DecrRef()
	}
	fids := db.vlog.manager.ListFIDs()
	require.GreaterOrEqual(t, len(fids), 2)
	staleFID := fids[0]

	require.NoError(t, db.lsm.LogValueLogDelete(staleFID))

	stalePath := filepath.Join(dir, "vlog", fmt.Sprintf("%05d.vlog", staleFID))
	if _, err := os.Stat(stalePath); err != nil {
		t.Fatalf("expected stale value log file %s to exist: %v", stalePath, err)
	}

	require.NoError(t, db.Close())

	db2 := Open(opt)
	defer db2.Close()

	_, err := os.Stat(stalePath)
	require.Error(t, err)
	removed := os.IsNotExist(err)
	require.True(t, removed, "expected stale value log file to be deleted on recovery")

	status := db2.lsm.ValueLogStatus()
	meta, ok := status[staleFID]
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
		WorkDir:          dir,
		ValueThreshold:   0,
		MemTableSize:     1 << 12,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 14,
		MaxBatchCount:    100,
		MaxBatchSize:     1 << 20,
	}

	db := Open(opt)
	key := []byte("orphan-key")
	val := make([]byte, 512)
	e := utils.NewEntry(key, val)
	require.NoError(t, db.Set(e))
	e.DecrRef()

	headPtr := db.vlog.manager.Head()
	require.False(t, headPtr.IsZero(), "expected value log head to be initialized")
	headCopy := headPtr
	require.NoError(t, db.lsm.LogValueLogHead(&headCopy))
	before := db.lsm.ValueLogStatus()
	beforeInfo := make(map[uint32]bool, len(before))
	for fid, meta := range before {
		beforeInfo[fid] = meta.Valid
	}
	require.NoError(t, db.Close())

	orphanFID := uint32(123)
	orphanPath := filepath.Join(dir, "vlog", fmt.Sprintf("%05d.vlog", orphanFID))
	require.NoError(t, os.WriteFile(orphanPath, []byte("orphan"), 0o666))

	db2 := Open(opt)
	defer db2.Close()

	headMeta, hasHead := db2.lsm.ValueLogHead()
	status := db2.lsm.ValueLogStatus()
	statusInfo := make(map[uint32]bool, len(status))
	for fid, meta := range status {
		statusInfo[fid] = meta.Valid
	}
	remainingFIDs := db2.vlog.manager.ListFIDs()

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
		e := utils.NewEntry(key, val)
		require.NoError(t, db.Set(e))
		e.DecrRef()
	}
	require.NoError(t, db.Close())

	files, err := filepath.Glob(filepath.Join(dir, "*.sst"))
	require.NoError(t, err)
	require.NotEmpty(t, files)

	removed := files[0]
	require.NoError(t, os.Remove(removed))

	db2 := Open(opt)
	defer db2.Close()

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
	e := utils.NewEntry([]byte("rewrite-key"), []byte("rewrite-val"))
	require.NoError(t, db.Set(e))
	e.DecrRef()
	require.NoError(t, db.Close())

	current := filepath.Join(dir, "CURRENT")
	data, err := os.ReadFile(current)
	require.NoError(t, err)
	manifestName := string(data)

	tmp := filepath.Join(dir, "CURRENT.tmp")
	require.NoError(t, os.WriteFile(tmp, []byte("MANIFEST-999999"), 0o666))

	db2 := Open(opt)
	defer db2.Close()

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
	defer walMgr.Close()

	manifestMgr, err := manifest.Open(manifestDir)
	require.NoError(t, err)
	defer manifestMgr.Close()

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
	defer walMgrRestore.Close()

	manifestMgrRestore, err := manifest.Open(restoreManifestDir)
	require.NoError(t, err)
	defer manifestMgrRestore.Close()

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
		WorkDir:          dir,
		ValueThreshold:   1 << 20,
		MemTableSize:     1 << 16,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 20,
		MaxBatchCount:    100,
		MaxBatchSize:     1 << 20,
	}

	db := Open(opt)
	key := []byte("wal-crash-key")
	val := []byte("wal-crash-value")
	e := utils.NewEntry(key, val)
	require.NoError(t, db.Set(e))
	e.DecrRef()

	// Simulate crash: close WAL/ValueLog handles without flushing LSM.
	_ = db.stats.close()
	_ = db.vlog.manager.Close()
	_ = db.wal.Close()
	if db.dirLock != nil {
		_ = db.dirLock.Release()
		db.dirLock = nil
	}

	db2 := Open(opt)
	defer db2.Close()

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
		WorkDir:          root,
		ValueThreshold:   1 << 20,
		MemTableSize:     1 << 12,
		SSTableMaxSz:     1 << 20,
		ValueLogFileSize: 1 << 20,
		MaxBatchCount:    32,
		MaxBatchSize:     1 << 20,
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
		"wal_segments_with_raft": snapBefore.WALSegmentsWithRaftRecords,
		"wal_removable_segments": snapBefore.WALRemovableRaftSegments,
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
	require.Greater(t, snapAfter.WALSegmentsWithRaftRecords, 0, "expected raft segments to be tracked")
	require.Greater(t, snapAfter.WALRemovableRaftSegments, 0, "expected removable raft backlog once followers catch up")
	logRecoveryMetric(t, "raft_wal_backlog_post", map[string]any{
		"wal_segments_with_raft": snapAfter.WALSegmentsWithRaftRecords,
		"wal_removable_segments": snapAfter.WALRemovableRaftSegments,
	})
}
