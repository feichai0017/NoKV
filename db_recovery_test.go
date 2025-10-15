package NoKV

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
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

	for i := 0; i < 48; i++ {
		val := make([]byte, 512)
		key := []byte(fmt.Sprintf("key-%03d", i))
		require.NoError(t, db.Set(utils.NewEntry(key, val)))
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
	require.NoError(t, db.Set(utils.NewEntry(key, val)))
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
		require.NoError(t, db.Set(utils.NewEntry(key, val)))
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
	require.NoError(t, db.Set(utils.NewEntry([]byte("rewrite-key"), []byte("rewrite-val"))))
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
	require.NoError(t, db.Set(utils.NewEntry(key, val)))

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
	require.Equal(t, val, item.Value)
	logRecoveryMetric(t, "wal_replay", map[string]any{
		"key":           string(key),
		"value_base64":  item.Value,
		"wal_dir":       filepath.Join(opt.WorkDir, "wal"),
		"recovered_len": len(item.Value),
	})
}
