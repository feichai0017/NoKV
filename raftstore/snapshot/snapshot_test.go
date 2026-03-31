package snapshot_test

import (
	"bytes"
	"path/filepath"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func TestSnapshotExportSSTArtifact(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	valueBacked := bytes.Repeat([]byte("v"), 4096)
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFWrite, []byte("alpha"), 3, []byte("aw"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, valueBacked, 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := raftmeta.RegionMeta{
		ID:       7,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    raftmeta.RegionStateRunning,
	}
	artifactDir := filepath.Join(t.TempDir(), "region.sst.snapshot")
	result, err := snapshot.ExportSST(srcDB, artifactDir, region, testSnapshotLSMOptions(t), nil)
	require.NoError(t, err)
	require.Equal(t, uint64(3), result.Manifest.EntryCount)
	require.Equal(t, uint64(1), result.Manifest.TableCount)
	require.True(t, result.Manifest.InlineValues)

	manifest, err := snapshot.ReadSSTManifest(artifactDir, nil)
	require.NoError(t, err)
	require.Equal(t, result.Manifest.EntryCount, manifest.EntryCount)
	require.Len(t, manifest.Tables, 1)
	require.FileExists(t, filepath.Join(artifactDir, manifest.Tables[0].RelativePath))

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportSST(dstLSM, artifactDir, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), imported.ImportedTables)
	require.NotZero(t, imported.ImportedBytes)

	for _, entry := range entries {
		expected, err := srcDB.MaterializeInternalEntry(entry)
		require.NoError(t, err)
		got, err := dstLSM.Get(entry.Key)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, expected.Value, got.Value)
		got.DecrRef()
	}
}

func TestSnapshotExportSSTPayloadRoundTrip(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	valueBacked := bytes.Repeat([]byte("w"), 4096)
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, valueBacked, 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := raftmeta.RegionMeta{
		ID:       19,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    raftmeta.RegionStateRunning,
	}
	payload, manifest, err := snapshot.ExportSSTPayload(srcDB, t.TempDir(), region, testSnapshotLSMOptions(t), nil)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, uint64(2), manifest.EntryCount)

	readManifest, err := snapshot.ReadSSTPayloadManifest(payload)
	require.NoError(t, err)
	require.Equal(t, manifest.EntryCount, readManifest.EntryCount)
	require.Equal(t, manifest.Region.ID, readManifest.Region.ID)

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportSSTPayload(dstLSM, t.TempDir(), payload, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(1), imported.ImportedTables)
	for _, entry := range entries {
		expected, err := srcDB.MaterializeInternalEntry(entry)
		require.NoError(t, err)
		got, err := dstLSM.Get(entry.Key)
		require.NoError(t, err)
		require.NotNil(t, got)
		require.Equal(t, expected.Value, got.Value)
		got.DecrRef()
	}
}

func TestSnapshotImportSSTPayloadRollback(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("beta"), 2, bytes.Repeat([]byte("r"), 1024), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := raftmeta.RegionMeta{
		ID:       29,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    raftmeta.RegionStateRunning,
	}
	payload, _, err := snapshot.ExportSSTPayload(srcDB, t.TempDir(), region, testSnapshotLSMOptions(t), nil)
	require.NoError(t, err)

	dstLSM := openSnapshotLSM(t)
	defer func() { require.NoError(t, dstLSM.Close()) }()
	imported, err := snapshot.ImportSSTPayload(dstLSM, t.TempDir(), payload, nil)
	require.NoError(t, err)
	require.NotEmpty(t, imported.ImportedFileIDs)

	got, err := dstLSM.Get(entries[0].Key)
	require.NoError(t, err)
	require.NotNil(t, got)
	got.DecrRef()

	require.NoError(t, imported.Rollback(dstLSM))

	got, err = dstLSM.Get(entries[0].Key)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.Nil(t, got)
}

func openSnapshotDB(t testing.TB) *NoKV.DB {
	t.Helper()
	opt := NoKV.NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.SyncWrites = false
	opt.ValueThreshold = 32
	db, err := NoKV.Open(opt)
	require.NoError(t, err)
	return db
}

func testSnapshotLSMOptions(t testing.TB) *lsm.Options {
	t.Helper()
	return &lsm.Options{
		WorkDir:             t.TempDir(),
		MemTableSize:        1 << 20,
		SSTableMaxSz:        1 << 20,
		BlockSize:           4 << 10,
		BloomFalsePositive:  0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
	}
}

func openSnapshotLSM(t testing.TB) *lsm.LSM {
	t.Helper()
	opt := testSnapshotLSMOptions(t)
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	require.NoError(t, err)
	lsmInst, err := lsm.NewLSM(opt, wlog)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, wlog.Close()) })
	return lsmInst
}
