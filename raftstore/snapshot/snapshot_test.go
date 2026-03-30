package snapshot_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/kv"
	raftmeta "github.com/feichai0017/NoKV/raftstore/meta"
	"github.com/feichai0017/NoKV/raftstore/snapshot"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestSnapshotExportImportRoundTrip(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	valueBacked := bytes.Repeat([]byte("v"), 4096)
	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("apple"), 10, []byte("a-default"), 0, 0),
		kv.NewInternalEntry(kv.CFWrite, []byte("apple"), 10, []byte("a-write"), 0, 0),
		kv.NewInternalEntry(kv.CFLock, []byte("banana"), 8, []byte("b-lock"), 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("banana"), 8, valueBacked, 0, 0),
		kv.NewInternalEntry(kv.CFDefault, []byte("carrot"), 6, []byte("c-default"), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := raftmeta.RegionMeta{
		ID:       1,
		StartKey: []byte("apple"),
		EndKey:   []byte("carrot"),
		Epoch: raftmeta.RegionEpoch{
			Version:     1,
			ConfVersion: 1,
		},
		State: raftmeta.RegionStateRunning,
	}
	artifactDir := filepath.Join(t.TempDir(), "region.snapshot")
	result, err := snapshot.Export(srcDB, artifactDir, region, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(4), result.Manifest.EntryCount)

	dstDB := openSnapshotDB(t)
	defer func() { _ = dstDB.Close() }()
	imported, err := snapshot.Import(dstDB, artifactDir, nil)
	require.NoError(t, err)
	require.Equal(t, result.Manifest.EntryCount, imported.Imported)

	got := collectRangeEntries(t, dstDB, region)
	want := collectRangeEntries(t, srcDB, region)
	require.Equal(t, want, got)

	manifest, err := snapshot.ReadManifest(artifactDir, nil)
	require.NoError(t, err)
	require.Equal(t, result.Manifest.EntryCount, manifest.EntryCount)
}

func TestSnapshotImportDetectsCorruptPayload(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("apple"), 1, []byte("value"), 0, 0)
	defer entry.DecrRef()
	require.NoError(t, srcDB.ApplyInternalEntries([]*kv.Entry{entry}))

	region := raftmeta.RegionMeta{ID: 1, State: raftmeta.RegionStateRunning}
	artifactDir := filepath.Join(t.TempDir(), "region.snapshot")
	_, err := snapshot.Export(srcDB, artifactDir, region, nil)
	require.NoError(t, err)

	entriesPath := filepath.Join(artifactDir, "entries.bin")
	data, err := os.ReadFile(entriesPath)
	require.NoError(t, err)
	require.NotEmpty(t, data)
	data[len(data)-1] ^= 0xff
	require.NoError(t, os.WriteFile(entriesPath, data, 0o600))

	dstDB := openSnapshotDB(t)
	defer func() { _ = dstDB.Close() }()
	_, err = snapshot.Import(dstDB, artifactDir, nil)
	require.Error(t, err)
}

func TestSnapshotPayloadRoundTrip(t *testing.T) {
	srcDB := openSnapshotDB(t)
	defer func() { _ = srcDB.Close() }()

	entries := []*kv.Entry{
		kv.NewInternalEntry(kv.CFDefault, []byte("alpha"), 3, []byte("a"), 0, 0),
		kv.NewInternalEntry(kv.CFWrite, []byte("beta"), 2, []byte("b"), 0, 0),
	}
	for _, entry := range entries {
		defer entry.DecrRef()
	}
	require.NoError(t, srcDB.ApplyInternalEntries(entries))

	region := raftmeta.RegionMeta{
		ID:       9,
		StartKey: []byte("alpha"),
		EndKey:   []byte("z"),
		State:    raftmeta.RegionStateRunning,
	}
	payload, manifest, err := snapshot.ExportPayload(srcDB, region)
	require.NoError(t, err)
	require.NotEmpty(t, payload)
	require.Equal(t, uint64(2), manifest.EntryCount)

	dstDB := openSnapshotDB(t)
	defer func() { _ = dstDB.Close() }()
	result, err := snapshot.ImportPayload(dstDB, payload)
	require.NoError(t, err)
	require.Equal(t, manifest.EntryCount, result.Imported)
	require.Equal(t, collectRangeEntries(t, srcDB, region), collectRangeEntries(t, dstDB, region))
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

func collectRangeEntries(t testing.TB, db *NoKV.DB, region raftmeta.RegionMeta) []string {
	t.Helper()
	iter := db.NewInternalIterator(&utils.Options{
		IsAsc:      true,
		LowerBound: region.StartKey,
		UpperBound: region.EndKey,
	})
	require.NotNil(t, iter)
	defer func() { _ = iter.Close() }()

	var out []string
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item()
		require.NotNil(t, item)
		entry := item.Entry()
		require.NotNil(t, entry)
		_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		if len(region.StartKey) > 0 && bytes.Compare(userKey, region.StartKey) < 0 {
			continue
		}
		if len(region.EndKey) > 0 && bytes.Compare(userKey, region.EndKey) >= 0 {
			continue
		}
		materialized, err := db.MaterializeInternalEntry(entry)
		require.NoError(t, err)
		cf, userKey, ts, ok := kv.SplitInternalKey(materialized.Key)
		require.True(t, ok)
		out = append(out, fmt.Sprintf("%s|%s|%d|%x|%d", cf.String(), string(userKey), ts, materialized.Value, materialized.Meta))
	}
	return out
}
