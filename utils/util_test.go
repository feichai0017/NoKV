package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/stretchr/testify/require"
)

func TestCompareUserKeysAndChecksum(t *testing.T) {
	k1 := kv.KeyWithTs([]byte("a"), 1)
	k2 := kv.KeyWithTs([]byte("b"), 1)
	require.Less(t, CompareUserKeys(k1, k2), 0)
	require.Equal(t, 0, CompareUserKeys([]byte("c"), []byte("c")))

	data := []byte("checksum")
	sum := CalculateChecksum(data)
	require.NoError(t, VerifyChecksum(data, kv.U64ToBytes(sum)))
	require.Error(t, VerifyChecksum(data, []byte{0x00}))
}

func TestRemoveDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "file.txt")
	require.NoError(t, os.WriteFile(path, []byte("data"), 0o644))
	require.NotPanics(t, func() {
		RemoveDir(nil, dir)
	})
	_, err := os.Stat(dir)
	require.Error(t, err)
}

func TestFileHelpers(t *testing.T) {
	require.Equal(t, uint64(12), FID("00012.sst"))
	require.Equal(t, uint64(0), FID("bad.txt"))

	dir := t.TempDir()
	require.Equal(t, filepath.Join(dir, "00042.sst"), FileNameSSTable(dir, 42))
	require.Equal(t, filepath.Join(dir, "00007.vlog"), VlogFilePath(dir, 7))

	filePath := filepath.Join(dir, "fresh.txt")
	f, err := CreateSyncedFile(nil, filePath, false)
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = CreateSyncedFile(nil, filePath, false)
	require.Error(t, err)

	require.NoError(t, SyncDir(nil, dir))
}

func TestLoadIDMap(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00001.sst"), []byte("a"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "00002.sst"), []byte("b"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "ignore.txt"), []byte("c"), 0o644))

	ids := LoadIDMap(nil, dir)
	require.Contains(t, ids, uint64(1))
	require.Contains(t, ids, uint64(2))
	require.NotContains(t, ids, uint64(3))
}
