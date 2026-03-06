package utils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/vfs"
	"github.com/stretchr/testify/require"
)

func TestCompareUserKeysAndChecksum(t *testing.T) {
	k1 := kv.InternalKey(kv.CFDefault, []byte("a"), 1)
	k2 := kv.InternalKey(kv.CFDefault, []byte("b"), 1)
	require.Less(t, CompareUserKeys(k1, k2), 0)
	require.Equal(t, 0, CompareUserKeys(
		kv.InternalKey(kv.CFDefault, []byte("c"), 10),
		kv.InternalKey(kv.CFDefault, []byte("c"), 1),
	))

	data := []byte("checksum")
	sum := CalculateChecksum(data)
	require.NoError(t, VerifyChecksum(data, kv.U64ToBytes(sum)))
	require.Error(t, VerifyChecksum(data, []byte{0x00}))
}

func TestFileHelpers(t *testing.T) {
	require.Equal(t, uint64(12), FID("00012.sst"))
	require.Equal(t, uint64(0), FID("bad.txt"))

	dir := t.TempDir()
	require.Equal(t, filepath.Join(dir, "00042.sst"), FileNameSSTable(dir, 42))
	require.Equal(t, filepath.Join(dir, "00007.vlog"), VlogFilePath(dir, 7))

	require.NoError(t, vfs.SyncDir(nil, dir))
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
