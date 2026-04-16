package vfs

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFileHelpers(t *testing.T) {
	require.Equal(t, uint64(12), FID("00012.sst"))
	require.Equal(t, uint64(0), FID("bad.txt"))

	dir := t.TempDir()
	require.Equal(t, filepath.Join(dir, "00042.sst"), FileNameSSTable(dir, 42))
	require.Equal(t, filepath.Join(dir, "00007.vlog"), VlogFilePath(dir, 7))

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
