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
		RemoveDir(dir)
	})
	_, err := os.Stat(dir)
	require.Error(t, err)
}
