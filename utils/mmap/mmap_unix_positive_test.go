//go:build darwin || linux

package mmap

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Smoke-test mmap/munmap and sync helpers on supported platforms.
func TestMmapReadWriteCycle(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "mmap-positive-*")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	// Pre-size the file.
	require.NoError(t, tmpFile.Truncate(4096))

	data, err := Mmap(tmpFile, true, 4096)
	require.NoError(t, err)
	require.Len(t, data, 4096)

	copy(data[:11], []byte("hello mmap"))
	require.NoError(t, Msync(data))
	require.NoError(t, MsyncAsync(data))

	// Downgrade advice to random and then unmap.
	require.NoError(t, MadvisePattern(data, AdviceRandom))
	require.NoError(t, Munmap(data))
}
