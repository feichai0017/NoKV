//go:build darwin || linux

package mmap

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestMsyncAsyncRangeRejectsInvalid(t *testing.T) {
	err := MsyncAsyncRange([]byte{}, 0, 0)
	require.ErrorIs(t, err, unix.EINVAL)
}

// Smoke-test mmap/munmap and sync helpers on supported platforms.
func TestMmapReadWriteCycle(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "mmap-positive-*")
	require.NoError(t, err)
	defer func() { _ = os.Remove(tmpFile.Name()) }()
	defer func() { _ = tmpFile.Close() }()

	// Pre-size the file.
	require.NoError(t, tmpFile.Truncate(4096))

	data, err := Mmap(tmpFile.Fd(), true, 4096)
	require.NoError(t, err)
	require.Len(t, data, 4096)

	copy(data[:11], []byte("hello mmap"))
	require.NoError(t, Msync(data))
	require.NoError(t, MsyncAsync(data))

	// Downgrade advice to random and then unmap.
	require.NoError(t, MadvisePattern(data, AdviceRandom))
	require.NoError(t, Munmap(data))
}
