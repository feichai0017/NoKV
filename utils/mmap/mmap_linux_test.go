//go:build linux
// +build linux

package mmap

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMmap_Basic(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-util-mmap-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	filePath := filepath.Join(dir, "test.mmap")

	// Create a file.
	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, fd.Close())
	})

	// Truncate it to a specific size.
	require.NoError(t, fd.Truncate(1024))

	// Mmap the file.
	data, err := Mmap(fd, true, 1024)
	require.NoError(t, err)
	require.Equal(t, 1024, len(data))

	// Write to the mmaped region.
	copy(data[100:110], []byte("test data"))

	// Sync and unmap.
	require.NoError(t, Msync(data))
	require.NoError(t, Munmap(data))

	// Re-read the file to verify content.
	fileContent, err := os.ReadFile(filePath)
	require.NoError(t, err)
	require.Equal(t, "test data", string(fileContent[100:109]))
}

func TestMremap(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-util-mremap-test")
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, os.RemoveAll(dir))
	})

	filePath := filepath.Join(dir, "test.mmap")

	fd, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0666)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, fd.Close())
	})

	require.NoError(t, fd.Truncate(128))

	// Mmap the file.
	data, err := Mmap(fd, true, 128)
	require.NoError(t, err)
	require.Equal(t, 128, len(data))

	// Remap to a larger size.
	newData, err := Mremap(data, 256)
	require.NoError(t, err)
	require.Equal(t, 256, len(newData))

	// Unmap the new region.
	require.NoError(t, Munmap(newData))
}
