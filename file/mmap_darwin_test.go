//go:build darwin
// +build darwin

package file

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestMmapFileBasics(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap.dat")

	mf, err := OpenMmapFile(path, os.O_CREATE|os.O_RDWR, 64)
	require.NoError(t, err)
	require.NotNil(t, mf)
	defer func() { _ = mf.Close() }()

	payload := []byte("payload")
	allocated, next, err := mf.AllocateSlice(len(payload), 0)
	require.NoError(t, err)
	require.Equal(t, 4+len(payload), next)
	copy(allocated, payload)

	got := mf.Slice(0)
	require.Equal(t, payload, got)

	view, err := mf.View(4, len(payload))
	require.NoError(t, err)
	require.Equal(t, payload, view)

	b, err := mf.Bytes(4, len(payload))
	require.NoError(t, err)
	require.Equal(t, payload, b)

	reader := mf.NewReader(4)
	buf := make([]byte, len(payload))
	n, err := reader.Read(buf)
	require.NoError(t, err)
	require.Equal(t, len(payload), n)
	require.Equal(t, payload, buf)

	err = mf.AppendBuffer(uint32(next), []byte("tail"))
	require.NoError(t, err)

	require.NoError(t, mf.SyncAsync())
	require.NoError(t, mf.SyncAsyncRange(0, int64(len(mf.Data))))

	require.NoError(t, mf.Advise(utils.AccessPatternSequential))

	require.NoError(t, mf.Truncature(128))
	require.GreaterOrEqual(t, len(mf.Data), 128)

	_, _, err = mf.AllocateSlice(len(mf.Data), len(mf.Data)-4)
	require.NoError(t, err)

	require.NoError(t, mf.Remap(false))
	require.NoError(t, mf.Remap(true))
}

func TestMmapFileDeleteAndSyncDir(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap-delete.dat")

	mf, err := OpenMmapFile(path, os.O_CREATE|os.O_RDWR, 32)
	require.NoError(t, err)
	require.NotNil(t, mf)

	require.NoError(t, SyncDir(dir))
	require.NoError(t, mf.Delete())

	_, err = os.Stat(path)
	require.Error(t, err)
}

func TestMmapReaderEOF(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mmap-reader.dat")

	mf, err := OpenMmapFile(path, os.O_CREATE|os.O_RDWR, 16)
	require.NoError(t, err)
	defer func() { _ = mf.Close() }()

	reader := mf.NewReader(len(mf.Data) + 1)
	buf := make([]byte, 1)
	n, err := reader.Read(buf)
	require.Equal(t, 0, n)
	require.Equal(t, io.EOF, err)

	_, err = mf.Bytes(-1, 1)
	require.Error(t, err)
	_, err = mf.View(-1, 1)
	require.Error(t, err)

	empty := mf.Slice(0)
	require.True(t, bytes.Equal(empty, []byte{}))
}
