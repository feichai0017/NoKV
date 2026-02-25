package file

import (
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/vfs"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestLogFileBootstrapReadWrite(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		FID:      1,
		FileName: filepath.Join(dir, "vlog"),
		MaxSz:    1 << 20,
	}

	var lf LogFile
	require.NoError(t, lf.Open(opt))
	defer func() { _ = lf.Close() }()

	require.NoError(t, lf.Bootstrap())

	payload := []byte("hello-world")
	offset := kv.ValueLogHeaderSize
	require.NoError(t, lf.Write(uint32(offset), payload))

	vp := &kv.ValuePtr{Offset: uint32(offset), Len: uint32(len(payload))}
	read, err := lf.Read(vp)
	require.NoError(t, err)
	require.Equal(t, payload, read)

	require.NoError(t, lf.SetReadOnly())
	require.True(t, lf.ro)
	require.NoError(t, lf.SetWritable())
	require.False(t, lf.ro)
}

func TestLogFileLifecycleHelpers(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "vlog2")
	opt := &Options{
		FID:      9,
		FileName: path,
		MaxSz:    1 << 20,
	}

	var lf LogFile
	require.NoError(t, lf.Open(opt))
	defer func() { _ = lf.Close() }()

	require.NoError(t, lf.Bootstrap())

	payload := []byte("log-data")
	offset := kv.ValueLogHeaderSize
	require.NoError(t, lf.Write(uint32(offset), payload))
	require.Greater(t, lf.Size(), int64(0))

	require.NoError(t, lf.Sync())
	pos, err := lf.Seek(0, 0)
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	require.NotNil(t, lf.FD())
	require.NotNil(t, lf.File())
	require.Equal(t, path, lf.FileName())

	end := uint32(offset + len(payload))
	require.NoError(t, lf.DoneWriting(end))
	require.Equal(t, int64(end), lf.Size())

	require.NoError(t, lf.Truncate(int64(offset)))
	require.Equal(t, int64(offset), lf.Size())

	require.NoError(t, lf.Init())
}

func TestDoneWritingTruncateFailure(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "vlog")

	truncateErr := errors.New("truncate failure")
	policy := vfs.NewFaultPolicy(
		vfs.FailOnNthRule(vfs.OpFileTrunc, filename, 2, truncateErr),
	)
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	opt := &Options{
		FID:      1,
		FileName: filename,
		MaxSz:    1 << 10,
		FS:       fs,
	}
	var lf LogFile
	require.NoError(t, lf.Open(opt))
	defer func() { _ = lf.Close() }()

	require.NoError(t, lf.SetWritable())
	data := []byte("TestDoneWritingTruncateFailure")
	require.NoError(t, lf.Write(0, data))
	require.Equal(t, int64(len(data)), lf.Size())

	err := lf.DoneWriting(uint32(len(data)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "truncate failure")
	require.Contains(t, err.Error(), "Unable to truncate file")
}

func TestDoneWritingFileSyncFailure(t *testing.T) {
	dir := t.TempDir()
	filename := filepath.Join(dir, "vlog")

	syncErr := errors.New("sync failure")
	policy := vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpFileSync, filename, syncErr),
	)
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	opt := &Options{
		FID:      1,
		FileName: filename,
		MaxSz:    1 << 10,
		FS:       fs,
	}
	var lf LogFile
	require.NoError(t, lf.Open(opt))
	defer func() { _ = lf.Close() }()

	require.NoError(t, lf.SetWritable())
	data := []byte("TestDoneWritingFileSyncFailure")
	require.NoError(t, lf.Write(0, data))
	require.Equal(t, int64(len(data)), lf.Size())

	err := lf.DoneWriting(uint32(len(data)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "sync failure")
	require.Contains(t, err.Error(), "Unable to sync file descriptor (metadata) after truncate")
}
