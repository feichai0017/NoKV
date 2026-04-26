package file

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/vfs"
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

	fd, ok := lf.FileFD()
	require.True(t, ok)
	require.NotZero(t, fd)
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
	require.GreaterOrEqual(t, lf.Size(), int64(len(data)))

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
	require.GreaterOrEqual(t, lf.Size(), int64(len(data)))

	err := lf.DoneWriting(uint32(len(data)))
	require.Error(t, err)
	require.Contains(t, err.Error(), "sync failure")
	require.Contains(t, err.Error(), "Unable to sync file descriptor (metadata) after truncate")
}

// TestLogFileWriteSizeMonotonicOutOfOrder reproduces the production race
// where vlog/manager.reserve() hands out non-overlapping offset ranges
// without serializing the subsequent store.Write calls. If a Write that
// landed at a larger offset finishes first, a plain size.Store from a
// later Write at a smaller offset would shrink lfsz back below an
// already-published pointer, producing spurious EOF on Read. This test
// drives that exact pattern under a serializing lock (matching how
// store.Lock guards Write in production) and asserts lfsz only ever
// advances.
func TestLogFileWriteSizeMonotonicOutOfOrder(t *testing.T) {
	dir := t.TempDir()
	opt := &Options{
		FID:      42,
		FileName: filepath.Join(dir, "vlog-mono"),
		MaxSz:    16 << 20,
	}

	var lf LogFile
	require.NoError(t, lf.Open(opt))
	defer func() { _ = lf.Close() }()

	// Truncate down to the header high-water mark so subsequent writes
	// extend the live region. Mirrors a freshly-rotated active segment
	// where vlog.Manager.offset starts at the header.
	require.NoError(t, lf.Truncate(int64(kv.ValueLogHeaderSize)))

	const (
		entries     = 1024
		payloadSize = 1024
	)
	payload := make([]byte, payloadSize)
	for i := range payload {
		payload[i] = byte(i & 0xff)
	}

	// Reserve N disjoint ranges in ascending order (matches reserve()).
	starts := make([]uint32, entries)
	cursor := uint32(kv.ValueLogHeaderSize)
	for i := range starts {
		starts[i] = cursor
		cursor += uint32(payloadSize)
	}

	// Replay the writes in REVERSE order under a single lock — same
	// invariant as production where store.Lock serializes Writes but
	// reserve() decouples reservation order from execution order. The
	// largest-offset Write lands first; without monotonic CAS, the
	// smallest-offset Write would then shrink lfsz to (start + len)
	// of the smallest reservation.
	var lock sync.Mutex
	highWater := uint32(0)
	for i := entries - 1; i >= 0; i-- {
		start := starts[i]
		lock.Lock()
		require.NoError(t, lf.Write(start, payload))
		end := start + uint32(payloadSize)
		if end > highWater {
			highWater = end
		}
		lfsz := uint32(lf.Size())
		require.GreaterOrEqual(t, lfsz, highWater,
			"lfsz=%d shrunk below high-water=%d after Write at start=%d", lfsz, highWater, start)
		lock.Unlock()
	}

	// Final lfsz must cover the largest reservation.
	require.Equal(t, int64(cursor), lf.Size(), "final lfsz must cover all reservations")
}
