package NoKV

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

// TestSyncPipelineWALConsistency opens two DBs (one with SyncPipeline off, one
// with SyncPipeline on), writes the same keys, closes them, then compares the
// raw WAL file bytes to make sure they are identical.
func TestSyncPipelineWALConsistency(t *testing.T) {
	const numKeys = 10
	value := []byte("hello-sync-pipeline")

	// readWALFiles returns all *.wal file contents concatenated in sorted order.
	readWALFiles := func(dir string) []byte {
		matches, err := filepath.Glob(filepath.Join(dir, "*.wal"))
		require.NoError(t, err)
		var all []byte
		for _, f := range matches {
			data, err := os.ReadFile(f)
			require.NoError(t, err)
			all = append(all, data...)
		}
		return all
	}

	writeAndClose := func(dir string, pipeline bool) {
		opts := NewDefaultOptions()
		opts.WorkDir = dir
		opts.SyncWrites = true
		opts.SyncPipeline = pipeline
		opts.EnableWALWatchdog = false
		opts.ValueLogGCInterval = 0
		opts.ManifestSync = false
		opts.ValueThreshold = 1 << 20 // all values inline, no vlog pointers
		opts.WriteBatchWait = 0

		db := openTestDB(t, opts)
		for i := range numKeys {
			key := fmt.Appendf(nil, "key-%04d", i)
			require.NoError(t, db.Set(key, value))
			// wal should be flushed and synced after each Set since SyncWrites=true,
			//so we can compare the WAL contents after Set
		}
		require.NoError(t, db.Close())
	}

	dirInline := t.TempDir()
	dirPipeline := t.TempDir()

	writeAndClose(dirInline, false)
	writeAndClose(dirPipeline, true)

	walInline := readWALFiles(dirInline)
	walPipeline := readWALFiles(dirPipeline)

	require.NotEmpty(t, walInline, "inline WAL should not be empty")
	require.NotEmpty(t, walPipeline, "pipeline WAL should not be empty")
	require.Equal(t, walInline, walPipeline,
		"WAL file contents should be identical between SyncPipeline=false and SyncPipeline=true")
}

func TestSendToWriteChWaitsForThrottleClear(t *testing.T) {
	opts := newTestOptions(t)
	opts.WriteBatchWait = 0
	db := openTestDB(t, opts)
	defer func() { _ = db.Close() }()

	db.applyThrottle(lsm.WriteThrottleStop)
	defer db.applyThrottle(lsm.WriteThrottleNone)

	done := make(chan error, 1)
	go func() {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("throttle-clear"), 1, []byte("value"), 0, 0)
		req, err := db.sendToWriteCh([]*kv.Entry{entry}, true)
		if err != nil {
			entry.DecrRef()
			done <- err
			return
		}
		done <- req.Wait()
	}()

	select {
	case err := <-done:
		t.Fatalf("write finished before throttle cleared: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	db.applyThrottle(lsm.WriteThrottleNone)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("write did not resume after throttle cleared")
	}
}

func TestSendToWriteChReturnsBlockedWritesWhenClosedWhileThrottled(t *testing.T) {
	opts := newTestOptions(t)
	opts.WriteBatchWait = 0
	db := openTestDB(t, opts)

	db.applyThrottle(lsm.WriteThrottleStop)

	done := make(chan error, 1)
	go func() {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte("throttle-close"), 1, []byte("value"), 0, 0)
		_, err := db.sendToWriteCh([]*kv.Entry{entry}, true)
		if err != nil {
			entry.DecrRef()
		}
		done <- err
	}()

	select {
	case err := <-done:
		t.Fatalf("write finished before db close: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	require.NoError(t, db.Close())

	select {
	case err := <-done:
		require.ErrorIs(t, err, utils.ErrBlockedWrites)
	case <-time.After(2 * time.Second):
		t.Fatal("throttled write did not return after db close")
	}
}
