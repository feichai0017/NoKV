package NoKV

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

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

		db := Open(opts)
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
