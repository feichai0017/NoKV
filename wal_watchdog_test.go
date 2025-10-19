package NoKV

import (
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func TestWALWatchdogAutoGC(t *testing.T) {
	opt := NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.EnableWALWatchdog = true
	opt.WALAutoGCInterval = time.Hour
	opt.WALAutoGCMinRemovable = 1
	opt.WALAutoGCMaxBatch = 2
	opt.WALTypedRecordWarnRatio = 0
	opt.WALTypedRecordWarnSegments = 0

	db := Open(opt)
	t.Cleanup(func() { _ = db.Close() })

	record := wal.Record{Type: wal.RecordTypeRaftEntry, Payload: []byte("raft-entry")}
	for i := 0; i < 4; i++ {
		_, err := db.wal.AppendRecords(record)
		require.NoError(t, err)
		if i < 3 {
			require.NoError(t, db.wal.Rotate())
		}
	}
	require.NoError(t, db.wal.Sync())

	ptr := manifest.RaftLogPointer{GroupID: 42, Segment: 4, SegmentIndex: 4}
	require.NoError(t, db.Manifest().LogRaftPointer(ptr))

	db.walWatchdog.runOnce()

	wSnap := db.walWatchdog.snapshot()
	require.Equal(t, uint64(1), wSnap.AutoRuns)
	require.GreaterOrEqual(t, wSnap.SegmentsRemoved, uint64(1))
	require.False(t, wSnap.Warning)

	files, err := db.wal.ListSegments()
	require.NoError(t, err)
	require.NotEmpty(t, files)
	var ids []string
	for _, f := range files {
		ids = append(ids, filepath.Base(f))
	}
	joined := strings.Join(ids, ",")
	require.NotContains(t, joined, "00001.wal")
	require.NotContains(t, joined, "00002.wal")

	snap := db.Info().Snapshot()
	require.Equal(t, uint64(1), snap.WALAutoGCRuns)
	require.GreaterOrEqual(t, snap.WALAutoGCRemoved, uint64(1))
	require.True(t, snap.WALRemovableRaftSegments >= 1)
	require.False(t, snap.WALTypedRecordWarning)
}

func TestWALWatchdogTypedWarning(t *testing.T) {
	opt := NewDefaultOptions()
	opt.WorkDir = t.TempDir()
	opt.EnableWALWatchdog = true
	opt.WALAutoGCInterval = time.Hour
	opt.WALAutoGCMinRemovable = 10
	opt.WALAutoGCMaxBatch = 0
	opt.WALTypedRecordWarnRatio = 0.1
	opt.WALTypedRecordWarnSegments = 0

	db := Open(opt)
	t.Cleanup(func() { _ = db.Close() })

	record := wal.Record{Type: wal.RecordTypeRaftEntry, Payload: []byte("raft-entry")}
	_, err := db.wal.AppendRecords(record)
	require.NoError(t, err)
	require.NoError(t, db.wal.Sync())

	db.walWatchdog.runOnce()

	wSnap := db.walWatchdog.snapshot()
	require.True(t, wSnap.Warning)
	require.NotEmpty(t, wSnap.WarningReason)
	require.Contains(t, wSnap.WarningReason, "typed record ratio")
	require.Equal(t, uint64(0), wSnap.AutoRuns)

	snap := db.Info().Snapshot()
	require.True(t, snap.WALTypedRecordWarning)
	require.Contains(t, snap.WALTypedRecordReason, "typed record ratio")
	require.Equal(t, uint64(0), snap.WALAutoGCRuns)
	require.Greater(t, snap.WALTypedRecordRatio, 0.0)
}
