package lsm

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/wal"
)

func TestLevelManagerSlowFollowerPreventsWalGC(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	manifestDir := filepath.Join(root, "manifest")

	walMgr, err := wal.Open(wal.Config{Dir: walDir})
	require.NoError(t, err)
	defer func() { _ = walMgr.Close() }()

	manifestMgr, err := manifest.Open(manifestDir, nil)
	require.NoError(t, err)
	defer func() { _ = manifestMgr.Close() }()

	lm := &levelManager{manifestMgr: manifestMgr}

	require.True(t, lm.canRemoveWalSegment(1))

	ptr := manifest.RaftLogPointer{
		GroupID: 1,
		Segment: 3,
		Offset:  128,
	}
	require.NoError(t, manifestMgr.LogRaftPointer(ptr))

	require.True(t, lm.canRemoveWalSegment(2))
	require.False(t, lm.canRemoveWalSegment(3))
	require.False(t, lm.canRemoveWalSegment(4))

	ptr = manifest.RaftLogPointer{
		GroupID: 1,
		Segment: 10,
		Offset:  256,
	}
	require.NoError(t, manifestMgr.LogRaftPointer(ptr))

	require.True(t, lm.canRemoveWalSegment(9))

	require.NoError(t, manifestMgr.LogRaftTruncate(1, 50, 5, 8, 0))

	require.True(t, lm.canRemoveWalSegment(7))
	require.False(t, lm.canRemoveWalSegment(8))
	require.False(t, lm.canRemoveWalSegment(9))
}

func TestLevelManagerMultiGroupTruncationBlocksWalGC(t *testing.T) {
	root := t.TempDir()
	walDir := filepath.Join(root, "wal")
	manifestDir := filepath.Join(root, "manifest")

	walMgr, err := wal.Open(wal.Config{Dir: walDir})
	require.NoError(t, err)
	defer func() { _ = walMgr.Close() }()

	manifestMgr, err := manifest.Open(manifestDir, nil)
	require.NoError(t, err)
	defer func() { _ = manifestMgr.Close() }()

	lm := &levelManager{manifestMgr: manifestMgr}

	// Initialize pointers for two groups sharing the manifest.
	require.NoError(t, manifestMgr.LogRaftPointer(manifest.RaftLogPointer{
		GroupID:      1,
		Segment:      4,
		Offset:       4096,
		AppliedIndex: 200,
		AppliedTerm:  8,
	}))
	require.NoError(t, manifestMgr.LogRaftPointer(manifest.RaftLogPointer{
		GroupID:      2,
		Segment:      4,
		Offset:       2048,
		AppliedIndex: 120,
		AppliedTerm:  7,
	}))

	// Group 1 truncates past segment 3, but group 2 lags at segment 2.
	require.NoError(t, manifestMgr.LogRaftTruncate(1, 180, 8, 3, 0))
	require.NoError(t, manifestMgr.LogRaftTruncate(2, 100, 7, 2, 0))

	require.False(t, lm.canRemoveWalSegment(2), "segment 2 must be retained while group 2 lags")
	require.True(t, lm.canRemoveWalSegment(1), "segment 1 has no remaining references")

	// Once group 2 truncates beyond segment 2, segment 2 can be removed.
	require.NoError(t, manifestMgr.LogRaftTruncate(2, 140, 8, 5, 0))
	require.True(t, lm.canRemoveWalSegment(2), "segment 2 becomes eligible after all groups advance")

	// To mirror real GC, create dummy WAL segments and ensure removal aligns with canRemoveWalSegment.
	for seg := uint32(1); seg <= 3; seg++ {
		require.NoError(t, walMgr.SwitchSegment(seg, true))
	}
	require.NoError(t, walMgr.SwitchSegment(4, true))
	require.True(t, lm.canRemoveWalSegment(2))
	require.NoError(t, walMgr.RemoveSegment(2))
}

func TestL0ReplaceTablesOrdering(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		_ = lsm.Close()
		_ = os.RemoveAll(lsm.option.WorkDir)
	}()

	t1 := buildTableWithEntry(t, lsm, 1, "C", 1, "old")
	t2 := buildTableWithEntry(t, lsm, 2, "A", 1, "old")
	t3 := buildTableWithEntry(t, lsm, 3, "B", 1, "old")
	t4 := buildTableWithEntry(t, lsm, 4, "A", 2, "new")

	levelHandler := lsm.levels.levels[0]
	levelHandler.tables = []*table{t1, t2, t3}
	toDel := []*table{t2, t3}
	toAdd := []*table{t4}
	err := levelHandler.replaceTables(toDel, toAdd)
	require.NoError(t, err)
	require.Equal(t, []*table{t1, t4}, levelHandler.tables)

	t1.DecrRef()
	t4.DecrRef()
}
