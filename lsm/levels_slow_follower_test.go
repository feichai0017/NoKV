package lsm

import (
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
	defer walMgr.Close()

	manifestMgr, err := manifest.Open(manifestDir)
	require.NoError(t, err)
	defer manifestMgr.Close()

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
}
