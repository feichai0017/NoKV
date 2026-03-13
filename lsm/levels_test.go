package lsm

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type testWALGCPolicy struct {
	blocked map[uint32]struct{}
}

// CanRemoveSegment returns false for blocked segment IDs and true otherwise.
func (p testWALGCPolicy) CanRemoveSegment(segmentID uint32) bool {
	_, ok := p.blocked[segmentID]
	return !ok
}

func TestLevelsRuntimeCanRemoveWalSegmentDelegatesPolicy(t *testing.T) {
	lsm := &LSM{walGCPolicy: testWALGCPolicy{
		blocked: map[uint32]struct{}{
			3: {},
			8: {},
		},
	}}
	lm := &levelsRuntime{lsm: lsm}
	require.True(t, lm.canRemoveWalSegment(1))
	require.False(t, lm.canRemoveWalSegment(3))
	require.True(t, lm.canRemoveWalSegment(7))
	require.False(t, lm.canRemoveWalSegment(8))
}

func TestLevelsRuntimeCanRemoveWalSegmentNilLSM(t *testing.T) {
	lm := &levelsRuntime{}
	require.True(t, lm.canRemoveWalSegment(1))
}

func TestL0ReplaceTablesOrdering(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	t1 := buildTableWithEntry(t, lsm, 1, "C", 1, "old")
	t2 := buildTableWithEntry(t, lsm, 2, "A", 1, "old")
	t3 := buildTableWithEntry(t, lsm, 3, "B", 1, "old")
	t4 := buildTableWithEntry(t, lsm, 4, "A", 2, "new")

	levelHandler := lsm.levels.levels[0]
	levelHandler.tables = []*table{t1, t2, t3}
	toDel := []*table{t2, t3}
	toAdd := []*table{t4}
	require.NoError(t, levelHandler.replaceTables(toDel, toAdd))
	require.Equal(t, []*table{t1, t4}, levelHandler.tables)

	require.NoError(t, t1.DecrRef())
	require.NoError(t, t4.DecrRef())
}
