package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildTargets(t *testing.T) {
	targets := BuildTargets([]int64{0, 0, 100}, TargetOptions{
		BaseLevelSize:       10,
		LevelSizeMultiplier: 10,
		BaseTableSize:       4,
		TableSizeMultiplier: 2,
		MemTableSize:        8,
	})
	require.GreaterOrEqual(t, targets.BaseLevel, 1)
	require.Len(t, targets.TargetSz, 3)
	require.Len(t, targets.FileSz, 3)
	// MemTableSize seeds level 0 file size.
	require.Equal(t, int64(8), targets.FileSz[0])
}

func TestTargetsFileSizeForLevel(t *testing.T) {
	targets := Targets{
		FileSz:   []int64{0, 16, 32, 0},
		TargetSz: []int64{0, 100, 0, 200},
	}
	// File size present.
	require.Equal(t, int64(16), targets.FileSizeForLevel(1))
	// Falls back to TargetSz when FileSz is zero.
	require.Equal(t, int64(200), targets.FileSizeForLevel(3))
	// Out of range returns zero.
	require.Equal(t, int64(0), targets.FileSizeForLevel(-1))
	require.Equal(t, int64(0), targets.FileSizeForLevel(99))
}
