package lsm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestStagingModeFlags(t *testing.T) {
	require.False(t, StagingNone.UsesStaging())
	require.True(t, StagingDrain.UsesStaging())
	require.True(t, StagingKeep.UsesStaging())
	require.True(t, StagingKeep.KeepsStaging())
	require.False(t, StagingDrain.KeepsStaging())
}

func TestStagingPicker(t *testing.T) {
	shards := []StagingShardView{
		{Index: 1, SizeBytes: 10},
		{Index: 2, SizeBytes: 30},
		{Index: 3, SizeBytes: 20, MaxAgeSec: 120, ValueDensity: 0.5},
	}
	order := PickShardOrder(StagingPickInput{Shards: shards})
	require.Equal(t, []int{2, 3, 1}, order)

	pick := PickShardByBacklog(StagingPickInput{Shards: shards})
	require.Equal(t, 3, pick)

	require.Equal(t, -1, PickShardByBacklog(StagingPickInput{}))
}

func TestPriorityHelpers(t *testing.T) {
	prios := []Priority{
		{Level: 1, Score: 2},
		{Level: 0, Score: 3},
	}
	out := MoveL0ToFront(prios)
	require.Equal(t, 0, out[0].Level)

	p := Priority{Score: 1}
	p.ApplyValueWeight(1.5, 2.0)
	require.Greater(t, p.Score, 1.0)
	require.Equal(t, p.Score, p.Adjusted)
}

func TestPickPrioritiesBoostsRangeTombstoneDenseLevels(t *testing.T) {
	targets := Targets{
		BaseLevel: 1,
		TargetSz:  []int64{0, 100, 100},
		FileSz:    []int64{10, 10, 10},
	}
	input := PickerInput{
		Levels: []LevelInput{
			{Level: 0, NumTables: 0},
			{Level: 1, TotalSize: 50, KeyCount: 100, RangeTombstones: 50},
			{Level: 2, TotalSize: 10, KeyCount: 100},
		},
		Targets:                   targets,
		NumLevelZeroTables:        8,
		BaseTableSize:             10,
		BaseLevelSize:             100,
		CompactionTombstoneWeight: 1.0,
	}
	prios := PickPriorities(input)
	require.NotEmpty(t, prios)
	require.Equal(t, 1, prios[0].Level)
	require.Greater(t, prios[0].Score, 1.0)
}

func TestBuildTargetsAndPickPriorities(t *testing.T) {
	targets := BuildTargets([]int64{0, 0, 100}, TargetOptions{
		BaseLevelSize:       10,
		LevelSizeMultiplier: 10,
		BaseTableSize:       4,
		TableSizeMultiplier: 2,
		MemTableSize:        8,
	})
	require.GreaterOrEqual(t, targets.BaseLevel, 1)

	input := PickerInput{
		Levels: []LevelInput{
			{
				Level:           0,
				NumTables:       4,
				TotalValueBytes: 100,
			},
			{
				Level:               1,
				StagingTables:       2,
				StagingSize:         200,
				StagingValueBytes:   40,
				StagingValueDensity: 1.5,
				StagingAgeSeconds:   200,
				MainValueBytes:      30,
			},
		},
		Targets:                  targets,
		NumLevelZeroTables:       4,
		BaseTableSize:            4,
		BaseLevelSize:            10,
		StagingBacklogMergeScore: 1.0,
		CompactionValueWeight:    1.0,
	}
	prios := PickPriorities(input)
	require.NotEmpty(t, prios)

	var hasStagingDrain bool
	for _, p := range prios {
		if p.StagingMode == StagingDrain {
			hasStagingDrain = true
		}
	}
	require.True(t, hasStagingDrain)
}
