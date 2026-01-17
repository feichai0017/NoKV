package compact

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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
				HotOverlap:      0.5,
			},
			{
				Level:              1,
				IngestTables:       2,
				IngestSize:         200,
				IngestValueBytes:   40,
				IngestValueDensity: 1.5,
				IngestAgeSeconds:   200,
				MainValueBytes:     30,
				HotOverlapIngest:   0.7,
			},
		},
		Targets:                 targets,
		NumLevelZeroTables:      4,
		BaseTableSize:           4,
		BaseLevelSize:           10,
		IngestBacklogMergeScore: 1.0,
		CompactionValueWeight:   1.0,
	}
	prios := PickPriorities(input)
	require.NotEmpty(t, prios)

	var hasIngestDrain bool
	for _, p := range prios {
		if p.IngestMode == IngestDrain {
			hasIngestDrain = true
		}
	}
	require.True(t, hasIngestDrain)
}
