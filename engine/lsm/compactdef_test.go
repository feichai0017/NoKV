package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/stretchr/testify/require"
)

func TestCompactDefTargetFileSize(t *testing.T) {
	cd := compactDef{
		spec: plan.Plan{
			ThisLevel:    1,
			ThisFileSize: 4096,
			NextLevel:    2,
			NextFileSize: 8192,
		},
	}
	require.Equal(t, int64(4096), cd.targetFileSize())
}

func TestCompactDefFileSize(t *testing.T) {
	cd := compactDef{
		spec: plan.Plan{
			ThisLevel:    3,
			ThisFileSize: 1024,
			NextLevel:    4,
			NextFileSize: 2048,
		},
	}
	require.Equal(t, int64(1024), cd.fileSize(3))
	require.Equal(t, int64(2048), cd.fileSize(4))
	// Out-of-band level returns zero so callers can detect mis-routed queries.
	require.Equal(t, int64(0), cd.fileSize(99))
}

func TestCompactDefStateEntryReflectsThisSize(t *testing.T) {
	cd := compactDef{
		spec: plan.Plan{
			ThisLevel: 0,
			NextLevel: 1,
			TopIDs:    []uint64{1, 2},
			BotIDs:    []uint64{3},
		},
		thisSize: 256,
	}
	entry := cd.stateEntry()
	require.Equal(t, int64(256), entry.ThisSize)
	require.ElementsMatch(t, []uint64{1, 2, 3}, entry.TableIDs)
}

func TestCompactDefSetNextLevelBindsTargets(t *testing.T) {
	targets := plan.Targets{
		FileSz:   []int64{0, 1024, 2048, 4096},
		TargetSz: []int64{0, 100, 200, 400},
	}
	next := &levelHandler{levelNum: 3}

	cd := compactDef{
		spec: plan.Plan{ThisLevel: 2, ThisFileSize: 1024},
	}
	cd.setNextLevel(targets, next)
	require.Equal(t, next, cd.nextLevel)
	require.Equal(t, 3, cd.spec.NextLevel)
	require.Equal(t, int64(4096), cd.spec.NextFileSize)
}

func TestCompactDefSetNextLevelNilLeavesPlanUntouched(t *testing.T) {
	cd := compactDef{
		spec: plan.Plan{NextLevel: 5, NextFileSize: 999},
	}
	cd.setNextLevel(plan.Targets{}, nil)
	require.Nil(t, cd.nextLevel)
	require.Equal(t, 5, cd.spec.NextLevel)
	require.Equal(t, int64(999), cd.spec.NextFileSize)
}

func TestCompactDefApplyPlanPreservesBuilderFields(t *testing.T) {
	cd := compactDef{
		spec: plan.Plan{
			ThisFileSize: 1024,
			NextFileSize: 2048,
			LandingMode:  plan.LandingDrain,
			DropPrefixes: [][]byte{[]byte("legacy/")},
			StatsTag:     "executor-tag",
		},
	}
	newPlan := plan.Plan{
		ThisLevel: 7,
		NextLevel: 8,
		TopIDs:    []uint64{42},
	}
	cd.applyPlan(newPlan)

	// Plan-supplied fields override.
	require.Equal(t, 7, cd.spec.ThisLevel)
	require.Equal(t, 8, cd.spec.NextLevel)
	require.ElementsMatch(t, []uint64{42}, cd.spec.TopIDs)
	// Builder-relevant fields preserved from the prior spec.
	require.Equal(t, int64(1024), cd.spec.ThisFileSize)
	require.Equal(t, int64(2048), cd.spec.NextFileSize)
	require.Equal(t, plan.LandingDrain, cd.spec.LandingMode)
	require.Equal(t, "executor-tag", cd.spec.StatsTag)
	require.Equal(t, [][]byte{[]byte("legacy/")}, cd.spec.DropPrefixes)
}

func TestCompactDefLockUnlockSameLevelTakesOneLock(t *testing.T) {
	lh := &levelHandler{levelNum: 0}
	cd := compactDef{thisLevel: lh, nextLevel: lh}
	// The shared single-handler case should not double-RLock; lockLevels +
	// unlockLevels must be balanced. Failure mode is a deadlock or panic.
	cd.lockLevels()
	cd.unlockLevels()
}

func TestCompactDefLockUnlockDistinctLevels(t *testing.T) {
	this := &levelHandler{levelNum: 1}
	next := &levelHandler{levelNum: 2}
	cd := compactDef{thisLevel: this, nextLevel: next}
	cd.lockLevels()
	cd.unlockLevels()
}
