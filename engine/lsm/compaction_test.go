// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package lsm

import (
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/stretchr/testify/require"
)

func TestStateCompareAndDelete(t *testing.T) {
	state := plan.NewState(2)
	entry := plan.StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: plan.KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: plan.KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.True(t, state.CompareAndAdd(plan.LevelsLocked{}, entry))
	require.True(t, state.HasRanges())
	require.True(t, state.HasTable(1))
	require.Equal(t, int64(128), state.DelSize(0))
	require.True(t, state.Overlaps(0, entry.ThisRange))

	overlap := entry
	overlap.ThisRange = plan.KeyRange{Left: ikey("a", 9), Right: ikey("b", 0)}
	require.False(t, state.CompareAndAdd(plan.LevelsLocked{}, overlap))

	require.Nil(t, state.Delete(entry))
	require.False(t, state.HasRanges())
	require.False(t, state.HasTable(1))
	require.Zero(t, state.DelSize(0))
}

func TestStateCompareAndDeleteIfKeyNotInRange(t *testing.T) {
	state := plan.NewState(2)
	entry := plan.StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: plan.KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: plan.KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.NotNil(t, state.Delete(entry))
}

func TestStateDeleteErrorIsAtomic(t *testing.T) {
	state := plan.NewState(2)
	entry := plan.StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: plan.KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: plan.KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.True(t, state.CompareAndAdd(plan.LevelsLocked{}, entry))

	missing := entry
	missing.ThisRange = plan.KeyRange{Left: ikey("x", 10), Right: ikey("y", 1)}
	err := state.Delete(missing)
	require.Error(t, err)
	require.Equal(t, int64(128), state.DelSize(0))
	require.True(t, state.HasRanges())
	require.True(t, state.HasTable(1))
	require.True(t, state.Overlaps(0, entry.ThisRange))

	require.NoError(t, state.Delete(entry))
	require.Zero(t, state.DelSize(0))
	require.False(t, state.HasRanges())
	require.False(t, state.HasTable(1))
}

func TestStateAddRangeAndDebug(t *testing.T) {
	state := plan.NewState(1)
	kr := plan.KeyRange{Left: ikey("a", 1), Right: ikey("b", 1)}
	state.AddRangeWithTables(0, kr, []uint64{10, 20})
	require.True(t, state.HasTable(10))
	require.Contains(t, kr.String(), "left=")
	require.True(t, state.HasRanges())
}

func TestNewSchedulerPolicy(t *testing.T) {
	require.Equal(t, PolicyLeveled, NewSchedulerPolicy("").mode)
	require.Equal(t, PolicyLeveled, NewSchedulerPolicy("unknown").mode)
	require.Equal(t, PolicyLeveled, NewSchedulerPolicy(PolicyLeveled).mode)
	require.Equal(t, PolicyTiered, NewSchedulerPolicy(PolicyTiered).mode)
	require.Equal(t, PolicyHybrid, NewSchedulerPolicy(PolicyHybrid).mode)
}

func TestSchedulerPolicyArrangeLeveled(t *testing.T) {
	p := NewSchedulerPolicy(PolicyLeveled)
	in := []plan.Priority{
		{Level: 1, Adjusted: 2},
		{Level: 0, Adjusted: 1},
		{Level: 2, Adjusted: 0.5},
	}

	forWorker0 := p.Arrange(0, in)
	require.Equal(t, 0, forWorker0[0].Level)
	require.Equal(t, 1, forWorker0[1].Level)

	forWorker1 := p.Arrange(1, in)
	require.Equal(t, 1, forWorker1[0].Level)
	require.Equal(t, 0, forWorker1[1].Level)
}

func TestSchedulerPolicyArrangeTieredPrefersLanding(t *testing.T) {
	p := NewSchedulerPolicy(PolicyTiered)
	in := []plan.Priority{
		{Level: 0, Adjusted: 9, LandingMode: plan.LandingNone},
		{Level: 3, Adjusted: 2, LandingMode: plan.LandingKeep},
		{Level: 2, Adjusted: 5, LandingMode: plan.LandingDrain},
		{Level: 1, Adjusted: 8, LandingMode: plan.LandingNone},
	}
	out := p.Arrange(0, in)
	require.Len(t, out, 4)
	require.Equal(t, 0, out[0].Level)
	require.Equal(t, plan.LandingKeep, out[1].LandingMode)
	require.Equal(t, plan.LandingDrain, out[2].LandingMode)
	require.Equal(t, 1, out[3].Level)
}

func TestSchedulerPolicyArrangeHybridSwitchesByLandingPressure(t *testing.T) {
	p := NewSchedulerPolicy(PolicyHybrid)
	withMildLanding := []plan.Priority{
		{Level: 1, Adjusted: 2, LandingMode: plan.LandingNone},
		{Level: 2, Adjusted: 1.5, LandingMode: plan.LandingDrain},
	}
	out := p.Arrange(0, withMildLanding)
	require.Equal(t, 1, out[0].Level)

	noLanding := []plan.Priority{
		{Level: 2, Adjusted: 2, LandingMode: plan.LandingNone},
		{Level: 0, Adjusted: 1.5, LandingMode: plan.LandingNone},
	}
	out = p.Arrange(0, noLanding)
	require.Equal(t, 0, out[0].Level)

	withHeavyLanding := []plan.Priority{
		{Level: 1, Adjusted: 1.2, LandingMode: plan.LandingNone},
		{Level: 2, Adjusted: 4.5, LandingMode: plan.LandingDrain},
		{Level: 3, Adjusted: 3.5, LandingMode: plan.LandingKeep},
	}
	out = p.Arrange(0, withHeavyLanding)
	require.Equal(t, plan.LandingKeep, out[0].LandingMode)
	require.Equal(t, plan.LandingDrain, out[1].LandingMode)
}

func TestSchedulerPolicyTieredFeedbackAdjustsQuota(t *testing.T) {
	baseInput := []plan.Priority{
		{Level: 0, Adjusted: 3.0, LandingMode: plan.LandingNone},
		{Level: 6, Adjusted: 6.0, LandingMode: plan.LandingKeep},
		{Level: 6, Adjusted: 5.9, LandingMode: plan.LandingKeep},
		{Level: 6, Adjusted: 5.8, LandingMode: plan.LandingKeep},
		{Level: 6, Adjusted: 5.7, LandingMode: plan.LandingKeep},
		{Level: 5, Adjusted: 6.5, LandingMode: plan.LandingDrain},
		{Level: 5, Adjusted: 6.4, LandingMode: plan.LandingDrain},
		{Level: 5, Adjusted: 6.3, LandingMode: plan.LandingDrain},
		{Level: 5, Adjusted: 6.2, LandingMode: plan.LandingDrain},
		{Level: 2, Adjusted: 5.5, LandingMode: plan.LandingNone},
		{Level: 2, Adjusted: 5.4, LandingMode: plan.LandingNone},
	}

	normal := NewSchedulerPolicy(PolicyTiered)
	normalOut := normal.Arrange(0, baseInput)
	normalIdx := firstRegularNonL0(normalOut)
	require.Greater(t, normalIdx, 0)

	failed := NewSchedulerPolicy(PolicyTiered)
	for range 3 {
		failed.Observe(FeedbackEvent{
			Priority: plan.Priority{LandingMode: plan.LandingDrain},
			Err:      errors.New("injected landing failure"),
		})
	}
	failedOut := failed.Arrange(0, baseInput)
	failedIdx := firstRegularNonL0(failedOut)
	require.Less(t, failedIdx, normalIdx, "landing failures should shift quota toward regular progress")

	success := NewSchedulerPolicy(PolicyTiered)
	for range 3 {
		success.Observe(FeedbackEvent{
			Priority: plan.Priority{LandingMode: plan.LandingKeep},
			Err:      nil,
		})
	}
	successOut := success.Arrange(0, baseInput)
	successIdx := firstRegularNonL0(successOut)
	require.Greater(t, successIdx, normalIdx, "landing successes should increase landing scheduling share")
}

func firstRegularNonL0(prios []plan.Priority) int {
	for i, p := range prios {
		if p.LandingMode == plan.LandingNone && p.Level != 0 {
			return i
		}
	}
	return -1
}

func tableRefSnapshot(tables []*table.Table) map[*table.Table]int32 {
	out := make(map[*table.Table]int32, len(tables))
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		out[tbl] = tbl.Load()
	}
	return out
}

func requireDecrOnce(t *testing.T, before map[*table.Table]int32) {
	t.Helper()
	for tbl, ref := range before {
		after := tbl.Load()
		if after != ref-1 {
			t.Fatalf("table %d ref mismatch: before=%d after=%d expected=%d", tbl.FID(), ref, after, ref-1)
		}
		if after < 0 {
			t.Fatalf("table %d ref underflow: after=%d", tbl.FID(), after)
		}
	}
}

func hasLandingTable(lh *levelHandler, fid uint64) bool {
	lh.RLock()
	defer lh.RUnlock()
	for _, tbl := range lh.landing.AllTables() {
		if tbl != nil && tbl.FID() == fid {
			return true
		}
	}
	return false
}

// trip for an IntraLevel entry: CompareAndAdd succeeds, Delete undoes the
// table claims and does NOT panic on missing range bookkeeping. IntraLevel
// is the marker used by L0→L0 compactions to claim by table id only.
func TestStateIntraLevelEntryDeletesCleanly(t *testing.T) {
	state := plan.NewState(8)
	entry := plan.StateEntry{
		ThisLevel:  0,
		NextLevel:  0,
		TableIDs:   []uint64{1, 2, 3, 4},
		IntraLevel: true,
	}
	require.True(t, state.CompareAndAdd(plan.LevelsLocked{}, entry))
	require.True(t, state.HasTable(1))
	require.True(t, state.HasTable(4))
	require.False(t, state.HasTable(99))
	require.NoError(t, state.Delete(entry))
	require.False(t, state.HasTable(1))
	require.False(t, state.HasTable(4))
}
