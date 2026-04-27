package lsm

import (
	"bytes"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

func TestCompactionMoveToStaging(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	// Generate enough data to force multiple L0 tables.
	for range 3 {
		baseTest(t, lsm, 256)
	}

	l0 := lsm.levels.levels[0]
	tables := l0.tablesSnapshot()
	if len(tables) == 0 {
		t.Fatalf("expected L0 to have tables after writes")
	}

	cd := buildCompactDef(lsm, 0, 0, 1)
	cd.top = []*table{tables[0]}
	cd.plan.ThisRange = getKeyRange(cd.top...)
	cd.plan.NextRange = cd.plan.ThisRange
	if cd.nextLevel == nil {
		cd.nextLevel = lsm.levels.levels[1]
	}

	beforeStaging := cd.nextLevel.numStagingTables()
	if err := lsm.levels.moveToStaging(cd); err != nil {
		t.Fatalf("moveToStaging: %v", err)
	}
	afterStaging := cd.nextLevel.numStagingTables()
	if afterStaging <= beforeStaging {
		t.Fatalf("expected staging buffer to grow, before=%d after=%d", beforeStaging, afterStaging)
	}

	// Ensure the moved table has been removed from the source level.
	found := false
	cd.nextLevel.RLock()
	for _, sh := range cd.nextLevel.staging.shards {
		for _, tbl := range sh.tables {
			if tbl.fid == cd.top[0].fid {
				found = true
				break
			}
		}
		if found {
			break
		}
	}
	cd.nextLevel.RUnlock()
	if !found {
		t.Fatalf("table %d not found in staging buffer", cd.top[0].fid)
	}
}

func TestCompactionTrivialMoveToNextLevel(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tbl := buildTableWithEntry(t, lsm, 1101, "trivial-move", 3, "value")
	src := lsm.levels.levels[1]
	dst := lsm.levels.levels[2]
	src.add(tbl)

	cd := buildCompactDef(lsm, 0, 1, 2)
	cd.top = []*table{tbl}
	cd.plan.TopIDs = []uint64{tbl.fid}
	cd.plan.ThisRange = getKeyRange(tbl)
	cd.plan.NextRange = cd.plan.ThisRange
	cd.thisSize = tbl.Size()

	beforeRef := tbl.Load()
	require.True(t, lsm.levels.canMoveToNextLevel(cd))
	require.NoError(t, lsm.levels.moveToNextLevel(cd))
	require.Equal(t, beforeRef, tbl.Load())
	require.Equal(t, int32(2), tbl.lvl.Load())

	require.Empty(t, src.tablesSnapshot())
	got := dst.tablesSnapshot()
	require.Len(t, got, 1)
	require.Equal(t, tbl.fid, got[0].fid)

	version := lsm.levels.manifestMgr.Current()
	require.Empty(t, version.Levels[1])
	require.Len(t, version.Levels[2], 1)
	require.Equal(t, tbl.fid, version.Levels[2][0].FileID)
}

func TestCompactBuildTablesOverlappingBotTablesKeepsOrder(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	top := buildTableWithEntries(t, lsm, 1001,
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("u"), 3), []byte("top-u")),
	)
	botA := buildTableWithEntries(t, lsm, 1002,
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("m"), 2), []byte("bot-a-m")),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("z"), 2), []byte("bot-a-z")),
	)
	// botB overlaps botA on user-key range [t,y], so concat order would be unsafe.
	botB := buildTableWithEntries(t, lsm, 1003,
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("t"), 1), []byte("bot-b-t")),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("y"), 1), []byte("bot-b-y")),
	)
	defer func() {
		_ = top.DecrRef()
		_ = botA.DecrRef()
		_ = botB.DecrRef()
	}()

	cd := compactDef{
		thisLevel: lsm.levels.levels[5],
		nextLevel: lsm.levels.levels[6],
		top:       []*table{top},
		bot:       []*table{botA, botB},
		splits:    []KeyRange{{}},
		plan: Plan{
			NextFileSize: 1 << 20,
		},
	}

	newTables, decr, err := lsm.levels.compactBuildTables(5, cd)
	if err != nil {
		t.Fatalf("compactBuildTables: %v", err)
	}
	defer func() {
		if decr != nil {
			_ = decr()
		}
	}()
	if len(newTables) == 0 {
		t.Fatalf("expected output tables")
	}

	for _, tbl := range newTables {
		it := tbl.NewIterator(&index.Options{IsAsc: true})
		if it == nil {
			t.Fatalf("nil iterator for output table")
		}
		var prev []byte
		var seen [][]byte
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if item == nil || item.Entry() == nil {
				continue
			}
			cur := item.Entry().Key
			if prev != nil && kv.CompareInternalKeys(prev, cur) > 0 {
				_ = it.Close()
				t.Fatalf("output table out of order: prev=%q cur=%q", prev, cur)
			}
			prev = kv.SafeCopy(prev, cur)
			_, user, _, ok := kv.SplitInternalKey(cur)
			if !ok {
				_ = it.Close()
				t.Fatalf("expected internal key, got %x", cur)
			}
			seen = append(seen, kv.SafeCopy(nil, user))
		}
		_ = it.Close()
		joined := bytes.Join(seen, []byte(","))
		if !bytes.Contains(joined, []byte("m")) ||
			!bytes.Contains(joined, []byte("t")) ||
			!bytes.Contains(joined, []byte("u")) ||
			!bytes.Contains(joined, []byte("y")) ||
			!bytes.Contains(joined, []byte("z")) {
			t.Fatalf("expected merged output keys m,t,u,y,z; got %q", string(joined))
		}
	}
}

func TestCompactStatusGuards(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)

	l0 := lsm.levels.levels[0]
	tables := l0.tablesSnapshot()
	if len(tables) == 0 {
		t.Fatalf("expected L0 tables for compact status test")
	}
	tbl := tables[0]

	cd := compactDef{
		thisLevel: l0,
		nextLevel: l0,
		top:       []*table{tbl},
		plan: Plan{
			ThisLevel:    0,
			NextLevel:    0,
			ThisRange:    getKeyRange(tbl),
			NextRange:    getKeyRange(tbl),
			ThisFileSize: 1,
			NextFileSize: 1,
		},
		thisSize: tbl.Size(),
	}
	cs := lsm.newCompactStatus()
	if !cs.CompareAndAdd(LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected first compareAndAdd to succeed")
	}
	if cs.CompareAndAdd(LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected overlapping compaction to be rejected")
	}
	require.Nil(t, cs.Delete(cd.stateEntry()))
	if !cs.CompareAndAdd(LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected compareAndAdd to succeed after delete")
	}
}

func TestStateCompareAndDelete(t *testing.T) {
	state := NewState(2)
	entry := StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.True(t, state.CompareAndAdd(LevelsLocked{}, entry))
	require.True(t, state.HasRanges())
	require.True(t, state.HasTable(1))
	require.Equal(t, int64(128), state.DelSize(0))
	require.True(t, state.Overlaps(0, entry.ThisRange))

	overlap := entry
	overlap.ThisRange = KeyRange{Left: ikey("a", 9), Right: ikey("b", 0)}
	require.False(t, state.CompareAndAdd(LevelsLocked{}, overlap))

	require.Nil(t, state.Delete(entry))
	require.False(t, state.HasRanges())
	require.False(t, state.HasTable(1))
	require.Zero(t, state.DelSize(0))
}

func TestStateCompareAndDeleteIfKeyNotInRange(t *testing.T) {
	state := NewState(2)
	entry := StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.NotNil(t, state.Delete(entry))
}

func TestStateDeleteErrorIsAtomic(t *testing.T) {
	state := NewState(2)
	entry := StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.True(t, state.CompareAndAdd(LevelsLocked{}, entry))

	missing := entry
	missing.ThisRange = KeyRange{Left: ikey("x", 10), Right: ikey("y", 1)}
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
	state := NewState(1)
	kr := KeyRange{Left: ikey("a", 1), Right: ikey("b", 1)}
	state.AddRangeWithTables(0, kr, []uint64{10, 20})
	require.True(t, state.HasTable(10))
	require.Contains(t, kr.String(), "left=")
	require.NotEmpty(t, state.levels[0].debug())
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
	in := []Priority{
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

func TestSchedulerPolicyArrangeTieredPrefersStaging(t *testing.T) {
	p := NewSchedulerPolicy(PolicyTiered)
	in := []Priority{
		{Level: 0, Adjusted: 9, StagingMode: StagingNone},
		{Level: 3, Adjusted: 2, StagingMode: StagingKeep},
		{Level: 2, Adjusted: 5, StagingMode: StagingDrain},
		{Level: 1, Adjusted: 8, StagingMode: StagingNone},
	}
	out := p.Arrange(0, in)
	require.Len(t, out, 4)
	require.Equal(t, 0, out[0].Level)
	require.Equal(t, StagingKeep, out[1].StagingMode)
	require.Equal(t, StagingDrain, out[2].StagingMode)
	require.Equal(t, 1, out[3].Level)
}

func TestSchedulerPolicyArrangeHybridSwitchesByStagingPressure(t *testing.T) {
	p := NewSchedulerPolicy(PolicyHybrid)
	withMildStaging := []Priority{
		{Level: 1, Adjusted: 2, StagingMode: StagingNone},
		{Level: 2, Adjusted: 1.5, StagingMode: StagingDrain},
	}
	out := p.Arrange(0, withMildStaging)
	require.Equal(t, 1, out[0].Level)

	noStaging := []Priority{
		{Level: 2, Adjusted: 2, StagingMode: StagingNone},
		{Level: 0, Adjusted: 1.5, StagingMode: StagingNone},
	}
	out = p.Arrange(0, noStaging)
	require.Equal(t, 0, out[0].Level)

	withHeavyStaging := []Priority{
		{Level: 1, Adjusted: 1.2, StagingMode: StagingNone},
		{Level: 2, Adjusted: 4.5, StagingMode: StagingDrain},
		{Level: 3, Adjusted: 3.5, StagingMode: StagingKeep},
	}
	out = p.Arrange(0, withHeavyStaging)
	require.Equal(t, StagingKeep, out[0].StagingMode)
	require.Equal(t, StagingDrain, out[1].StagingMode)
}

func TestSchedulerPolicyTieredFeedbackAdjustsQuota(t *testing.T) {
	baseInput := []Priority{
		{Level: 0, Adjusted: 3.0, StagingMode: StagingNone},
		{Level: 6, Adjusted: 6.0, StagingMode: StagingKeep},
		{Level: 6, Adjusted: 5.9, StagingMode: StagingKeep},
		{Level: 6, Adjusted: 5.8, StagingMode: StagingKeep},
		{Level: 6, Adjusted: 5.7, StagingMode: StagingKeep},
		{Level: 5, Adjusted: 6.5, StagingMode: StagingDrain},
		{Level: 5, Adjusted: 6.4, StagingMode: StagingDrain},
		{Level: 5, Adjusted: 6.3, StagingMode: StagingDrain},
		{Level: 5, Adjusted: 6.2, StagingMode: StagingDrain},
		{Level: 2, Adjusted: 5.5, StagingMode: StagingNone},
		{Level: 2, Adjusted: 5.4, StagingMode: StagingNone},
	}

	normal := NewSchedulerPolicy(PolicyTiered)
	normalOut := normal.Arrange(0, baseInput)
	normalIdx := firstRegularNonL0(normalOut)
	require.Greater(t, normalIdx, 0)

	failed := NewSchedulerPolicy(PolicyTiered)
	for range 3 {
		failed.Observe(FeedbackEvent{
			Priority: Priority{StagingMode: StagingDrain},
			Err:      errors.New("injected staging failure"),
		})
	}
	failedOut := failed.Arrange(0, baseInput)
	failedIdx := firstRegularNonL0(failedOut)
	require.Less(t, failedIdx, normalIdx, "staging failures should shift quota toward regular progress")

	success := NewSchedulerPolicy(PolicyTiered)
	for range 3 {
		success.Observe(FeedbackEvent{
			Priority: Priority{StagingMode: StagingKeep},
			Err:      nil,
		})
	}
	successOut := success.Arrange(0, baseInput)
	successIdx := firstRegularNonL0(successOut)
	require.Greater(t, successIdx, normalIdx, "staging successes should increase staging scheduling share")
}

func firstRegularNonL0(prios []Priority) int {
	for i, p := range prios {
		if p.StagingMode == StagingNone && p.Level != 0 {
			return i
		}
	}
	return -1
}

func tableRefSnapshot(tables []*table) map[*table]int32 {
	out := make(map[*table]int32, len(tables))
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		out[tbl] = tbl.Load()
	}
	return out
}

func requireDecrOnce(t *testing.T, before map[*table]int32) {
	t.Helper()
	for tbl, ref := range before {
		after := tbl.Load()
		if after != ref-1 {
			t.Fatalf("table %d ref mismatch: before=%d after=%d expected=%d", tbl.fid, ref, after, ref-1)
		}
		if after < 0 {
			t.Fatalf("table %d ref underflow: after=%d", tbl.fid, after)
		}
	}
}

func hasStagingTable(lh *levelHandler, fid uint64) bool {
	lh.RLock()
	defer lh.RUnlock()
	for _, sh := range lh.staging.shards {
		for _, tbl := range sh.tables {
			if tbl != nil && tbl.fid == fid {
				return true
			}
		}
	}
	return false
}

func TestRunCompactDefStagingNoneDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	cd := buildCompactDef(lsm, 0, 0, 1)
	tricky(cd.thisLevel.tablesSnapshot())
	if ok := lsm.levels.fillTables(cd); !ok {
		t.Fatalf("fillTables failed for staging-none path")
	}
	if cd.plan.StagingMode != StagingNone {
		t.Fatalf("expected staging-none plan, got %v", cd.plan.StagingMode)
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 0, *cd); err != nil {
		t.Fatalf("runCompactDef staging-none: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
}

func TestRunCompactDefStagingDrainDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	l0 := lsm.levels.levels[0]
	l0Tables := l0.tablesSnapshot()
	if len(l0Tables) == 0 {
		t.Fatalf("expected L0 tables before moveToStaging")
	}

	move := buildCompactDef(lsm, 0, 0, 6)
	move.top = []*table{l0Tables[0]}
	move.plan.ThisRange = getKeyRange(move.top...)
	move.plan.NextRange = move.plan.ThisRange
	if move.nextLevel == nil {
		move.nextLevel = lsm.levels.levels[6]
	}
	if err := lsm.levels.moveToStaging(move); err != nil {
		t.Fatalf("moveToStaging: %v", err)
	}

	target := lsm.levels.levels[6]
	if target.numStagingTables() == 0 {
		t.Fatalf("expected staging tables after moveToStaging")
	}

	cd := buildCompactDef(lsm, 0, 6, 6)
	cd.plan.StagingMode = StagingDrain
	cd.plan.StatsTag = "test-staging-drain"
	if ok := lsm.levels.fillTablesStagingShard(cd, -1); !ok {
		t.Fatalf("fillTablesStagingShard failed for staging-drain path")
	}
	if len(cd.top) == 0 {
		t.Fatalf("expected staging top tables for drain compaction")
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 6, *cd); err != nil {
		t.Fatalf("runCompactDef staging-drain: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
	for tbl := range before {
		if hasStagingTable(target, tbl.fid) {
			t.Fatalf("drained table %d still present in staging buffer", tbl.fid)
		}
	}
}

func TestRunCompactDefStagingKeepDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	l0Tables := lsm.levels.levels[0].tablesSnapshot()
	if len(l0Tables) == 0 {
		t.Fatalf("expected L0 tables before moveToStaging")
	}

	move := buildCompactDef(lsm, 0, 0, 6)
	move.top = []*table{l0Tables[0]}
	move.plan.ThisRange = getKeyRange(move.top...)
	move.plan.NextRange = move.plan.ThisRange
	if move.nextLevel == nil {
		move.nextLevel = lsm.levels.levels[6]
	}
	if err := lsm.levels.moveToStaging(move); err != nil {
		t.Fatalf("moveToStaging: %v", err)
	}

	target := lsm.levels.levels[6]
	if target.numStagingTables() == 0 {
		t.Fatalf("expected staging tables after moveToStaging")
	}

	cd := buildCompactDef(lsm, 0, 6, 6)
	cd.plan.StagingMode = StagingKeep
	cd.plan.StatsTag = "test-staging-keep"
	if ok := lsm.levels.fillTablesStagingShard(cd, -1); !ok {
		t.Fatalf("fillTablesStagingShard failed for staging-keep path")
	}
	if len(cd.top) == 0 {
		t.Fatalf("expected staging top tables for keep compaction")
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 6, *cd); err != nil {
		t.Fatalf("runCompactDef staging-keep: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
	if target.numStagingTables() == 0 {
		t.Fatalf("expected staging tables to remain after staging-keep compaction")
	}
	for tbl := range before {
		if hasStagingTable(target, tbl.fid) {
			t.Fatalf("replaced table %d still present in staging buffer", tbl.fid)
		}
	}
}

// TestStateIntraLevelEntryDeletesCleanly verifies the state machine round
// trip for an IntraLevel entry: CompareAndAdd succeeds, Delete undoes the
// table claims and does NOT panic on missing range bookkeeping. IntraLevel
// is the marker used by L0→L0 compactions to claim by table id only.
func TestStateIntraLevelEntryDeletesCleanly(t *testing.T) {
	state := NewState(8)
	entry := StateEntry{
		ThisLevel:  0,
		NextLevel:  0,
		TableIDs:   []uint64{1, 2, 3, 4},
		IntraLevel: true,
	}
	require.True(t, state.CompareAndAdd(LevelsLocked{}, entry))
	require.True(t, state.HasTable(1))
	require.True(t, state.HasTable(4))
	require.False(t, state.HasTable(99))
	require.NoError(t, state.Delete(entry))
	require.False(t, state.HasTable(1))
	require.False(t, state.HasTable(4))
}
