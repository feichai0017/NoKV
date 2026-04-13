package lsm

import (
	"bytes"
	"errors"
	"testing"

	"github.com/feichai0017/NoKV/index"
	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestCompactionMoveToIngest(t *testing.T) {
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

	beforeIngest := cd.nextLevel.numIngestTables()
	if err := lsm.levels.moveToIngest(cd); err != nil {
		t.Fatalf("moveToIngest: %v", err)
	}
	afterIngest := cd.nextLevel.numIngestTables()
	if afterIngest <= beforeIngest {
		t.Fatalf("expected ingest buffer to grow, before=%d after=%d", beforeIngest, afterIngest)
	}

	// Ensure the moved table has been removed from the source level.
	found := false
	cd.nextLevel.RLock()
	for _, sh := range cd.nextLevel.ingest.shards {
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
		t.Fatalf("table %d not found in ingest buffer", cd.top[0].fid)
	}
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
			if prev != nil && utils.CompareInternalKeys(prev, cur) > 0 {
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

func TestSchedulerPolicyArrangeTieredPrefersIngest(t *testing.T) {
	p := NewSchedulerPolicy(PolicyTiered)
	in := []Priority{
		{Level: 0, Adjusted: 9, IngestMode: IngestNone},
		{Level: 3, Adjusted: 2, IngestMode: IngestKeep},
		{Level: 2, Adjusted: 5, IngestMode: IngestDrain},
		{Level: 1, Adjusted: 8, IngestMode: IngestNone},
	}
	out := p.Arrange(0, in)
	require.Len(t, out, 4)
	require.Equal(t, 0, out[0].Level)
	require.Equal(t, IngestKeep, out[1].IngestMode)
	require.Equal(t, IngestDrain, out[2].IngestMode)
	require.Equal(t, 1, out[3].Level)
}

func TestSchedulerPolicyArrangeHybridSwitchesByIngestPressure(t *testing.T) {
	p := NewSchedulerPolicy(PolicyHybrid)
	withMildIngest := []Priority{
		{Level: 1, Adjusted: 2, IngestMode: IngestNone},
		{Level: 2, Adjusted: 1.5, IngestMode: IngestDrain},
	}
	out := p.Arrange(0, withMildIngest)
	require.Equal(t, 1, out[0].Level)

	noIngest := []Priority{
		{Level: 2, Adjusted: 2, IngestMode: IngestNone},
		{Level: 0, Adjusted: 1.5, IngestMode: IngestNone},
	}
	out = p.Arrange(0, noIngest)
	require.Equal(t, 0, out[0].Level)

	withHeavyIngest := []Priority{
		{Level: 1, Adjusted: 1.2, IngestMode: IngestNone},
		{Level: 2, Adjusted: 4.5, IngestMode: IngestDrain},
		{Level: 3, Adjusted: 3.5, IngestMode: IngestKeep},
	}
	out = p.Arrange(0, withHeavyIngest)
	require.Equal(t, IngestKeep, out[0].IngestMode)
	require.Equal(t, IngestDrain, out[1].IngestMode)
}

func TestSchedulerPolicyTieredFeedbackAdjustsQuota(t *testing.T) {
	baseInput := []Priority{
		{Level: 0, Adjusted: 3.0, IngestMode: IngestNone},
		{Level: 6, Adjusted: 6.0, IngestMode: IngestKeep},
		{Level: 6, Adjusted: 5.9, IngestMode: IngestKeep},
		{Level: 6, Adjusted: 5.8, IngestMode: IngestKeep},
		{Level: 6, Adjusted: 5.7, IngestMode: IngestKeep},
		{Level: 5, Adjusted: 6.5, IngestMode: IngestDrain},
		{Level: 5, Adjusted: 6.4, IngestMode: IngestDrain},
		{Level: 5, Adjusted: 6.3, IngestMode: IngestDrain},
		{Level: 5, Adjusted: 6.2, IngestMode: IngestDrain},
		{Level: 2, Adjusted: 5.5, IngestMode: IngestNone},
		{Level: 2, Adjusted: 5.4, IngestMode: IngestNone},
	}

	normal := NewSchedulerPolicy(PolicyTiered)
	normalOut := normal.Arrange(0, baseInput)
	normalIdx := firstRegularNonL0(normalOut)
	require.Greater(t, normalIdx, 0)

	failed := NewSchedulerPolicy(PolicyTiered)
	for range 3 {
		failed.Observe(FeedbackEvent{
			Priority: Priority{IngestMode: IngestDrain},
			Err:      errors.New("injected ingest failure"),
		})
	}
	failedOut := failed.Arrange(0, baseInput)
	failedIdx := firstRegularNonL0(failedOut)
	require.Less(t, failedIdx, normalIdx, "ingest failures should shift quota toward regular progress")

	success := NewSchedulerPolicy(PolicyTiered)
	for range 3 {
		success.Observe(FeedbackEvent{
			Priority: Priority{IngestMode: IngestKeep},
			Err:      nil,
		})
	}
	successOut := success.Arrange(0, baseInput)
	successIdx := firstRegularNonL0(successOut)
	require.Greater(t, successIdx, normalIdx, "ingest successes should increase ingest scheduling share")
}

func firstRegularNonL0(prios []Priority) int {
	for i, p := range prios {
		if p.IngestMode == IngestNone && p.Level != 0 {
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

func hasIngestTable(lh *levelHandler, fid uint64) bool {
	lh.RLock()
	defer lh.RUnlock()
	for _, sh := range lh.ingest.shards {
		for _, tbl := range sh.tables {
			if tbl != nil && tbl.fid == fid {
				return true
			}
		}
	}
	return false
}

func TestRunCompactDefIngestNoneDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	cd := buildCompactDef(lsm, 0, 0, 1)
	tricky(cd.thisLevel.tablesSnapshot())
	if ok := lsm.levels.fillTables(cd); !ok {
		t.Fatalf("fillTables failed for ingest-none path")
	}
	if cd.plan.IngestMode != IngestNone {
		t.Fatalf("expected ingest-none plan, got %v", cd.plan.IngestMode)
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 0, *cd); err != nil {
		t.Fatalf("runCompactDef ingest-none: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
}

func TestRunCompactDefIngestDrainDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	l0 := lsm.levels.levels[0]
	l0Tables := l0.tablesSnapshot()
	if len(l0Tables) == 0 {
		t.Fatalf("expected L0 tables before moveToIngest")
	}

	move := buildCompactDef(lsm, 0, 0, 6)
	move.top = []*table{l0Tables[0]}
	move.plan.ThisRange = getKeyRange(move.top...)
	move.plan.NextRange = move.plan.ThisRange
	if move.nextLevel == nil {
		move.nextLevel = lsm.levels.levels[6]
	}
	if err := lsm.levels.moveToIngest(move); err != nil {
		t.Fatalf("moveToIngest: %v", err)
	}

	target := lsm.levels.levels[6]
	if target.numIngestTables() == 0 {
		t.Fatalf("expected ingest tables after moveToIngest")
	}

	cd := buildCompactDef(lsm, 0, 6, 6)
	cd.plan.IngestMode = IngestDrain
	cd.plan.StatsTag = "test-ingest-drain"
	if ok := lsm.levels.fillTablesIngestShard(cd, -1); !ok {
		t.Fatalf("fillTablesIngestShard failed for ingest-drain path")
	}
	if len(cd.top) == 0 {
		t.Fatalf("expected ingest top tables for drain compaction")
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 6, *cd); err != nil {
		t.Fatalf("runCompactDef ingest-drain: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
	for tbl := range before {
		if hasIngestTable(target, tbl.fid) {
			t.Fatalf("drained table %d still present in ingest buffer", tbl.fid)
		}
	}
}

func TestRunCompactDefIngestKeepDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	l0Tables := lsm.levels.levels[0].tablesSnapshot()
	if len(l0Tables) == 0 {
		t.Fatalf("expected L0 tables before moveToIngest")
	}

	move := buildCompactDef(lsm, 0, 0, 6)
	move.top = []*table{l0Tables[0]}
	move.plan.ThisRange = getKeyRange(move.top...)
	move.plan.NextRange = move.plan.ThisRange
	if move.nextLevel == nil {
		move.nextLevel = lsm.levels.levels[6]
	}
	if err := lsm.levels.moveToIngest(move); err != nil {
		t.Fatalf("moveToIngest: %v", err)
	}

	target := lsm.levels.levels[6]
	if target.numIngestTables() == 0 {
		t.Fatalf("expected ingest tables after moveToIngest")
	}

	cd := buildCompactDef(lsm, 0, 6, 6)
	cd.plan.IngestMode = IngestKeep
	cd.plan.StatsTag = "test-ingest-keep"
	if ok := lsm.levels.fillTablesIngestShard(cd, -1); !ok {
		t.Fatalf("fillTablesIngestShard failed for ingest-keep path")
	}
	if len(cd.top) == 0 {
		t.Fatalf("expected ingest top tables for keep compaction")
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 6, *cd); err != nil {
		t.Fatalf("runCompactDef ingest-keep: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
	if target.numIngestTables() == 0 {
		t.Fatalf("expected ingest tables to remain after ingest-keep compaction")
	}
	for tbl := range before {
		if hasIngestTable(target, tbl.fid) {
			t.Fatalf("replaced table %d still present in ingest buffer", tbl.fid)
		}
	}
}
