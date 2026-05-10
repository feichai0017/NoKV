package lsm

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/stretchr/testify/require"
)

func TestCompactionMoveToLanding(t *testing.T) {
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
	cd.top = []*table.Table{tables[0]}
	cd.spec.ThisRange = getKeyRange(cd.top...)
	cd.spec.NextRange = cd.spec.ThisRange
	if cd.nextLevel == nil {
		cd.nextLevel = lsm.levels.levels[1]
	}

	beforeLanding := cd.nextLevel.numLandingTables()
	if err := lsm.levels.compactor.moveToLanding(cd); err != nil {
		t.Fatalf("moveToLanding: %v", err)
	}
	afterLanding := cd.nextLevel.numLandingTables()
	if afterLanding <= beforeLanding {
		t.Fatalf("expected landing buffer to grow, before=%d after=%d", beforeLanding, afterLanding)
	}

	// Ensure the moved table has been removed from the source level.
	found := false
	cd.nextLevel.RLock()
	for _, tbl := range cd.nextLevel.landing.AllTables() {
		if tbl != nil && tbl.FID() == cd.top[0].FID() {
			found = true
			break
		}
	}
	cd.nextLevel.RUnlock()
	if !found {
		t.Fatalf("table %d not found in landing buffer", cd.top[0].FID())
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
	cd.top = []*table.Table{tbl}
	cd.spec.TopIDs = []uint64{tbl.FID()}
	cd.spec.ThisRange = getKeyRange(tbl)
	cd.spec.NextRange = cd.spec.ThisRange
	cd.thisSize = tbl.Size()

	beforeRef := tbl.Load()
	require.True(t, lsm.levels.compactor.canMoveToNextLevel(cd))
	require.NoError(t, lsm.levels.compactor.moveToNextLevel(cd))
	require.Equal(t, beforeRef, tbl.Load())
	require.Equal(t, 2, tbl.Level())

	require.Empty(t, src.tablesSnapshot())
	got := dst.tablesSnapshot()
	require.Len(t, got, 1)
	require.Equal(t, tbl.FID(), got[0].FID())

	version := lsm.levels.manifestMgr.Current()
	require.Empty(t, version.Levels[1])
	require.Len(t, version.Levels[2], 1)
	require.Equal(t, tbl.FID(), version.Levels[2][0].FileID)
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
		top:       []*table.Table{top},
		bot:       []*table.Table{botA, botB},
		splits:    []plan.KeyRange{{}},
		spec: plan.Plan{
			NextFileSize: 1 << 20,
		},
	}

	newTables, decr, err := lsm.levels.compactor.compactBuildTables(5, cd)
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
		top:       []*table.Table{tbl},
		spec: plan.Plan{
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
	if !cs.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected first compareAndAdd to succeed")
	}
	if cs.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected overlapping compaction to be rejected")
	}
	require.Nil(t, cs.Delete(cd.stateEntry()))
	if !cs.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected compareAndAdd to succeed after delete")
	}
}

func TestRunCompactDefLandingDrainDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	l0 := lsm.levels.levels[0]
	l0Tables := l0.tablesSnapshot()
	if len(l0Tables) == 0 {
		t.Fatalf("expected L0 tables before moveToLanding")
	}

	move := buildCompactDef(lsm, 0, 0, 6)
	move.top = []*table.Table{l0Tables[0]}
	move.spec.ThisRange = getKeyRange(move.top...)
	move.spec.NextRange = move.spec.ThisRange
	if move.nextLevel == nil {
		move.nextLevel = lsm.levels.levels[6]
	}
	if err := lsm.levels.compactor.moveToLanding(move); err != nil {
		t.Fatalf("moveToLanding: %v", err)
	}

	target := lsm.levels.levels[6]
	if target.numLandingTables() == 0 {
		t.Fatalf("expected landing tables after moveToLanding")
	}

	cd := buildCompactDef(lsm, 0, 6, 6)
	cd.spec.LandingMode = plan.LandingDrain
	cd.spec.StatsTag = "test-landing-drain"
	if ok := lsm.levels.compactor.fillTablesLandingShard(cd, -1); !ok {
		t.Fatalf("fillTablesLandingShard failed for landing-drain path")
	}
	if len(cd.top) == 0 {
		t.Fatalf("expected landing top tables for drain compaction")
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.compactor.runCompactDef(0, 6, *cd); err != nil {
		t.Fatalf("runCompactDef landing-drain: %v", err)
	}
	require.Nil(t, lsm.levels.compactor.state.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
	for tbl := range before {
		if hasLandingTable(target, tbl.FID()) {
			t.Fatalf("drained table %d still present in landing buffer", tbl.FID())
		}
	}
}

func TestRunCompactDefLandingKeepDecrementsTopOnce(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	baseTest(t, lsm, 256)
	waitForL0(t, lsm)

	l0Tables := lsm.levels.levels[0].tablesSnapshot()
	if len(l0Tables) == 0 {
		t.Fatalf("expected L0 tables before moveToLanding")
	}

	move := buildCompactDef(lsm, 0, 0, 6)
	move.top = []*table.Table{l0Tables[0]}
	move.spec.ThisRange = getKeyRange(move.top...)
	move.spec.NextRange = move.spec.ThisRange
	if move.nextLevel == nil {
		move.nextLevel = lsm.levels.levels[6]
	}
	if err := lsm.levels.compactor.moveToLanding(move); err != nil {
		t.Fatalf("moveToLanding: %v", err)
	}

	target := lsm.levels.levels[6]
	if target.numLandingTables() == 0 {
		t.Fatalf("expected landing tables after moveToLanding")
	}

	cd := buildCompactDef(lsm, 0, 6, 6)
	cd.spec.LandingMode = plan.LandingKeep
	cd.spec.StatsTag = "test-landing-keep"
	if ok := lsm.levels.compactor.fillTablesLandingShard(cd, -1); !ok {
		t.Fatalf("fillTablesLandingShard failed for landing-keep path")
	}
	if len(cd.top) == 0 {
		t.Fatalf("expected landing top tables for keep compaction")
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.compactor.runCompactDef(0, 6, *cd); err != nil {
		t.Fatalf("runCompactDef landing-keep: %v", err)
	}
	require.Nil(t, lsm.levels.compactor.state.Delete(cd.stateEntry()))
	requireDecrOnce(t, before)
	if target.numLandingTables() == 0 {
		t.Fatalf("expected landing tables to remain after landing-keep compaction")
	}
	for tbl := range before {
		if hasLandingTable(target, tbl.FID()) {
			t.Fatalf("replaced table %d still present in landing buffer", tbl.FID())
		}
	}
}

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

func TestCompactorPacerBypassesWhenL0IsNearStall(t *testing.T) {
	lm := &levelManager{
		opt: &Options{
			CompactionWriteBytesPerSec: 100,
			CompactionPacingBypassL0:   2,
		},
		levels: []*levelHandler{
			{tables: []*table.Table{{}, {}}},
		},
	}
	lm.compactor = &compactor{lm: lm, pacer: pacer.New(100)}

	require.True(t, lm.compactor.compactionPacerBypassActive())
	require.Nil(t, lm.compactor.compactionPacerForBuild())
}
