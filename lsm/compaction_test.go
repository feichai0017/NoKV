package lsm

import (
	"sync/atomic"
	"testing"

	"github.com/feichai0017/NoKV/lsm/compact"
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
		plan: compact.Plan{
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
	if !cs.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected first compareAndAdd to succeed")
	}
	if cs.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected overlapping compaction to be rejected")
	}
	cs.Delete(cd.stateEntry())
	if !cs.CompareAndAdd(compact.LevelsLocked{}, cd.stateEntry()) {
		t.Fatalf("expected compareAndAdd to succeed after delete")
	}
}

func tableRefSnapshot(tables []*table) map[*table]int32 {
	out := make(map[*table]int32, len(tables))
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		out[tbl] = atomic.LoadInt32(&tbl.ref)
	}
	return out
}

func requireDecrOnce(t *testing.T, before map[*table]int32) {
	t.Helper()
	for tbl, ref := range before {
		after := atomic.LoadInt32(&tbl.ref)
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
	if cd.plan.IngestMode != compact.IngestNone {
		t.Fatalf("expected ingest-none plan, got %v", cd.plan.IngestMode)
	}
	before := tableRefSnapshot(cd.top)
	if err := lsm.levels.runCompactDef(0, 0, *cd); err != nil {
		t.Fatalf("runCompactDef ingest-none: %v", err)
	}
	lsm.levels.compactState.Delete(cd.stateEntry())
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
	cd.plan.IngestMode = compact.IngestDrain
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
	lsm.levels.compactState.Delete(cd.stateEntry())
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
	cd.plan.IngestMode = compact.IngestKeep
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
	lsm.levels.compactState.Delete(cd.stateEntry())
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
