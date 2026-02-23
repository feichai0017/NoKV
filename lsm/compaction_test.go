package lsm

import (
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
