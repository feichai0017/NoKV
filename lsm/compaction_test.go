package lsm

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/compact"
	"github.com/feichai0017/NoKV/utils"
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
		splits:    []compact.KeyRange{{}},
		plan: compact.Plan{
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
		it := tbl.NewIterator(&utils.Options{IsAsc: true})
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
			if prev != nil && utils.CompareKeys(prev, cur) > 0 {
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
		out[tbl] = tbl.ref.Load()
	}
	return out
}

func requireDecrOnce(t *testing.T, before map[*table]int32) {
	t.Helper()
	for tbl, ref := range before {
		after := tbl.ref.Load()
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

func TestCompactRangeTracker_CoversWithMonotonicKeys(t *testing.T) {
	tracker := newCompactRangeTracker()

	// Compaction iterates keys in ascending order. Once a range tombstone
	// [b,f)@10 has been seen, subsequent keys >= b should respect it until f.
	tracker.add(RangeTombstone{
		CF:      kv.CFDefault,
		Start:   []byte("b"),
		End:     []byte("f"),
		Version: 10,
	})

	if !tracker.covers(kv.CFDefault, []byte("b"), 1) {
		t.Fatalf("expected key b@1 to be covered")
	}
	if !tracker.covers(kv.CFDefault, []byte("e"), 9) {
		t.Fatalf("expected key e@9 to be covered")
	}
	if tracker.covers(kv.CFDefault, []byte("e"), 10) {
		t.Fatalf("expected key e@10 not covered when versions are equal")
	}
	if tracker.covers(kv.CFDefault, []byte("f"), 1) {
		t.Fatalf("expected key f@1 not covered (end is exclusive)")
	}
}

func TestCompactRangeTracker_OverlapAndCFIsolation(t *testing.T) {
	tracker := newCompactRangeTracker()

	tracker.add(RangeTombstone{
		CF:      kv.CFDefault,
		Start:   []byte("a"),
		End:     []byte("z"),
		Version: 100,
	})
	tracker.add(RangeTombstone{
		CF:      kv.CFDefault,
		Start:   []byte("d"),
		End:     []byte("h"),
		Version: 200,
	})

	if !tracker.covers(kv.CFDefault, []byte("e"), 150) {
		t.Fatalf("expected key e@150 covered by newer overlapping tombstone")
	}
	if tracker.covers(kv.CFDefault, []byte("e"), 200) {
		t.Fatalf("expected key e@200 not covered when versions are equal")
	}
	if !tracker.covers(kv.CFDefault, []byte("y"), 99) {
		t.Fatalf("expected key y@99 covered by wide tombstone")
	}
	if tracker.covers(kv.CFLock, []byte("e"), 1) {
		t.Fatalf("expected key e@1 in lock CF not covered by default CF tombstones")
	}
}
