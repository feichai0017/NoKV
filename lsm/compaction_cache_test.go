package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/utils"
)

func TestCompactionMoveToIngest(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

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
	cd.thisRange = getKeyRange(cd.top...)
	cd.nextRange = cd.thisRange
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

func TestCacheHotColdMetrics(t *testing.T) {
	opt := &Options{
		BlockCacheSize: 4,
		BloomCacheSize: 4,
	}
	cache := newCache(opt)
	if cache == nil {
		t.Fatalf("expected cache to initialize")
	}

	tbl := &table{}
	blk := &block{tbl: tbl}
	cache.addBlock(0, tbl, 1, blk, true)
	cache.blocks.rc.Wait()
	if v, ok := cache.getBlock(0, 1, true); !ok || v == nil {
		t.Fatalf("expected hot block hit")
	}
	// Miss on different key.
	cache.getBlock(0, 2, false)

	cache.addBlock(1, tbl, 42, &block{tbl: tbl}, true)
	if v, ok := cache.getBlock(1, 42, true); !ok || v == nil {
		t.Fatalf("expected L1 block hit")
	}

	filter := utils.NewFilter([]uint32{utils.Hash([]byte("foo"))}, 10)
	cache.addBloom(7, filter)
	if _, ok := cache.getBloom(7); !ok {
		t.Fatalf("expected bloom hit")
	}
	cache.getBloom(8) // miss

	metrics := cache.metricsSnapshot()
	if metrics.L0Hits != 1 || metrics.L0Misses != 1 {
		t.Fatalf("unexpected L0 metrics: %+v", metrics)
	}
	if metrics.L1Hits != 1 {
		t.Fatalf("unexpected L1 metrics: %+v", metrics)
	}
	if metrics.BloomHits != 1 || metrics.BloomMisses != 1 {
		t.Fatalf("unexpected bloom metrics: %+v", metrics)
	}
}

func TestCompactStatusGuards(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

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
		thisRange: getKeyRange(tbl),
		nextRange: getKeyRange(tbl),
		thisSize:  tbl.Size(),
	}
	cs := lsm.newCompactStatus()
	if !cs.compareAndAdd(thisAndNextLevelRLocked{}, cd) {
		t.Fatalf("expected first compareAndAdd to succeed")
	}
	if cs.compareAndAdd(thisAndNextLevelRLocked{}, cd) {
		t.Fatalf("expected overlapping compaction to be rejected")
	}
	cs.delete(cd)
	if !cs.compareAndAdd(thisAndNextLevelRLocked{}, cd) {
		t.Fatalf("expected compareAndAdd to succeed after delete")
	}
}
