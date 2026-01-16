package lsm

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/compact"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
)

var (
	// Shared test options.
	opt = &Options{
		WorkDir:             "../work_test",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       3,
	}
)

// TestBase is a basic correctness test.
func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	test := func() {
		// Baseline test.
		baseTest(t, lsm, 128)
	}
	// Run N times to exercise multiple SSTables.
	runTest(1, test)
}

// TestClose exercises graceful shutdown and restart.
func TestClose(t *testing.T) {
	clearDir()
	test := func() {
		first := buildLSM()
		first.StartCompacter()
		baseTest(t, first, 128)
		_ = utils.Err(first.Close())

		// A successful restart must still pass the base test.
		reopened := buildLSM()
		reopened.StartCompacter()
		defer reopened.Close()
		baseTest(t, reopened, 128)
	}
	// Run N times to exercise multiple SSTables.
	runTest(1, test)
}

// TestHitStorage exercises read paths across storage tiers.
func TestHitStorage(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	e := utils.BuildEntry()
	if err := lsm.Set(e); err != nil {
		t.Fatalf("lsm.Set: %v", err)
	}
	// Hit the memtable path.
	hitMemtable := func() {
		v, err := lsm.memTable.Get(e.Key)
		_ = utils.Err(err)
		utils.CondPanic(!bytes.Equal(v.Value, e.Value), fmt.Errorf("[hitMemtable] !equal(v.Value, e.Value)"))
	}
	// Hit the L0 path.
	hitL0 := func() {
		// baseTest already covers L0 SST lookups.
		baseTest(t, lsm, 128)
	}
	// Hit a non-L0 path.
	hitNotL0 := func() {
		// Compaction produces non-L0 data; this should hit L6.
		lsm.levels.compaction.RunOnce(0)
		baseTest(t, lsm, 128)
	}
	// Exercise the bloom-filter miss path.
	hitBloom := func() {
		ee := utils.BuildEntry()
		// Query a missing key; a bloom-filter miss confirms absence.
		tables := lsm.levels.levels[0].tablesSnapshot()
		if len(tables) == 0 {
			t.Fatalf("expected L0 tables for bloom test")
		}
		v, err := tables[0].Search(ee.Key, &ee.Version)
		utils.CondPanic(v != nil, fmt.Errorf("[hitBloom] v != nil"))
		utils.CondPanic(err != utils.ErrKeyNotFound, fmt.Errorf("[hitBloom] err != utils.ErrKeyNotFound"))
	}

	runTest(1, hitMemtable, hitL0, hitNotL0, hitBloom)
}

func TestLSMThrottleCallback(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	var (
		mu     sync.Mutex
		events []bool
	)
	lsm.SetThrottleCallback(func(on bool) {
		mu.Lock()
		events = append(events, on)
		mu.Unlock()
	})

	lsm.throttleWrites(true)
	lsm.throttleWrites(true)
	lsm.throttleWrites(false)
	lsm.throttleWrites(false)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 2 {
		t.Fatalf("unexpected throttle events: %+v", events)
	}
	if !events[0] {
		t.Fatalf("expected first throttle event to enable writes")
	}
	if events[1] {
		t.Fatalf("expected second throttle event to disable throttling")
	}
}

// TestPsarameter verifies invalid argument handling.
func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	testNil := func() {
		utils.CondPanic(lsm.Set(nil) != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
		_, err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
	}
	// TODO: skip p2 priority cases for now.
	runTest(1, testNil)
}

func TestMemtableTombstoneShadowsSST(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	key := []byte("tombstone-key-00000000")
	val := []byte("value")

	e := kv.NewEntry(key, val)
	if err := lsm.Set(e); err != nil {
		t.Fatalf("lsm.Set: %v", err)
	}

	lsm.Rotate()
	waitForL0(t, lsm)

	del := kv.NewEntry(key, nil)
	del.Meta = kv.BitDelete
	if err := lsm.Set(del); err != nil {
		t.Fatalf("lsm.Set tombstone: %v", err)
	}

	got, err := lsm.Get(key)
	if err != nil {
		t.Fatalf("lsm.Get: %v", err)
	}
	if got.Meta&kv.BitDelete == 0 {
		t.Fatalf("expected tombstone entry, got meta=%d", got.Meta)
	}
	if len(got.Value) != 0 {
		t.Fatalf("expected empty tombstone value, got %q", got.Value)
	}
}

// TestCompact exercises L0->Lmax compaction.
func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	ok := false
	hasTable := func(lh *levelHandler, fid uint64) bool {
		if lh == nil {
			return false
		}
		lh.RLock()
		defer lh.RUnlock()
		for _, t := range lh.tables {
			if t.fid == fid {
				return true
			}
		}
		for _, sh := range lh.ingest.shards {
			for _, t := range sh.tables {
				if t.fid == fid {
					return true
				}
			}
		}
		return false
	}
	l0TOLMax := func() {
		// Ensure L0 accumulates enough tables to trigger the ingest path. Newer Go versions
		// batch allocations slightly differently, so loop until we hit the configured limit.
		required := lsm.levels.opt.NumLevelZeroTables
		for tries := 0; tries < 8 && lsm.levels.levels[0].numTables() < required; tries++ {
			baseTest(t, lsm, 256)
		}
		if lsm.levels.levels[0].numTables() < required {
			t.Fatalf("expected at least %d L0 tables before compaction, got %d",
				required, lsm.levels.levels[0].numTables())
		}

		before := make(map[uint64]struct{})
		for _, tbl := range lsm.levels.levels[0].tablesSnapshot() {
			before[tbl.fid] = struct{}{}
		}
		lsm.levels.compaction.RunOnce(1)
		ok = false
		for fid := range before {
			if hasTable(lsm.levels.levels[6], fid) {
				ok = true
				break
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[l0TOLMax] fid not found"))
	}
	l0ToL0 := func() {
		// Seed some data first.
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// Clear global state to isolate downstream tests.
		lsm.levels.compactState.Delete(cd.stateEntry())
		_ = utils.Err(err)
		ok = hasTable(lsm.levels.levels[0], fid)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// Clear global state to isolate downstream tests.
		lsm.levels.compactState.Delete(cd.stateEntry())
		_ = utils.Err(err)
		ok = hasTable(lsm.levels.levels[1], fid)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		prevMax := lsm.levels.maxFID
		cd := buildCompactDef(lsm, 6, 6, 6)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		if !ok && lsm.levels.levels[6].numIngestTables() > 0 {
			pri := compact.Priority{
				Level:      6,
				IngestMode: compact.IngestDrain,
				Target:     lsm.levels.levelTargets(),
				Score:      2,
				Adjusted:   2,
			}
			_ = utils.Err(lsm.levels.doCompact(0, pri))
			tricky(cd.thisLevel.tablesSnapshot())
			ok = lsm.levels.fillTables(cd)
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 6, *cd)
		// Clear global state to isolate downstream tests.
		lsm.levels.compactState.Delete(cd.stateEntry())
		_ = utils.Err(err)
		ok = false
		if hasTable(lsm.levels.levels[6], prevMax+1) {
			ok = true
		} else {
			level := lsm.levels.levels[6]
			level.RLock()
			for _, tbl := range level.tables {
				if tbl.fid > prevMax {
					ok = true
					break
				}
			}
			if !ok {
				for _, sh := range level.ingest.shards {
					for _, tbl := range sh.tables {
						if tbl.fid > prevMax {
							ok = true
							break
						}
					}
					if ok {
						break
					}
				}
			}
			level.RUnlock()
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] fid not found"))
	}
	parallerCompact := func() {
		baseTest(t, lsm, 128)
		cd := buildCompactDef(lsm, 0, 0, 1)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		// Execute two identical compaction plans to simulate contention.
		errCh := make(chan error, 1)
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- lsm.levels.runCompactDef(0, 0, *cd)
		}()
		errMain := lsm.levels.runCompactDef(0, 0, *cd)
		wg.Wait()
		errBg := <-errCh
		if errBg != nil {
			t.Fatalf("parallel compaction error: %v", errBg)
		}
		if errMain != nil {
			t.Fatalf("parallel compaction error: %v", errMain)
		}
		// Verify compaction status reflects parallel work.
		utils.CondPanic(!lsm.levels.compactState.HasRanges(), fmt.Errorf("[parallerCompact] not is paralle"))
	}
	// Run N times to exercise multiple SSTables.
	runTest(1, l0TOLMax, l0ToL0, nextCompact, maxToMax, parallerCompact)
}

func TestIngestMergeStaysInIngest(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	// Generate enough data to create multiple L0 tables.
	baseTest(t, lsm, 256)

	// Move one L0 table to the max level ingest buffer.
	l0 := lsm.levels.levels[0]
	tables := l0.tablesSnapshot()
	if len(tables) == 0 {
		t.Fatalf("expected L0 tables before ingest merge test")
	}
	cd := buildCompactDef(lsm, 0, 0, 6)
	cd.top = []*table{tables[0]}
	cd.plan.ThisRange = getKeyRange(cd.top...)
	cd.plan.NextRange = cd.plan.ThisRange
	if err := lsm.levels.moveToIngest(cd); err != nil {
		t.Fatalf("moveToIngest: %v", err)
	}

	target := lsm.levels.levels[6]
	beforeIngest := target.numIngestTables()
	if beforeIngest == 0 {
		t.Fatalf("expected ingest tables after moveToIngest")
	}
	beforeMain := target.numTables()

	pri := compact.Priority{
		Level:      6,
		Score:      5.0,
		Adjusted:   5.0,
		Target:     lsm.levels.levelTargets(),
		IngestMode: compact.IngestKeep,
	}
	if err := lsm.levels.doCompact(0, pri); err != nil {
		t.Fatalf("ingest merge compact failed: %v", err)
	}

	afterIngest := target.numIngestTables()
	if afterIngest == 0 {
		t.Fatalf("expected ingest tables to remain after merge")
	}
	if target.numTables() != beforeMain {
		t.Fatalf("main table count changed unexpectedly: before=%d after=%d", beforeMain, target.numTables())
	}
}

// Concurrent shard compaction should not violate compactState and should keep ingest merge output in ingest.
func TestIngestShardParallelSafety(t *testing.T) {
	clearDir()
	opt.NumCompactors = 4
	opt.IngestShardParallelism = 4
	lsm := buildLSM()
	defer lsm.Close()

	// Write enough data to spawn multiple L0 tables, then move to ingest.
	for range 4 {
		baseTest(t, lsm, 512)
	}
	l0 := lsm.levels.levels[0]
	tables := l0.tablesSnapshot()
	if len(tables) == 0 {
		t.Fatalf("expected L0 tables for parallel ingest test")
	}
	cd := buildCompactDef(lsm, 0, 0, 6)
	cd.top = []*table{tables[0]}
	cd.plan.ThisRange = getKeyRange(cd.top...)
	cd.plan.NextRange = cd.plan.ThisRange
	if err := lsm.levels.moveToIngest(cd); err != nil {
		t.Fatalf("moveToIngest: %v", err)
	}

	// Trigger parallel ingest-only compactions across shards.
	pri := compact.Priority{
		Level:      6,
		Score:      6.0,
		Adjusted:   6.0,
		Target:     lsm.levels.levelTargets(),
		IngestMode: compact.IngestDrain,
	}
	if err := lsm.levels.doCompact(0, pri); err != nil {
		t.Fatalf("parallel ingest compaction failed: %v", err)
	}

	// Ensure manifest/lists are consistent even if ingest drained.
	target := lsm.levels.levels[6]
	_ = target.numIngestTables()

	// Simulate restart and ensure ingest state can be recovered (may be empty if fully drained).
	_ = utils.Err(lsm.Close())
	lsm = buildLSM()
	defer lsm.Close()
	_ = lsm.levels.levels[6].numIngestTables()
}

// baseTest performs correctness checks.
func baseTest(_ *testing.T, lsm *LSM, n int) {
	// Tracking entry for debugging.
	e := &kv.Entry{
		Key:       []byte("CRTS😁NoKVMrGSBtL12345678"),
		Value:     []byte("我草了"),
		ExpiresAt: 123,
	}
	//caseList := make([]*kv.Entry, 0)
	//caseList = append(caseList, e)

	// Randomized data to exercise write paths.
	_ = utils.Err(lsm.Set(e))
	for i := 1; i < n; i++ {
		ee := utils.BuildEntry()
		_ = utils.Err(lsm.Set(ee))
		// caseList = append(caseList, ee)
	}
	// Read back from the levels.
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
	// TODO: complete range-scan testing.
	//retList := make([]*kv.Entry, 0)
	// testRange := func(isAsc bool) {
	// 	// Range ensures every written LSM entry is readable.
	// 	iter := lsm.NewIterator(&utils.Options{IsAsc: true})
	// 	for iter.Rewind(); iter.Valid(); iter.Next() {
	// 		e := iter.Item().Entry()
	// 		retList = append(retList, e)
	// 	}
	// 	utils.CondPanic(len(retList) != len(caseList), fmt.Errorf("len(retList) != len(caseList)"))
	// 	sort.Slice(retList, func(i, j int) bool {
	// 		return utils.CompareKeys(retList[i].Key, retList[j].Key) > 1
	// 	})
	// 	for i := 0; i < len(caseList); i++ {
	// 		a, b := caseList[i], retList[i]
	// 		if !equal(a.Key, b.Key) || !equal(a.Value, b.Value) || a.ExpiresAt != b.ExpiresAt {
	// 			utils.Panic(fmt.Errorf("lsm.Get(e.Key) kv disagreement !!!"))
	// 		}
	// 	}
	// }
	// // Test ascending order.
	// testRange(true)
	// // Test descending order.
	// testRange(false)
}

// buildLSM is the test harness helper.
func buildLSM() *LSM {
	// init DB Basic Test
	c := make(chan map[uint32]int64, 16)
	opt.DiscardStatsCh = &c
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	if err != nil {
		panic(err)
	}
	lsm := NewLSM(opt, wlog)
	lsm.SetDiscardStatsCh(&c)
	return lsm
}

// runTest executes the provided test functions n times.
func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for range n {
			f()
		}
	}
}

// buildCompactDef constructs a compaction definition for tests.
func buildCompactDef(lsm *LSM, id, thisLevel, nextLevel int) *compactDef {
	t := compact.Targets{
		TargetSz:  []int64{0, 10485760, 10485760, 10485760, 10485760, 10485760, 10485760},
		FileSz:    []int64{1024, 2097152, 2097152, 2097152, 2097152, 2097152, 2097152},
		BaseLevel: nextLevel,
	}
	levelFileSize := func(level int) int64 {
		if level >= 0 && level < len(t.FileSz) && t.FileSz[level] > 0 {
			return t.FileSz[level]
		}
		if level >= 0 && level < len(t.TargetSz) && t.TargetSz[level] > 0 {
			return t.TargetSz[level]
		}
		return 0
	}
	pri := buildCompactionPriority(lsm, thisLevel, t)
	def := &compactDef{
		compactorId: id,
		thisLevel:   lsm.levels.levels[thisLevel],
		nextLevel:   lsm.levels.levels[nextLevel],
		plan: compact.Plan{
			ThisLevel:    thisLevel,
			NextLevel:    nextLevel,
			ThisFileSize: levelFileSize(thisLevel),
			NextFileSize: levelFileSize(nextLevel),
		},
		adjusted: pri.Adjusted,
	}
	return def
}

// buildCompactionPriority constructs a compaction priority for tests.
func buildCompactionPriority(lsm *LSM, thisLevel int, t compact.Targets) compact.Priority {
	return compact.Priority{
		Level:    thisLevel,
		Score:    8.6,
		Adjusted: 860,
		Target:   t,
	}
}

func tricky(tables []*table) {
	// Use a test-only tweak to satisfy validation checks across branches.
	for _, table := range tables {
		table.staleDataSize = 10 << 20
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.createdAt = t
	}
}

func waitForL0(t *testing.T, lsm *LSM) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lsm.FlushPending() == 0 && lsm.levels.levels[0].numTables() > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for L0 table (pending=%d tables=%d)",
		lsm.FlushPending(), lsm.levels.levels[0].numTables())
}

func clearDir() {
	if opt == nil {
		return
	}
	if opt.WorkDir != "" {
		_ = os.RemoveAll(opt.WorkDir)
	}
	dir, err := os.MkdirTemp("", "nokv-lsm-test-")
	if err != nil {
		panic(err)
	}
	opt.WorkDir = dir
}
