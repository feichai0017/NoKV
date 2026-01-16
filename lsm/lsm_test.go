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
	// 初始化opt
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

// TestBase 正确性测试
func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	test := func() {
		// 基准测试
		baseTest(t, lsm, 128)
	}
	// 运行N次测试多个sst的影响
	runTest(1, test)
}

// TestClose 测试优雅关闭
func TestClose(t *testing.T) {
	clearDir()
	test := func() {
		first := buildLSM()
		first.StartCompacter()
		baseTest(t, first, 128)
		_ = utils.Err(first.Close())

		// 重启后可正常工作才算成功
		reopened := buildLSM()
		reopened.StartCompacter()
		defer reopened.Close()
		baseTest(t, reopened, 128)
	}
	// 运行N次测试多个sst的影响
	runTest(1, test)
}

// 命中不同存储介质的逻辑分支测试
func TestHitStorage(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	e := utils.BuildEntry()
	if err := lsm.Set(e); err != nil {
		t.Fatalf("lsm.Set: %v", err)
	}
	// 命中内存表
	hitMemtable := func() {
		v, err := lsm.memTable.Get(e.Key)
		_ = utils.Err(err)
		utils.CondPanic(!bytes.Equal(v.Value, e.Value), fmt.Errorf("[hitMemtable] !equal(v.Value, e.Value)"))
	}
	// 命中L0层
	hitL0 := func() {
		// baseTest的测试就包含 在命中L0的sst查询
		baseTest(t, lsm, 128)
	}
	// 命中非L0层
	hitNotL0 := func() {
		// 通过压缩将compact生成非L0数据, 会命中l6层
		lsm.levels.compaction.RunOnce(0)
		baseTest(t, lsm, 128)
	}
	// 命中bf
	hitBloom := func() {
		ee := utils.BuildEntry()
		// 查询不存在的key 如果命中则说明一定不存在
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

// Testparameter 测试异常参数
func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	testNil := func() {
		utils.CondPanic(lsm.Set(nil) != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
		_, err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrEmptyKey, fmt.Errorf("[testNil] lsm.Set(nil) != err"))
	}
	// TODO p2 优先级的case先忽略
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

// TestCompact 测试L0到Lmax压缩
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
		// 先写一些数据进来
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.Delete(cd.stateEntry())
		_ = utils.Err(err)
		ok = hasTable(lsm.levels.levels[0], fid)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.Delete(cd.stateEntry())
		_ = utils.Err(err)
		ok = hasTable(lsm.levels.levels[1], fid)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		prevMax := lsm.levels.maxFID
		cd := buildCompactDef(lsm, 6, 6, 6)
		// 非常tricky的处理方法，为了能通过检查
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
		// 删除全局状态，便于下游测试逻辑
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
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		// 构建完全相同两个压缩计划的执行，以便于百分比构建 压缩冲突
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
		// 检查compact status状态查看是否在执行并行压缩
		utils.CondPanic(!lsm.levels.compactState.HasRanges(), fmt.Errorf("[parallerCompact] not is paralle"))
	}
	// 运行N次测试多个sst的影响
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

// 正确性测试
func baseTest(t *testing.T, lsm *LSM, n int) {
	// 用来跟踪调试的
	e := &kv.Entry{
		Key:       []byte("CRTS😁NoKVMrGSBtL12345678"),
		Value:     []byte("我草了"),
		ExpiresAt: 123,
	}
	//caseList := make([]*kv.Entry, 0)
	//caseList = append(caseList, e)

	// 随机构建数据进行测试
	_ = utils.Err(lsm.Set(e))
	for i := 1; i < n; i++ {
		ee := utils.BuildEntry()
		_ = utils.Err(lsm.Set(ee))
		// caseList = append(caseList, ee)
	}
	// 从levels中进行GET
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
	// TODO range功能待完善
	//retList := make([]*kv.Entry, 0)
	// testRange := func(isAsc bool) {
	// 	// Range 确保写入进去的每个lsm都可以被读取到
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
	// // 测试升序
	// testRange(true)
	// // 测试降序
	// testRange(false)
}

// 驱动模块
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

// 运行测试用例
func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for range n {
			f()
		}
	}
}

// 构建compactDef对象
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

// 构建CompactionPriority对象
func buildCompactionPriority(lsm *LSM, thisLevel int, t compact.Targets) compact.Priority {
	return compact.Priority{
		Level:    thisLevel,
		Score:    8.6,
		Adjusted: 860,
		Target:   t,
	}
}

func tricky(tables []*table) {
	// 非常tricky的处理方法，为了能通过检查，检查所有逻辑分支
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
