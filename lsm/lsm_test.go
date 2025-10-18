package lsm

import (
	"bytes"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

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
		utils.Err(first.Close())

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
	lsm.Set(e)
	// 命中内存表
	hitMemtable := func() {
		v, err := lsm.memTable.Get(e.Key)
		utils.Err(err)
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
		lsm.levels.compaction.runOnce(0)
		baseTest(t, lsm, 128)
	}
	// 命中bf
	hitBloom := func() {
		ee := utils.BuildEntry()
		// 查询不存在的key 如果命中则说明一定不存在
		v, err := lsm.levels.levels[0].tables[0].Search(ee.Key, &ee.Version)
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

// TestCompact 测试L0到Lmax压缩
func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()
	ok := false
	hasTable := func(lh *levelHandler, fid uint64) bool {
		for _, t := range lh.tables {
			if t.fid == fid {
				return true
			}
		}
		for _, t := range lh.ingest {
			if t.fid == fid {
				return true
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
		for _, tbl := range lsm.levels.levels[0].tables {
			before[tbl.fid] = struct{}{}
		}
		lsm.levels.compaction.runOnce(1)
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
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = hasTable(lsm.levels.levels[0], fid)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = hasTable(lsm.levels.levels[1], fid)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		prevMax := lsm.levels.maxFID
		cd := buildCompactDef(lsm, 6, 6, 6)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		if !ok && lsm.levels.levels[6].numIngestTables() > 0 {
			pri := compactionPriority{
				level:      6,
				ingestOnly: true,
				t:          lsm.levels.levelTargets(),
				score:      2,
				adjusted:   2,
			}
			utils.Err(lsm.levels.doCompact(0, pri))
			tricky(cd.thisLevel.tables)
			ok = lsm.levels.fillTables(cd)
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 6, *cd)
		// 删除全局状态，便于下游测试逻辑
		lsm.levels.compactState.delete(*cd)
		utils.Err(err)
		ok = false
		if hasTable(lsm.levels.levels[6], prevMax+1) {
			ok = true
		} else {
			for _, tbl := range lsm.levels.levels[6].tables {
				if tbl.fid > prevMax {
					ok = true
					break
				}
			}
			if !ok {
				for _, tbl := range lsm.levels.levels[6].ingest {
					if tbl.fid > prevMax {
						ok = true
						break
					}
				}
			}
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] fid not found"))
	}
	parallerCompact := func() {
		baseTest(t, lsm, 128)
		cd := buildCompactDef(lsm, 0, 0, 1)
		// 非常tricky的处理方法，为了能通过检查
		tricky(cd.thisLevel.tables)
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[parallerCompact] lsm.levels.fillTables(cd) ret == false"))
		// 构建完全相同两个压缩计划的执行，以便于百分比构建 压缩冲突
		go lsm.levels.runCompactDef(0, 0, *cd)
		lsm.levels.runCompactDef(0, 0, *cd)
		// 检查compact status状态查看是否在执行并行压缩
		isParaller := false
		for _, state := range lsm.levels.compactState.levels {
			if len(state.ranges) != 0 {
				isParaller = true
			}
		}
		utils.CondPanic(!isParaller, fmt.Errorf("[parallerCompact] not is paralle"))
	}
	// 运行N次测试多个sst的影响
	runTest(1, l0TOLMax, l0ToL0, nextCompact, maxToMax, parallerCompact)
}

// 正确性测试
func baseTest(t *testing.T, lsm *LSM, n int) {
	// 用来跟踪调试的
	e := &utils.Entry{
		Key:       []byte("CRTS😁NoKVMrGSBtL12345678"),
		Value:     []byte("我草了"),
		ExpiresAt: 123,
	}
	//caseList := make([]*utils.Entry, 0)
	//caseList = append(caseList, e)

	// 随机构建数据进行测试
	utils.Err(lsm.Set(e))
	for i := 1; i < n; i++ {
		ee := utils.BuildEntry()
		utils.Err(lsm.Set(ee))
		// caseList = append(caseList, ee)
	}
	// 从levels中进行GET
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
	// TODO range功能待完善
	//retList := make([]*utils.Entry, 0)
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
	t := targets{
		targetSz:  []int64{0, 10485760, 10485760, 10485760, 10485760, 10485760, 10485760},
		fileSz:    []int64{1024, 2097152, 2097152, 2097152, 2097152, 2097152, 2097152},
		baseLevel: nextLevel,
	}
	def := &compactDef{
		compactorId: id,
		thisLevel:   lsm.levels.levels[thisLevel],
		nextLevel:   lsm.levels.levels[nextLevel],
		t:           t,
		p:           buildCompactionPriority(lsm, thisLevel, t),
	}
	return def
}

// 构建CompactionPriority对象
func buildCompactionPriority(lsm *LSM, thisLevel int, t targets) compactionPriority {
	return compactionPriority{
		level:    thisLevel,
		score:    8.6,
		adjusted: 860,
		t:        t,
	}
}

func tricky(tables []*table) {
	// 非常tricky的处理方法，为了能通过检查，检查所有逻辑分支
	for _, table := range tables {
		table.ss.Indexs().StaleDataSize = 10 << 20
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.ss.SetCreatedAt(&t)
	}
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
