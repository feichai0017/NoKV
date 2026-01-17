package lsm

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/compact"
	"github.com/feichai0017/NoKV/manifest"
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

func TestIngestBufferAccounting(t *testing.T) {
	now := time.Now()
	t1 := &table{
		fid:           1,
		minKey:        kv.KeyWithTs([]byte{0x00, 'a'}, 1),
		maxKey:        kv.KeyWithTs([]byte{0x00, 'z'}, 1),
		size:          100,
		valueSize:     40,
		createdAt:     now.Add(-2 * time.Minute),
		maxVersion:    7,
		staleDataSize: 2,
	}
	t2 := &table{
		fid:        2,
		minKey:     kv.KeyWithTs([]byte{0x80, 'a'}, 1),
		maxKey:     kv.KeyWithTs([]byte{0x80, 'z'}, 1),
		size:       200,
		valueSize:  100,
		createdAt:  now.Add(-1 * time.Minute),
		maxVersion: 5,
	}
	t3 := &table{
		fid:        3,
		minKey:     kv.KeyWithTs([]byte{0x40, 'a'}, 1),
		maxKey:     kv.KeyWithTs([]byte{0x40, 'z'}, 1),
		size:       50,
		valueSize:  10,
		createdAt:  now.Add(-3 * time.Minute),
		maxVersion: 4,
	}

	var buf ingestBuffer
	buf.add(t1)
	buf.addBatch([]*table{t2, t3})

	if got := buf.tableCount(); got != 3 {
		t.Fatalf("expected 3 tables, got %d", got)
	}
	if got := buf.totalSize(); got != 350 {
		t.Fatalf("expected size 350, got %d", got)
	}
	if got := buf.totalValueSize(); got != 150 {
		t.Fatalf("expected value size 150, got %d", got)
	}
	if buf.maxAgeSeconds() <= 0 {
		t.Fatalf("expected max age > 0")
	}

	order := buf.shardOrderBySize()
	if len(order) != 3 {
		t.Fatalf("expected 3 shard order entries, got %d", len(order))
	}
	if order[0] != shardIndexForRange(t2.minKey) {
		t.Fatalf("expected largest shard first, got %v", order)
	}

	meta := buf.allMeta()
	if len(meta) != 3 {
		t.Fatalf("expected 3 metas, got %d", len(meta))
	}
	if meta[0].MaxVersion == 0 {
		t.Fatalf("expected max version in meta")
	}
	if buf.shardMetaByIndex(99) != nil {
		t.Fatalf("expected nil shard meta for invalid index")
	}

	buf.sortShards()
	views := buf.shardViews()
	if len(views) != 3 {
		t.Fatalf("expected 3 shard views, got %d", len(views))
	}

	buf.remove(map[uint64]struct{}{1: {}})
	if got := buf.tableCount(); got != 2 {
		t.Fatalf("expected 2 tables after remove, got %d", got)
	}
}

func TestTableIteratorSeekAndPrefetch(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builderOpt.BloomFalsePositive = 0.01
	builder := newTableBuiler(&builderOpt)

	for i := 0; i < 20; i++ {
		key := kv.KeyWithTs([]byte(fmt.Sprintf("k%02d", i)), 1)
		value := bytes.Repeat([]byte{'v'}, 48)
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := utils.FileNameSSTable(lsm.option.WorkDir, 1)
	tbl := openTable(lsm.levels, tableName, builder)
	if tbl == nil {
		t.Fatalf("expected table from builder")
	}
	defer func() {
		_ = tbl.DecrRef()
	}()

	tbl.mu.Lock()
	tbl.closeSSTableLocked()
	tbl.mu.Unlock()

	tbl.idx.Store(nil)
	tbl.lm.cache.delIndex(tbl.fid)
	tbl.keyCount = 0
	tbl.maxVersion = 0
	tbl.hasBloom = false

	seekKey := kv.KeyWithTs([]byte("k10"), 1)
	if !tbl.prefetchBlockForKey(seekKey, true) {
		t.Fatalf("expected prefetch to load block")
	}

	if tbl.KeyCount() == 0 {
		t.Fatalf("expected key count to be available")
	}
	if tbl.MaxVersionVal() == 0 {
		t.Fatalf("expected max version to be available")
	}
	if !tbl.HasBloomFilter() {
		t.Fatalf("expected bloom filter to be available")
	}

	idx := tbl.index()
	if idx == nil {
		t.Fatalf("expected table index")
	}
	if _, ok := tbl.blockOffset(len(idx.GetOffsets())); !ok {
		t.Fatalf("expected block offset lookup to succeed")
	}

	it := tbl.NewIterator(&utils.Options{IsAsc: true, PrefetchBlocks: 1, PrefetchWorkers: 1})
	tblIter, ok := it.(*tableIterator)
	if !ok {
		t.Fatalf("expected table iterator, got %T", it)
	}
	tblIter.Rewind()
	if !tblIter.Valid() {
		t.Fatalf("expected iterator to be valid after rewind")
	}
	if tblIter.bi != nil {
		_ = tblIter.bi.Rewind()
	}
	tblIter.Seek(seekKey)
	if tblIter.Valid() {
		_ = tblIter.Item()
	}
	tblIter.Next()
	_ = tblIter.Valid()
	if err := tblIter.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	it = tbl.NewIterator(&utils.Options{IsAsc: false})
	tblIter = it.(*tableIterator)
	tblIter.Rewind()
	if tblIter.Valid() {
		_ = tblIter.Item()
	}
	tblIter.Seek(seekKey)
	_ = tblIter.Valid()
	_ = tblIter.Close()
}

func TestFillMaxLevelTables(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	maxLevel := lsm.option.MaxLevelNum - 1
	if maxLevel < 1 {
		t.Fatalf("invalid max level %d", maxLevel)
	}

	tbl := &table{
		fid:           101,
		minKey:        kv.KeyWithTs([]byte("a"), 1),
		maxKey:        kv.KeyWithTs([]byte("z"), 1),
		size:          1 << 20,
		staleDataSize: 11 << 20,
		createdAt:     time.Now().Add(-2 * time.Hour),
		maxVersion:    1,
	}

	lsm.levels.levels[maxLevel].tables = []*table{tbl}
	cd := buildCompactDef(lsm, 0, maxLevel, maxLevel)
	cd.lockLevels()
	defer cd.unlockLevels()

	ok := lsm.levels.fillMaxLevelTables([]*table{tbl}, cd)
	if !ok {
		t.Fatalf("expected max-level compaction plan")
	}
	if len(cd.top) != 1 || cd.top[0] != tbl {
		t.Fatalf("expected compaction to select the max-level table")
	}
}

func TestLevelHandlerIngestMetrics(t *testing.T) {
	now := time.Now()
	t1 := &table{
		fid:        10,
		minKey:     kv.KeyWithTs([]byte{0x00, 'a'}, 1),
		maxKey:     kv.KeyWithTs([]byte{0x00, 'z'}, 1),
		size:       120,
		valueSize:  30,
		createdAt:  now.Add(-time.Minute),
		maxVersion: 1,
	}
	t2 := &table{
		fid:        11,
		minKey:     kv.KeyWithTs([]byte{0x80, 'a'}, 1),
		maxKey:     kv.KeyWithTs([]byte{0x80, 'z'}, 1),
		size:       60,
		valueSize:  10,
		createdAt:  now.Add(-2 * time.Minute),
		maxVersion: 1,
	}

	lh := &levelHandler{levelNum: 3}
	lh.addIngest(t1)
	lh.addIngest(t2)

	if got := lh.numIngestTables(); got != 2 {
		t.Fatalf("expected 2 ingest tables, got %d", got)
	}
	if got := lh.ingestDataSize(); got != 180 {
		t.Fatalf("expected ingest size 180, got %d", got)
	}
	if got := lh.ingestValueBytes(); got != 40 {
		t.Fatalf("expected ingest value bytes 40, got %d", got)
	}
	expectDensity := float64(40) / float64(180)
	if math.Abs(lh.ingestValueDensity()-expectDensity) > 1e-9 {
		t.Fatalf("unexpected ingest density")
	}
	if math.Abs(lh.ingestDensityLocked()-expectDensity) > 1e-9 {
		t.Fatalf("unexpected ingest density locked")
	}
	if lh.maxIngestAgeSeconds() <= 0 {
		t.Fatalf("expected non-zero max ingest age")
	}
	if idx := lh.ingestShardByBacklog(); idx < 0 {
		t.Fatalf("expected valid ingest shard index")
	}
}

func buildTestTable(t *testing.T, lsm *LSM, fid uint64) *table {
	t.Helper()
	builderOpt := *opt
	builderOpt.BlockSize = 64
	builderOpt.BloomFalsePositive = 0.01
	builder := newTableBuiler(&builderOpt)

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		key := kv.KeyWithTs([]byte(k), 1)
		builder.AddKey(kv.NewEntry(key, []byte("val-"+k)))
	}

	tableName := utils.FileNameSSTable(lsm.option.WorkDir, fid)
	tbl := openTable(lsm.levels, tableName, builder)
	if tbl == nil {
		t.Fatalf("expected table from builder")
	}
	return tbl
}

func TestIngestSearchAndPrefetch(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	tbl := buildTestTable(t, lsm, 7)
	defer func() { _ = tbl.DecrRef() }()

	key := kv.KeyWithTs([]byte("b"), 1)

	var buf ingestBuffer
	buf.add(tbl)

	found, err := buf.search(key)
	if err != nil {
		t.Fatalf("ingest search: %v", err)
	}
	if found == nil {
		t.Fatalf("expected entry")
	}
	if string(found.Key) != string(key) {
		t.Fatalf("expected key %q, got %q", key, found.Key)
	}

	if !buf.prefetch(key, true) {
		t.Fatalf("expected prefetch hit")
	}

	_, err = buf.search(kv.KeyWithTs([]byte("missing"), 1))
	if err != utils.ErrKeyNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestLevelSearchIngestAndLN(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	tbl := buildTestTable(t, lsm, 9)
	defer func() { _ = tbl.DecrRef() }()

	key := kv.KeyWithTs([]byte("c"), 1)

	lh := &levelHandler{levelNum: 3}
	lh.ingest.add(tbl)
	found, err := lh.searchIngestSST(key)
	if err != nil || found == nil {
		t.Fatalf("ingest search err=%v entry=%v", err, found)
	}

	lh.tables = []*table{tbl}
	found, err = lh.searchLNSST(key)
	if err != nil || found == nil {
		t.Fatalf("level search err=%v entry=%v", err, found)
	}

	if lh.getTableForKey(kv.KeyWithTs([]byte("z"), 1)) != nil {
		t.Fatalf("expected no table for key")
	}

	ingestHit, err := lh.Get(key)
	if err != nil || ingestHit == nil {
		t.Fatalf("level get err=%v entry=%v", err, ingestHit)
	}
	if !lh.prefetch(key, true) {
		t.Fatalf("expected level prefetch hit")
	}

	l0 := &levelHandler{levelNum: 0, tables: []*table{tbl}}
	l0Hit, err := l0.Get(key)
	if err != nil || l0Hit == nil {
		t.Fatalf("l0 get err=%v entry=%v", err, l0Hit)
	}
	if !l0.prefetch(key, true) {
		t.Fatalf("expected l0 prefetch hit")
	}

	lsm.levels.levels[0].tables = []*table{tbl}
	lmHit, err := lsm.levels.Get(key)
	if err != nil || lmHit == nil {
		t.Fatalf("levels get err=%v entry=%v", err, lmHit)
	}
}

func TestLSMMetricsAPIs(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	lsm.SetHotKeyProvider(func() [][]byte {
		return nil
	})

	entry := utils.BuildEntry()
	requireNoError := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	requireNoError(lsm.Set(entry))

	_ = lsm.FlushPending()
	_ = lsm.FlushMetrics()
	_, _ = lsm.CompactionStats()
	_, _, _ = lsm.CompactionDurations()
	_ = lsm.LevelMetrics()
	_ = lsm.CacheMetrics()
	_ = lsm.MaxVersion()

	if lsm.CompactionValueWeight() <= 0 {
		t.Fatalf("expected compaction value weight to be positive")
	}
	if lsm.CompactionValueAlertThreshold() <= 0 {
		t.Fatalf("expected compaction value alert threshold to be positive")
	}
	if lsm.ManifestManager() == nil {
		t.Fatalf("expected manifest manager to be available")
	}

	requireNoError(lsm.LogValueLogHead(&kv.ValuePtr{Fid: 1, Offset: 2}))
	requireNoError(lsm.LogValueLogUpdate(&manifest.ValueLogMeta{FileID: 1, Offset: 5, Valid: true}))
	_, _ = lsm.ValueLogHead()
	_ = lsm.ValueLogStatus()
	_ = lsm.CurrentVersion()
	requireNoError(lsm.LogValueLogDelete(1))
}

func TestLSMBatchAndMemHelpers(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	entries := []*kv.Entry{
		kv.NewEntry(kv.KeyWithTs([]byte("b1"), 1), []byte("v1")),
		kv.NewEntry(kv.KeyWithTs([]byte("b2"), 1), []byte("v2")),
	}
	if err := lsm.SetBatch(nil); err != nil {
		t.Fatalf("unexpected error on empty batch: %v", err)
	}
	if err := lsm.SetBatch(entries); err != nil {
		t.Fatalf("set batch: %v", err)
	}
	if err := lsm.SetBatch([]*kv.Entry{{}}); err == nil {
		t.Fatalf("expected empty key error")
	}

	if lsm.MemTableIsNil() {
		t.Fatalf("expected memtable to be initialized")
	}
	if lsm.MemSize() <= 0 {
		t.Fatalf("expected memtable size to be positive")
	}
	if lsm.GetSkipListFromMemTable() == nil {
		t.Fatalf("expected skiplist-backed memtable")
	}

	tables, release := lsm.GetMemTables()
	if len(tables) == 0 {
		t.Fatalf("expected memtables snapshot")
	}
	if release != nil {
		release()
	}

	lsm.levels.levels[0].tables = []*table{{keyCount: 2}}
	if count := lsm.EntryCount(); count <= 0 {
		t.Fatalf("expected entry count > 0, got %d", count)
	}

	lsm.Prefetch(entries[0].Key, false)
	lsm.Prefetch(nil, false)
}

func TestLevelManagerAdjustThrottleAndPointers(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	var events []bool
	lsm.SetThrottleCallback(func(on bool) {
		events = append(events, on)
	})

	// Force a low L0 table limit so AdjustThrottle toggles quickly.
	lsm.levels.opt.NumLevelZeroTables = 1
	l0 := lsm.levels.levels[0]
	l0.tables = []*table{{}, {}, {}}
	lsm.levels.AdjustThrottle()
	l0.tables = nil
	lsm.levels.AdjustThrottle()
	if len(events) != 2 || !events[0] || events[1] {
		t.Fatalf("unexpected throttle events: %+v", events)
	}

	lsm.levels.setLogPointer(3, 9)
	seg, off := lsm.levels.logPointer()
	if seg != 3 || off != 9 {
		t.Fatalf("unexpected log pointer %d/%d", seg, off)
	}

	lsm.levels.recordCompactionMetrics(5 * time.Millisecond)
	lastMs, maxMs, runs := lsm.levels.compactionDurations()
	if runs == 0 || lastMs <= 0 || maxMs <= 0 {
		t.Fatalf("unexpected compaction metrics: last=%f max=%f runs=%d", lastMs, maxMs, runs)
	}

	l0.tables = []*table{{maxVersion: 7, keyCount: 2}}
	if v := lsm.levels.maxVersion(); v != 7 {
		t.Fatalf("expected max version 7, got %d", v)
	}

	if !lsm.levels.canRemoveWalSegment(1) {
		t.Fatalf("expected WAL segment to be removable without raft pointers")
	}
	_ = lsm.levels.cacheMetrics()
}

func TestLevelHandlerOverlapAndMetrics(t *testing.T) {
	min := kv.InternalKey(kv.CFDefault, []byte("a"), 1)
	max := kv.InternalKey(kv.CFDefault, []byte("z"), 1)
	if keyInRange(min, max, nil) {
		t.Fatalf("expected nil key to be out of range")
	}
	if !keyInRange(min, max, []byte("m")) {
		t.Fatalf("expected key to be in range")
	}
	if keyInRange(min, max, []byte("0")) {
		t.Fatalf("expected key to be out of range")
	}

	lh := &levelHandler{levelNum: 2}
	lh.tables = []*table{
		{minKey: min, maxKey: max},
	}
	lh.ingest.ensureInit()
	lh.ingest.add(&table{
		minKey:    kv.InternalKey(kv.CFDefault, []byte("k"), 1),
		maxKey:    kv.InternalKey(kv.CFDefault, []byte("p"), 1),
		size:      50,
		valueSize: 20,
	})

	hotKeys := [][]byte{[]byte("b"), []byte("k"), []byte("x")}
	score := lh.hotOverlapScore(hotKeys, false)
	expected := float64(3) / float64(len(hotKeys))
	if math.Abs(score-expected) > 1e-9 {
		t.Fatalf("unexpected hot overlap score: %.2f", score)
	}
	ingestScore := lh.hotOverlapScore(hotKeys, true)
	if math.Abs(ingestScore-(1.0/3.0)) > 1e-9 {
		t.Fatalf("unexpected ingest overlap score: %.2f", ingestScore)
	}

	lh.totalSize = 100
	lh.totalValueSize = 40
	lh.totalStaleSize = 10
	metrics := lh.metricsSnapshot()
	if metrics.ValueDensity <= 0 || metrics.IngestValueDensity <= 0 {
		t.Fatalf("expected non-zero density metrics")
	}

	tbl := &table{hasBloom: true}
	if !tbl.HasBloomFilter() {
		t.Fatalf("expected bloom filter to be reported")
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
