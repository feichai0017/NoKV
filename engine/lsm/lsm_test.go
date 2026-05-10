package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
	tablepkg "github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
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

// ikey builds an internal key for tests using the default column family.
func ikey(s string, ts uint64) []byte {
	return kv.InternalKey(kv.CFDefault, []byte(s), ts)
}

// splitUserKey extracts the user-key portion of an internal key, asserting
// that the input was a well-formed internal key.
func splitUserKey(t *testing.T, internal []byte) []byte {
	t.Helper()
	_, userKey, _, ok := kv.SplitInternalKey(internal)
	require.True(t, ok)
	return userKey
}

func buildInternalTestEntry() *kv.Entry {
	return newRandomTestEntry()
}

func newRandomTestEntry() *kv.Entry {
	const charset = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ~=+%^*/()[]{}/!@#$?|NoKV"
	randStr := func(length int) string {
		if length <= 0 {
			return ""
		}
		out := make([]byte, length)
		for i := range out {
			out[i] = charset[rand.Intn(len(charset))]
		}
		return string(out)
	}
	key := []byte(randStr(16))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12 * time.Hour).Unix())
	return kv.NewInternalEntry(kv.CFDefault, key, kv.MaxVersion, value, 0, expiresAt)
}

func newTestLSMOptions(workDir string, fs vfs.FS) *Options {
	return &Options{
		FS:                  fs,
		WorkDir:             workDir,
		SSTableMaxSz:        1 << 20,
		MemTableSize:        1 << 20,
		BlockSize:           4 << 10,
		BloomFalsePositive:  0.01,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       1,
	}
}

// TestBase is a basic correctness test.
func TestBase(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()
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
		require.NoError(t, first.Close())

		// A successful restart must still pass the base test.
		reopened := buildLSM()
		reopened.StartCompacter()
		defer func() { _ = reopened.Close() }()
		baseTest(t, reopened, 128)
	}
	// Run N times to exercise multiple SSTables.
	runTest(1, test)
}

func TestNewLSMInitReturnsError(t *testing.T) {
	dir := t.TempDir()
	injected := errors.New("manifest open injected")
	manifestPath := filepath.Join(dir, "MANIFEST-000001")
	policy := vfs.NewFaultPolicy(vfs.FailOnceRule(vfs.OpOpenFile, manifestPath, injected))
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	wlog, err := wal.Open(wal.Config{Dir: dir, FS: fs})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer func() { _ = wlog.Close() }()

	_, err = NewLSM(newTestLSMOptions(dir, fs), []*wal.Manager{wlog})
	if !errors.Is(err, injected) {
		t.Fatalf("expected injected init error, got: %v", err)
	}
}

func TestRotateReturnsSubmitError(t *testing.T) {
	dir := t.TempDir()
	wlog, err := wal.Open(wal.Config{Dir: dir})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	lsm, err := NewLSM(newTestLSMOptions(dir, nil), []*wal.Manager{wlog})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer func() { _ = wlog.Close() }()
	defer func() { _ = lsm.Close() }()

	if err := lsm.flushQueue.close(); err != nil {
		t.Fatalf("close flush queue: %v", err)
	}
	if err := lsm.Rotate(); err == nil {
		t.Fatalf("expected rotate to return submit error")
	}
}

func TestCloseBestEffortAggregatesErrors(t *testing.T) {
	dir := t.TempDir()
	policy := vfs.NewFaultPolicy()
	fs := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, policy)

	wlog, err := wal.Open(wal.Config{Dir: dir, FS: fs})
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	lsm, err := NewLSM(newTestLSMOptions(dir, fs), []*wal.Manager{wlog})
	if err != nil {
		t.Fatalf("new lsm: %v", err)
	}
	defer func() { _ = wlog.Close() }()

	for i := range 2 {
		entry := kv.NewInternalEntry(kv.CFDefault, []byte{byte('a' + i)}, uint64(i+1), []byte("v"), 0, 0)
		if err := lsm.Set(entry); err != nil {
			entry.DecrRef()
			t.Fatalf("set entry %d: %v", i, err)
		}
		entry.DecrRef()
		if err := lsm.Rotate(); err != nil {
			t.Fatalf("rotate %d: %v", i, err)
		}
	}
	waitForL0Tables(t, lsm, 2)

	l0Tables := lsm.levels.levels[0].tablesSnapshot()
	if len(l0Tables) < 2 {
		t.Fatalf("expected at least 2 L0 tables, got %d", len(l0Tables))
	}
	path1 := vfs.FileNameSSTable(dir, l0Tables[0].FID())
	path2 := vfs.FileNameSSTable(dir, l0Tables[1].FID())
	closeErr1 := errors.New("close table 1 injected")
	closeErr2 := errors.New("close table 2 injected")
	policy.AddRule(vfs.FailOnceRule(vfs.OpFileClose, path1, closeErr1))
	policy.AddRule(vfs.FailOnceRule(vfs.OpFileClose, path2, closeErr2))

	err = lsm.Close()
	if !errors.Is(err, closeErr1) {
		t.Fatalf("expected joined close error to include err1, got: %v", err)
	}
	if !errors.Is(err, closeErr2) {
		t.Fatalf("expected joined close error to include err2, got: %v", err)
	}
}

// TestHitStorage exercises read paths across storage tiers.
func TestHitStorage(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()
	e := buildInternalTestEntry()
	defer e.DecrRef()
	if err := lsm.Set(e); err != nil {
		t.Fatalf("lsm.Set: %v", err)
	}
	// Hit the memtable path.
	hitMemtable := func() {
		v, err := lsm.shards[0].memTable.Get(e.Key)
		require.NoError(t, err)
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
		ee := buildInternalTestEntry()
		defer ee.DecrRef()
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
	defer func() { _ = lsm.Close() }()

	var (
		mu     sync.Mutex
		events []WriteThrottleState
	)
	lsm.throttleFn = func(state WriteThrottleState) {
		mu.Lock()
		events = append(events, state)
		mu.Unlock()
	}

	lsm.throttleWrites(WriteThrottleStop, 1000, 0)
	lsm.throttleWrites(WriteThrottleStop, 1000, 0)
	lsm.throttleWrites(WriteThrottleSlowdown, 400, 256<<20)
	lsm.throttleWrites(WriteThrottleNone, 0, 0)
	lsm.throttleWrites(WriteThrottleNone, 0, 0)

	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 {
		t.Fatalf("unexpected throttle events: %+v", events)
	}
	if events[0] != WriteThrottleStop {
		t.Fatalf("expected first throttle event to enter stop mode, got %+v", events[0])
	}
	if events[1] != WriteThrottleSlowdown {
		t.Fatalf("expected second throttle event to enter slowdown mode, got %+v", events[1])
	}
	if events[2] != WriteThrottleNone {
		t.Fatalf("expected third throttle event to clear throttling, got %+v", events[2])
	}
}

// TestPsarameter verifies invalid argument handling.
func TestPsarameter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()
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
	defer func() { _ = lsm.Close() }()

	key := []byte("tombstone-key-00000000")
	val := []byte("value")

	e := kv.NewEntry(key, val)
	if err := lsm.Set(e); err != nil {
		t.Fatalf("lsm.Set: %v", err)
	}

	if err := lsm.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
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

// TestLandingBufferAccounting was removed: the synthetic-table construction
// it used poked at table package private fields (now inaccessible from lsm).
// engine/lsm/landing/landing_test.go covers the same Buffer behavior using
// a fakeTable shape, so this redundant lsm-side coverage is dropped.

// TestTableIteratorSeekAndIteratorPrefetch moved to
// engine/lsm/table/iterator_test.go because it pokes at internal Table state
// (mu, closeSSTableLocked, idx, keyCount, maxVersion, hasBloom) that is no
// longer reachable from this package.

func TestFillMaxLevelTables(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	maxLevel := lsm.option.MaxLevelNum - 1
	if maxLevel < 1 {
		t.Fatalf("invalid max level %d", maxLevel)
	}

	tbl := tablepkg.NewTestTable(tablepkg.TestTableSpec{
		FID:           101,
		MinKey:        kv.InternalKey(kv.CFDefault, []byte("a"), 1),
		MaxKey:        kv.InternalKey(kv.CFDefault, []byte("z"), 1),
		Size:          1 << 20,
		StaleDataSize: 11 << 20,
		CreatedAt:     time.Now().Add(-2 * time.Hour),
		MaxVersion:    1,
	})

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

// TestMaxLevelCompactionNoRangeDeleteResurrection verifies that a max-level
// compaction which rewrites only the tombstone table does not resurrect older
// covered point keys that remain in other max-level tables.
func TestMaxLevelCompactionNoRangeDeleteResurrection(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	maxLevel := lsm.option.MaxLevelNum - 1
	if maxLevel < 1 {
		t.Fatalf("invalid max level %d", maxLevel)
	}

	// Table A: range tombstone [a, z)@10.
	rt := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 10), []byte("z"))
	rt.Meta = kv.BitRangeDelete
	tombstoneTbl := buildTableWithEntries(t, lsm, 1001, rt)

	// Table B: older covered point key y@1.
	pointTbl := buildTableWithEntry(t, lsm, 1002, "y", 1, "old-y")

	lh := lsm.levels.levels[maxLevel]
	lh.add(tombstoneTbl)
	lh.add(pointTbl)
	lh.Sort()

	// Force max-level planner to pick only tombstoneTbl.
	tombstoneTbl.SetTestStaleDataSize(uint32(11 << 20))
	tombstoneTbl.SetTestCreatedAt(time.Now().Add(-2 * time.Hour))
	pointTbl.SetTestStaleDataSize(uint32(0))
	pointTbl.SetTestCreatedAt(time.Now().Add(-2 * time.Hour))

	// We inserted tables directly, so rebuild the in-memory tombstone index.
	lsm.levels.rebuildRangeTombstones()
	if lsm.RangeTombstoneCount() == 0 {
		t.Fatalf("expected range tombstone collector to contain tombstones")
	}

	seek := kv.InternalKey(kv.CFDefault, []byte("y"), math.MaxUint64)
	if got, err := lsm.Get(seek); err != utils.ErrKeyNotFound {
		if got != nil {
			got.DecrRef()
		}
		t.Fatalf("before compaction: expected key y to be hidden by range tombstone, got err=%v", err)
	}

	cd := buildCompactDef(lsm, 0, maxLevel, maxLevel)
	// Keep target size tiny so collectBotTables does not include pointTbl.
	cd.spec.ThisFileSize = 1
	cd.spec.NextFileSize = 1
	if ok := lsm.levels.fillTables(cd); !ok {
		t.Fatalf("expected max-level compaction plan")
	}
	if len(cd.top) != 1 || cd.top[0].FID() != tombstoneTbl.FID() {
		t.Fatalf("expected compaction to select only tombstone table, got top=%v", tablesToString(cd.top))
	}
	if len(cd.bot) != 0 {
		t.Fatalf("expected no bot tables to be compacted, got %d", len(cd.bot))
	}

	if err := lsm.levels.runCompactDef(0, maxLevel, *cd); err != nil {
		t.Fatalf("runCompactDef max-level: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))

	if got, err := lsm.Get(seek); err != utils.ErrKeyNotFound {
		if got != nil {
			got.DecrRef()
		}
		t.Fatalf("after compaction: key resurrected, expected ErrKeyNotFound, got err=%v", err)
	}
}

// TestMaxLevelCompactionRangeDeleteResurrection is a regression test for the
// bug where dropping a max-level range tombstone during partial rewrite could
// resurrect older covered point keys once tombstone state is rebuilt.
func TestMaxLevelCompactionRangeDeleteResurrection(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	maxLevel := lsm.option.MaxLevelNum - 1
	if maxLevel < 1 {
		t.Fatalf("invalid max level %d", maxLevel)
	}

	compactL0To := func(level int) {
		t.Helper()
		cd := buildCompactDef(lsm, 0, 0, level)
		if ok := lsm.levels.fillTables(cd); !ok {
			t.Fatalf("expected L0->L%d compaction plan", level)
		}
		if err := lsm.levels.runCompactDef(0, 0, *cd); err != nil {
			t.Fatalf("runCompactDef L0->L%d: %v", level, err)
		}
		require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	}
	seek := kv.InternalKey(kv.CFDefault, []byte("y"), math.MaxUint64)

	// 1) Create older point key in one max-level table.
	point := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("y"), 1), []byte("old-y"))
	if err := lsm.Set(point); err != nil {
		t.Fatalf("set point: %v", err)
	}
	if err := lsm.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	waitForL0(t, lsm)
	compactL0To(maxLevel)
	if got, err := lsm.Get(seek); err != nil {
		t.Fatalf("expected point key visible before tombstone, got err=%v", err)
	} else {
		if !bytes.Equal(got.Value, []byte("old-y")) {
			got.DecrRef()
			t.Fatalf("expected point value old-y, got %q", got.Value)
		}
		got.DecrRef()
	}

	// 2) Create newer range tombstone in a separate max-level table.
	rt := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 10), []byte("z"))
	rt.Meta = kv.BitRangeDelete
	if err := lsm.Set(rt); err != nil {
		t.Fatalf("set range tombstone: %v", err)
	}
	if err := lsm.Rotate(); err != nil {
		t.Fatalf("rotate: %v", err)
	}
	waitForL0(t, lsm)
	compactL0To(maxLevel)

	if got, err := lsm.Get(seek); err != utils.ErrKeyNotFound {
		if got != nil {
			got.DecrRef()
		}
		t.Fatalf("precondition failed: expected y hidden by tombstone, got err=%v", err)
	}

	// 3) Force max-level planner to rewrite only the tombstone table.
	maxTables := lsm.levels.levels[maxLevel].tablesSnapshot()
	if len(maxTables) < 2 {
		t.Fatalf("expected at least two max-level tables, got %d", len(maxTables))
	}
	var tombstoneTbl *table
	oldEnough := time.Now().Add(-2 * time.Hour)
	for _, tbl := range maxTables {
		tbl.SetTestCreatedAt(oldEnough)
		tbl.SetTestStaleDataSize(uint32(0))
		if tableContainsRangeDelete(tbl) {
			tombstoneTbl = tbl
		}
	}
	if tombstoneTbl == nil {
		t.Fatalf("expected one max-level table containing range tombstone")
	}
	tombstoneTbl.SetTestStaleDataSize(uint32(11 << 20))

	cd := buildCompactDef(lsm, 0, maxLevel, maxLevel)
	// Keep target size tiny so collectBotTables does not include adjacent tables.
	cd.spec.ThisFileSize = 1
	cd.spec.NextFileSize = 1
	if ok := lsm.levels.fillTables(cd); !ok {
		t.Fatalf("expected max-level compaction plan")
	}
	if len(cd.top) != 1 || cd.top[0].FID() != tombstoneTbl.FID() {
		t.Fatalf("expected compaction top to be only tombstone table, got %v", tablesToString(cd.top))
	}
	if len(cd.bot) != 0 {
		t.Fatalf("expected bot to be empty for partial max-level rewrite, got %d", len(cd.bot))
	}

	if err := lsm.levels.runCompactDef(0, maxLevel, *cd); err != nil {
		t.Fatalf("runCompactDef max-level: %v", err)
	}
	require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))

	// Sanity: point key table should still exist because only tombstone table was compacted.
	hasPointInSST := false
	for _, tbl := range lsm.levels.levels[maxLevel].tablesSnapshot() {
		if tbl == nil {
			continue
		}
		var v uint64
		e, err := tbl.Search(seek, &v)
		if err == nil && e != nil {
			hasPointInSST = true
			e.DecrRef()
			break
		}
	}
	if !hasPointInSST {
		t.Fatalf("sanity failed: no max-level SST contains key y after tombstone-only compaction")
	}

	// 4) Restart to ensure visibility comes only from persisted state.
	// If tombstone was dropped too early, y becomes visible again (resurrection).
	workDir := lsm.option.WorkDir
	if err := lsm.Close(); err != nil {
		t.Fatalf("close before reopen: %v", err)
	}
	opt.WorkDir = workDir
	lsm = buildLSM()

	if got, err := lsm.Get(seek); err != utils.ErrKeyNotFound {
		if got != nil {
			got.DecrRef()
		}
		t.Fatalf("regression: key y resurrected after max-level tombstone drop, err=%v", err)
	}
}

func TestLevelHandlerLandingMetrics(t *testing.T) {
	now := time.Now()
	t1 := tablepkg.NewTestTable(tablepkg.TestTableSpec{
		FID:        10,
		MinKey:     kv.InternalKey(kv.CFDefault, []byte{0x00, 'a'}, 1),
		MaxKey:     kv.InternalKey(kv.CFDefault, []byte{0x00, 'z'}, 1),
		Size:       120,
		ValueSize:  30,
		CreatedAt:  now.Add(-time.Minute),
		MaxVersion: 1,
	})
	t2 := tablepkg.NewTestTable(tablepkg.TestTableSpec{
		FID:        11,
		MinKey:     kv.InternalKey(kv.CFDefault, []byte{0x80, 'a'}, 1),
		MaxKey:     kv.InternalKey(kv.CFDefault, []byte{0x80, 'z'}, 1),
		Size:       60,
		ValueSize:  10,
		CreatedAt:  now.Add(-2 * time.Minute),
		MaxVersion: 1,
	})

	lh := &levelHandler{levelNum: 3}
	lh.addLanding(t1)
	lh.addLanding(t2)

	if got := lh.numLandingTables(); got != 2 {
		t.Fatalf("expected 2 landing tables, got %d", got)
	}
	if got := lh.landingDataSize(); got != 180 {
		t.Fatalf("expected landing size 180, got %d", got)
	}
	if got := lh.landingValueBytes(); got != 40 {
		t.Fatalf("expected landing value bytes 40, got %d", got)
	}
	expectDensity := float64(40) / float64(180)
	if math.Abs(lh.landingValueDensity()-expectDensity) > 1e-9 {
		t.Fatalf("unexpected landing density")
	}
	if math.Abs(lh.landingDensityLocked()-expectDensity) > 1e-9 {
		t.Fatalf("unexpected landing density locked")
	}
	if lh.maxLandingAgeSeconds() <= 0 {
		t.Fatalf("expected non-zero max landing age")
	}
	if idx := lh.landingShardByBacklog(); idx < 0 {
		t.Fatalf("expected valid landing shard index")
	}
}

func buildTestTable(t *testing.T, lsm *LSM, fid uint64) *table {
	t.Helper()
	builderOpt := *opt
	builderOpt.BlockSize = 64
	builderOpt.BloomFalsePositive = 0.01
	builder := tablepkg.NewBuilder(tableOptionsFor(&builderOpt))

	keys := []string{"a", "b", "c"}
	for _, k := range keys {
		key := kv.InternalKey(kv.CFDefault, []byte(k), 1)
		builder.AddKey(kv.NewEntry(key, []byte("val-"+k)))
	}

	tableName := vfs.FileNameSSTable(lsm.option.WorkDir, fid)
	tbl, err := tablepkg.Open(lsm.levels, tableName, builder)
	if err != nil {
		t.Fatalf("openTable: %v", err)
	}
	if tbl == nil {
		t.Fatalf("expected table from builder, got nil")
	}
	return tbl
}

func buildTableWithEntry(t *testing.T, lsm *LSM, fid uint64, key string, ver uint64, val string) *table {
	t.Helper()
	builderOpt := *opt
	builderOpt.BlockSize = 64
	builderOpt.BloomFalsePositive = 0.01
	builder := tablepkg.NewBuilder(tableOptionsFor(&builderOpt))

	ikey := kv.InternalKey(kv.CFDefault, []byte(key), ver)
	builder.AddKey(kv.NewEntry(ikey, []byte(val)))

	tableName := vfs.FileNameSSTable(lsm.option.WorkDir, fid)
	tbl, err := tablepkg.Open(lsm.levels, tableName, builder)
	if err != nil {
		t.Fatalf("openTable: %v", err)
	}
	if tbl == nil {
		t.Fatalf("expected table from builder, got nil")
	}
	return tbl
}

func buildTableWithEntries(t *testing.T, lsm *LSM, fid uint64, entries ...*kv.Entry) *table {
	t.Helper()
	builderOpt := *opt
	builderOpt.BlockSize = 64
	builderOpt.BloomFalsePositive = 0.01
	builder := tablepkg.NewBuilder(tableOptionsFor(&builderOpt))

	for _, e := range entries {
		builder.AddKey(e)
	}

	tableName := vfs.FileNameSSTable(lsm.option.WorkDir, fid)
	tbl, err := tablepkg.Open(lsm.levels, tableName, builder)
	if err != nil {
		t.Fatalf("openTable failed: %v", err)
	}
	if tbl == nil {
		t.Fatalf("expected table from builder")
	}
	return tbl
}

func tableContainsRangeDelete(tbl *table) bool {
	if tbl == nil {
		return false
	}
	it := tbl.NewIterator(&index.Options{IsAsc: true})
	if it == nil {
		return false
	}
	defer func() { _ = it.Close() }()

	for it.Rewind(); it.Valid(); it.Next() {
		item := it.Item()
		if item == nil || item.Entry() == nil {
			continue
		}
		if item.Entry().IsRangeDelete() {
			return true
		}
	}
	return false
}

func TestLandingSearch(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tbl := buildTestTable(t, lsm, 7)
	defer func() { _ = tbl.DecrRef() }()

	key := kv.InternalKey(kv.CFDefault, []byte("b"), 1)

	var buf landingBuffer
	buf.Add(tbl)

	found, err := buf.Search(key, nil)
	if err != nil {
		t.Fatalf("landing search: %v", err)
	}
	if found == nil {
		t.Fatalf("expected entry")
		return
	}
	if string(found.Key) != string(key) {
		t.Fatalf("expected key %q, got %q", key, found.Key)
	}
	found.DecrRef()

	_, err = buf.Search(kv.InternalKey(kv.CFDefault, []byte("missing"), 1), nil)
	if err != utils.ErrKeyNotFound {
		t.Fatalf("expected not found, got %v", err)
	}
}

func TestLandingSearchPrefersLatestVersion(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tblOld := buildTableWithEntry(t, lsm, 11, "b", 1, "v1")
	tblNew := buildTableWithEntry(t, lsm, 12, "b", 3, "v3")
	defer func() { _ = tblOld.DecrRef() }()
	defer func() { _ = tblNew.DecrRef() }()

	var buf landingBuffer
	buf.Add(tblOld)
	buf.Add(tblNew)

	key := kv.InternalKey(kv.CFDefault, []byte("b"), math.MaxUint64)
	found, err := buf.Search(key, nil)
	if err != nil || found == nil {
		t.Fatalf("landing search err=%v entry=%v", err, found)
	}
	if string(found.Value) != "v3" {
		t.Fatalf("expected latest value v3, got %q", string(found.Value))
	}
	found.DecrRef()
}

func TestLevelGetPrefersMainVersion(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	landingTbl := buildTableWithEntry(t, lsm, 21, "k", 1, "old")
	mainTbl := buildTableWithEntry(t, lsm, 22, "k", 3, "new")
	defer func() { _ = landingTbl.DecrRef() }()
	defer func() { _ = mainTbl.DecrRef() }()

	lh := &levelHandler{levelNum: 3}
	lh.landing.Add(landingTbl)
	lh.tables = []*table{mainTbl}

	key := kv.InternalKey(kv.CFDefault, []byte("k"), math.MaxUint64)
	got, err := lh.Get(key)
	if err != nil || got == nil {
		t.Fatalf("level get err=%v entry=%v", err, got)
	}
	if string(got.Value) != "new" {
		t.Fatalf("expected main value new, got %q", string(got.Value))
	}
	got.DecrRef()
}

func TestLevelGetMainWhenLandingEmpty(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	mainTbl := buildTableWithEntry(t, lsm, 23, "k", 2, "main")
	defer func() { _ = mainTbl.DecrRef() }()

	lh := &levelHandler{levelNum: 2}
	lh.tables = []*table{mainTbl}

	key := kv.InternalKey(kv.CFDefault, []byte("k"), math.MaxUint64)
	got, err := lh.Get(key)
	if err != nil || got == nil {
		t.Fatalf("level get err=%v entry=%v", err, got)
	}
	if string(got.Value) != "main" {
		t.Fatalf("expected main value, got %q", string(got.Value))
	}
	got.DecrRef()
}

func TestL0SearchPrefersLatestVersion(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tblOther := buildTableWithEntry(t, lsm, 31, "a", 2, "va")
	tblOld := buildTableWithEntry(t, lsm, 32, "b", 1, "v1")
	tblNew := buildTableWithEntry(t, lsm, 33, "b", 3, "v3")
	defer func() { _ = tblOther.DecrRef() }()
	defer func() { _ = tblOld.DecrRef() }()
	defer func() { _ = tblNew.DecrRef() }()

	key := kv.InternalKey(kv.CFDefault, []byte("b"), math.MaxUint64)
	l0 := &levelHandler{levelNum: 0, tables: []*table{tblOther, tblOld, tblNew}}
	got, err := l0.searchL0SST(key)
	if err != nil || got == nil {
		t.Fatalf("l0 search err=%v entry=%v", err, got)
	}
	if string(got.Value) != "v3" {
		t.Fatalf("expected latest value v3, got %q", string(got.Value))
	}
	got.DecrRef()

	l0 = &levelHandler{levelNum: 0, tables: []*table{tblNew, tblOld}}
	got, err = l0.searchL0SST(key)
	if err != nil || got == nil {
		t.Fatalf("l0 search err=%v entry=%v", err, got)
	}
	if string(got.Value) != "v3" {
		t.Fatalf("expected latest value v3, got %q", string(got.Value))
	}
	got.DecrRef()
}

func TestL0SearchPrefersNewestTableForSameVersion(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	key := []byte("same-version")
	oldLock := kv.NewInternalEntry(kv.CFLock, key, kv.MaxVersion, []byte("lock"), 0, 0)
	newDelete := kv.NewInternalEntry(kv.CFLock, key, kv.MaxVersion, nil, kv.BitDelete, 0)
	tblOld := buildTableWithEntries(t, lsm, 41, oldLock)
	tblNew := buildTableWithEntries(t, lsm, 42, newDelete)
	defer func() { _ = tblOld.DecrRef() }()
	defer func() { _ = tblNew.DecrRef() }()

	query := kv.InternalKey(kv.CFLock, key, kv.MaxVersion)
	l0 := &levelHandler{levelNum: 0, tables: []*table{tblOld, tblNew}}
	got, err := l0.searchL0SST(query)
	if err != nil || got == nil {
		t.Fatalf("l0 search err=%v entry=%v", err, got)
	}
	if got.Meta&kv.BitDelete == 0 {
		t.Fatalf("expected newest same-version table tombstone, got meta=%d value=%q", got.Meta, got.Value)
	}
	got.DecrRef()
}

func TestLevelSearchRespectsMaxVersion(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tbl := buildTableWithEntry(t, lsm, 41, "k", 2, "v2")
	defer func() { _ = tbl.DecrRef() }()

	lh := &levelHandler{levelNum: 3, tables: []*table{tbl}}
	key := kv.InternalKey(kv.CFDefault, []byte("k"), math.MaxUint64)

	maxVer := uint64(5)
	got, err := lh.searchLNSST(key, &maxVer)
	if err != utils.ErrKeyNotFound || got != nil {
		t.Fatalf("expected not found, got err=%v entry=%v", err, got)
	}
}

func TestLevelSearchLandingAndLN(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tbl := buildTestTable(t, lsm, 9)
	defer func() { _ = tbl.DecrRef() }()

	key := kv.InternalKey(kv.CFDefault, []byte("c"), 1)

	lh := &levelHandler{levelNum: 3}
	lh.landing.Add(tbl)
	found, err := lh.landing.Search(key, nil)
	if err != nil || found == nil {
		t.Fatalf("landing search err=%v entry=%v", err, found)
	}
	found.DecrRef()

	lh.tables = []*table{tbl}
	found, err = lh.searchLNSST(key, nil)
	if err != nil || found == nil {
		t.Fatalf("level search err=%v entry=%v", err, found)
	}
	found.DecrRef()

	if lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("z"), 1)) != nil {
		t.Fatalf("expected no table for key")
	}

	landingHit, err := lh.Get(key)
	if err != nil || landingHit == nil {
		t.Fatalf("level get err=%v entry=%v", err, landingHit)
	}
	landingHit.DecrRef()

	l0 := &levelHandler{levelNum: 0, tables: []*table{tbl}}
	l0Hit, err := l0.Get(key)
	if err != nil || l0Hit == nil {
		t.Fatalf("l0 get err=%v entry=%v", err, l0Hit)
	}
	l0Hit.DecrRef()

	lsm.levels.levels[0].tables = []*table{tbl}
	lmHit, err := lsm.levels.Get(key)
	if err != nil || lmHit == nil {
		t.Fatalf("levels get err=%v entry=%v", err, lmHit)
	}
	lmHit.DecrRef()
}

func TestGetTableForKeyBinarySearchBoundariesAndGap(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	tblA := buildTableWithEntry(t, lsm, 51, "a", 1, "va")
	tblD := buildTableWithEntry(t, lsm, 52, "d", 1, "vd")
	tblG := buildTableWithEntry(t, lsm, 53, "g", 1, "vg")
	defer func() { _ = tblA.DecrRef() }()
	defer func() { _ = tblD.DecrRef() }()
	defer func() { _ = tblG.DecrRef() }()

	lh := &levelHandler{
		levelNum: 2,
		tables:   []*table{tblA, tblD, tblG},
	}

	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("a"), math.MaxUint64)); got != tblA {
		t.Fatalf("expected table a, got %+v", got)
	}
	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("d"), 1)); got != tblD {
		t.Fatalf("expected table d, got %+v", got)
	}
	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("g"), 7)); got != tblG {
		t.Fatalf("expected table g, got %+v", got)
	}

	// Key gaps between single-key tables should return nil.
	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("b"), 1)); got != nil {
		t.Fatalf("expected nil for key gap b, got %+v", got)
	}
	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("f"), 1)); got != nil {
		t.Fatalf("expected nil for key gap f, got %+v", got)
	}

	// Out-of-range keys should return nil quickly.
	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("0"), 1)); got != nil {
		t.Fatalf("expected nil for low key, got %+v", got)
	}
	if got := lh.getTableForKey(kv.InternalKey(kv.CFDefault, []byte("z"), 1)); got != nil {
		t.Fatalf("expected nil for high key, got %+v", got)
	}
}

func TestLSMMetricsAPIs(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("diag-key"), 9, []byte("diag-value"), 0, 0)
	defer entry.DecrRef()
	requireNoError := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	requireNoError(lsm.Set(entry))

	_ = lsm.FlushPending()
	diag := lsm.Diagnostics()
	if diag.MaxVersion != lsm.MaxVersion() {
		t.Fatalf("expected diagnostics max version %d to match lsm max version %d", diag.MaxVersion, lsm.MaxVersion())
	}
	if diag.Compaction.ValueWeight <= 0 {
		t.Fatalf("expected compaction value weight to be positive")
	}
	if diag.Compaction.AlertThreshold <= 0 {
		t.Fatalf("expected compaction value alert threshold to be positive")
	}
}

func TestLSMBatchAndMemHelpers(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entries := []*kv.Entry{
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("b1"), 1), []byte("v1")),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("b2"), 1), []byte("v2")),
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

	if lsm.memTableIsNil() {
		t.Fatalf("expected memtable to be initialized")
	}
	if lsm.MemSize() <= 0 {
		t.Fatalf("expected memtable size to be positive")
	}
	if _, ok := lsm.shards[0].memTable.index.(*index.ART); !ok {
		t.Fatalf("expected ART-backed memtable")
	}

	tables, release := lsm.getMemTables()
	if len(tables) == 0 {
		t.Fatalf("expected memtables snapshot")
	}
	if release != nil {
		release()
	}

	lsm.levels.levels[0].tables = []*table{tablepkg.NewTestTable(tablepkg.TestTableSpec{KeyCount: 2, MaxVersion: 1})}
	if count := lsm.Diagnostics().Entries; count <= 0 {
		t.Fatalf("expected entry count > 0, got %d", count)
	}
}

func TestLSMSetBatchWritesSingleBatchRecord(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entries := []*kv.Entry{
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("ab1"), 1), []byte("v1")),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("ab2"), 1), []byte("v2")),
	}
	if err := lsm.SetBatch(entries); err != nil {
		t.Fatalf("set batch: %v", err)
	}
	shard := lsm.shards[0]
	if err := shard.wal.Sync(); err != nil {
		t.Fatalf("wal sync: %v", err)
	}

	var (
		entryRecords uint64
		batchRecords uint64
	)
	if err := shard.wal.Replay(func(info wal.EntryInfo, _ []byte) error {
		switch info.Type {
		case wal.RecordTypeEntry:
			entryRecords++
		case wal.RecordTypeEntryBatch:
			batchRecords++
		}
		return nil
	}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if entryRecords != 0 {
		t.Fatalf("expected zero single entry records, got %d", entryRecords)
	}
	if batchRecords != 1 {
		t.Fatalf("expected one batch record, got %d", batchRecords)
	}
}

func TestLSMSetBatchRejectsOversizedAtomicBatch(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	large := bytes.Repeat([]byte("x"), 700)
	entries := []*kv.Entry{
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("big1"), 1), large),
		kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("big2"), 1), large),
	}
	err := lsm.SetBatch(entries)
	if !errors.Is(err, utils.ErrTxnTooBig) {
		t.Fatalf("expected ErrTxnTooBig, got %v", err)
	}
}

func TestLSMSetBatchConcurrentReservations(t *testing.T) {
	clearDir()
	prevSize := opt.MemTableSize
	opt.MemTableSize = 8 << 10
	defer func() { opt.MemTableSize = prevSize }()

	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const (
		workers = 4
		rounds  = 30
	)
	value := bytes.Repeat([]byte("v"), 64)

	errCh := make(chan error, workers*rounds)
	var wg sync.WaitGroup
	for w := range workers {
		workerID := w
		wg.Go(func() {
			for i := range rounds {
				entries := []*kv.Entry{
					kv.NewEntry(kv.InternalKey(kv.CFDefault, fmt.Appendf(nil, "w%d-r%d-a", workerID, i), 1), value),
					kv.NewEntry(kv.InternalKey(kv.CFDefault, fmt.Appendf(nil, "w%d-r%d-b", workerID, i), 1), value),
				}
				err := lsm.SetBatch(entries)
				for _, entry := range entries {
					entry.DecrRef()
				}
				if err != nil {
					errCh <- err
					return
				}
			}
		})
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for concurrent SetBatch writers")
	}

	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatalf("set batch failed: %v", err)
		}
	}
}

func newWritePipelineEntry(key string, version uint64) *kv.Entry {
	return kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte(key), version), []byte("value-"+key))
}

func newTestWriteBatch(entries ...*kv.Entry) *writeBatch {
	return &writeBatch{entries: entries}
}

func TestWriteBatchesGroupIntoOneWALBatch(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entries := []*kv.Entry{
		newWritePipelineEntry("pipe-a", 1),
		newWritePipelineEntry("pipe-b", 2),
		newWritePipelineEntry("pipe-c", 3),
	}
	defer func() {
		for _, entry := range entries {
			entry.DecrRef()
		}
	}()
	batches := []*writeBatch{
		newTestWriteBatch(entries[0]),
		newTestWriteBatch(entries[1]),
		newTestWriteBatch(entries[2]),
	}

	failedAt, err := lsm.applyWriteBatches(lsm.shards[0], batches)
	require.Equal(t, -1, failedAt)
	require.NoError(t, err)
	for _, entry := range entries {
		got, err := lsm.Get(entry.Key)
		require.NoError(t, err)
		require.Equal(t, entry.Value, got.Value)
		got.DecrRef()
	}

	var batchRecords int
	var decoded int
	shard := lsm.shards[0]
	err = shard.wal.ReplaySegment(shard.memTable.segmentID, func(info wal.EntryInfo, payload []byte) error {
		if info.Type != wal.RecordTypeEntryBatch {
			return nil
		}
		batchRecords++
		entries, err := wal.DecodeEntryBatch(payload)
		if err != nil {
			return err
		}
		decoded += len(entries)
		for _, entry := range entries {
			entry.DecrRef()
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 1, batchRecords)
	require.Equal(t, 3, decoded)
}

func TestWriteBatchesWALFailureDoesNotApplyMemtable(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entry := newWritePipelineEntry("wal-fail", 1)
	defer entry.DecrRef()

	shard := lsm.shards[0]
	require.NoError(t, shard.wal.Close())
	failedAt, err := lsm.applyWriteBatches(lsm.shards[0], []*writeBatch{newTestWriteBatch(entry)})
	require.Equal(t, 0, failedAt)
	require.Error(t, err)

	got, err := shard.memTable.Get(entry.Key)
	require.NoError(t, err)
	require.Empty(t, got.Value)
	got.DecrRef()
}

func TestFitWritePrefixStopsAtRequestBoundary(t *testing.T) {
	entries := []*kv.Entry{
		newWritePipelineEntry("fit-a", 1),
		newWritePipelineEntry("fit-b", 2),
		newWritePipelineEntry("fit-c", 3),
	}
	defer func() {
		for _, entry := range entries {
			entry.DecrRef()
		}
	}()
	batches := []*writeBatch{
		newTestWriteBatch(entries[0]),
		newTestWriteBatch(entries[1]),
		newTestWriteBatch(entries[2]),
	}
	limit := estimatePipelineBatchWALSize(entries[:2])

	var mt memTable
	n, gotEntries, estimate, err := fitWritePrefix(&mt, limit, batches)
	require.NoError(t, err)
	require.Equal(t, 2, n)
	require.Len(t, gotEntries, 2)
	require.Equal(t, limit, estimate)
	require.Equal(t, entries[0], gotEntries[0])
	require.Equal(t, entries[1], gotEntries[1])
}

func TestWriteBatchesRotateOnlyBetweenRequests(t *testing.T) {
	entries := []*kv.Entry{
		newWritePipelineEntry("rotate-a", 1),
		newWritePipelineEntry("rotate-b", 2),
		newWritePipelineEntry("rotate-c", 3),
	}
	defer func() {
		for _, entry := range entries {
			entry.DecrRef()
		}
	}()
	limit := estimatePipelineBatchWALSize(entries[:2])
	dir := t.TempDir()
	wlog, err := wal.Open(wal.Config{Dir: dir})
	require.NoError(t, err)
	opts := newTestLSMOptions(dir, nil)
	opts.MemTableSize = limit
	lsm, err := NewLSM(opts, []*wal.Manager{wlog})
	require.NoError(t, err)
	defer func() { _ = lsm.Close() }()

	// Pin every WAL segment so the async flush worker, which fires after
	// the rotation triggered by entries[2], cannot race the Replay
	// assertion below by calling RemoveSegment on the rotated-out
	// segment. RetentionMark.FirstSegment is the *lowest* segment id this
	// participant still needs, so FirstSegment=1 keeps every segment.
	require.NoError(t, wlog.RegisterRetention("test-pin", func() wal.RetentionMark {
		return wal.RetentionMark{FirstSegment: 1}
	}))

	failedAt, err := lsm.applyWriteBatches(lsm.shards[0], []*writeBatch{
		newTestWriteBatch(entries[0]),
		newTestWriteBatch(entries[1]),
		newTestWriteBatch(entries[2]),
	})
	require.Equal(t, -1, failedAt)
	require.NoError(t, err)

	var batches int
	var decoded int
	err = lsm.shards[0].wal.Replay(func(info wal.EntryInfo, payload []byte) error {
		if info.Type != wal.RecordTypeEntryBatch {
			return nil
		}
		batches++
		entries, err := wal.DecodeEntryBatch(payload)
		if err != nil {
			return err
		}
		decoded += len(entries)
		for _, entry := range entries {
			entry.DecrRef()
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 2, batches)
	require.Equal(t, 3, decoded)
}

func TestLevelsRuntimeAdjustThrottleAndPointers(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	var events []WriteThrottleState
	lsm.throttleFn = func(state WriteThrottleState) {
		events = append(events, state)
	}

	// Force explicit thresholds so we can validate stop -> slowdown -> none.
	lsm.levels.opt.L0SlowdownWritesTrigger = 2
	lsm.levels.opt.L0StopWritesTrigger = 3
	lsm.levels.opt.L0ResumeWritesTrigger = 1
	lsm.levels.opt.CompactionSlowdownTrigger = 1000
	lsm.levels.opt.CompactionStopTrigger = 2000
	lsm.levels.opt.CompactionResumeTrigger = 500
	lsm.levels.opt.WriteThrottleMinRate = 64 << 20
	lsm.levels.opt.WriteThrottleMaxRate = 512 << 20
	l0 := lsm.levels.levels[0]
	l0.tables = []*table{{}, {}, {}}
	lsm.levels.adjustThrottle()
	if got := lsm.ThrottlePressurePermille(); got != 1000 {
		t.Fatalf("expected stop pressure=1000, got %d", got)
	}
	if got := lsm.ThrottleRateBytesPerSec(); got != 0 {
		t.Fatalf("expected stop rate=0, got %d", got)
	}
	l0.tables = []*table{{}, {}}
	lsm.levels.adjustThrottle()
	if got := lsm.ThrottlePressurePermille(); got == 0 || got >= 1000 {
		t.Fatalf("expected slowdown pressure in (0,1000), got %d", got)
	}
	if got := lsm.ThrottleRateBytesPerSec(); got == 0 {
		t.Fatalf("expected slowdown rate > 0")
	}
	l0.tables = nil
	lsm.levels.adjustThrottle()
	if got := lsm.ThrottlePressurePermille(); got != 0 {
		t.Fatalf("expected clear pressure=0, got %d", got)
	}
	if got := lsm.ThrottleRateBytesPerSec(); got != 0 {
		t.Fatalf("expected clear rate=0, got %d", got)
	}
	if len(events) != 3 ||
		events[0] != WriteThrottleStop ||
		events[1] != WriteThrottleSlowdown ||
		events[2] != WriteThrottleNone {
		t.Fatalf("unexpected throttle events: %+v", events)
	}

	// (removed: setLogPointer/logPointer were the in-memory cache for
	//  the legacy Version.LogSegment/LogOffset diagnostic fields.
	//  Recovery is per-shard via wal.Manager.Replay; the cache was dead.)

	lsm.levels.recordCompactionMetrics(5 * time.Millisecond)
	lastMs, maxMs, runs := lsm.levels.compactionDurations()
	if runs == 0 || lastMs <= 0 || maxMs <= 0 {
		t.Fatalf("unexpected compaction metrics: last=%f max=%f runs=%d", lastMs, maxMs, runs)
	}

	l0.tables = []*table{tablepkg.NewTestTable(tablepkg.TestTableSpec{MaxVersion: 7, KeyCount: 2})}
	if v := lsm.levels.maxVersion(); v != 7 {
		t.Fatalf("expected max version 7, got %d", v)
	}

	_ = lsm.levels.cacheMetrics()
}

func TestLevelHandlerOverlapAndMetrics(t *testing.T) {
	min := kv.InternalKey(kv.CFDefault, []byte("a"), 1)
	max := kv.InternalKey(kv.CFDefault, []byte("z"), 1)
	lh := &levelHandler{levelNum: 2}
	lh.tables = []*table{
		tablepkg.NewTestTable(tablepkg.TestTableSpec{MinKey: min, MaxKey: max}),
	}
	lh.landing.EnsureInit()
	lh.landing.Add(tablepkg.NewTestTable(tablepkg.TestTableSpec{
		MinKey:    kv.InternalKey(kv.CFDefault, []byte("k"), 1),
		MaxKey:    kv.InternalKey(kv.CFDefault, []byte("p"), 1),
		Size:      50,
		ValueSize: 20,
	}))

	lh.totalSize = 100
	lh.totalValueSize = 40
	lh.totalStaleSize = 10
	metrics := lh.metricsSnapshot()
	if metrics.ValueDensity <= 0 || metrics.LandingValueDensity <= 0 {
		t.Fatalf("expected non-zero density metrics")
	}

	tbl := tablepkg.NewTestTable(tablepkg.TestTableSpec{})
	tbl.SetTestBloomPresent(true)
	if !tbl.HasBloomFilter() {
		t.Fatalf("expected bloom filter to be reported")
	}
}

// TestCompact exercises L0->Lmax compaction.
func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()
	ok := false
	hasTable := func(lh *levelHandler, fid uint64) bool {
		if lh == nil {
			return false
		}
		lh.RLock()
		defer lh.RUnlock()
		for _, t := range lh.tables {
			if t.FID() == fid {
				return true
			}
		}
		for _, t := range lh.landing.AllTables() {
			if t.FID() == fid {
				return true
			}
		}
		return false
	}
	l0TOLMax := func() {
		// Ensure L0 accumulates enough tables to trigger the landing path. Newer Go versions
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
			before[tbl.FID()] = struct{}{}
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
		fid := lsm.levels.maxFID.Load() + 1
		cd := buildCompactDef(lsm, 0, 0, 0)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTablesL0ToL0(cd)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] lsm.levels.fillTablesL0ToL0(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// Clear global state to isolate downstream tests.
		require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
		require.NoError(t, err)
		ok = hasTable(lsm.levels.levels[0], fid)
		utils.CondPanic(!ok, fmt.Errorf("[l0ToL0] fid not found"))
	}
	nextCompact := func() {
		baseTest(t, lsm, 128)
		fid := lsm.levels.maxFID.Load() + 1
		cd := buildCompactDef(lsm, 0, 0, 1)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 0, *cd)
		// Clear global state to isolate downstream tests.
		require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
		require.NoError(t, err)
		ok = hasTable(lsm.levels.levels[1], fid)
		utils.CondPanic(!ok, fmt.Errorf("[nextCompact] fid not found"))
	}

	maxToMax := func() {
		baseTest(t, lsm, 128)
		prevMax := lsm.levels.maxFID.Load()
		cd := buildCompactDef(lsm, 6, 6, 6)
		// Use a test-only tweak to satisfy validation checks.
		tricky(cd.thisLevel.tablesSnapshot())
		ok := lsm.levels.fillTables(cd)
		if !ok && lsm.levels.levels[6].numLandingTables() > 0 {
			pri := plan.Priority{
				Level:       6,
				LandingMode: plan.LandingDrain,
				Target:      lsm.levels.levelTargets(),
				Score:       2,
				Adjusted:    2,
			}
			require.NoError(t, lsm.levels.doCompact(0, pri))
			tricky(cd.thisLevel.tablesSnapshot())
			ok = lsm.levels.fillTables(cd)
		}
		utils.CondPanic(!ok, fmt.Errorf("[maxToMax] lsm.levels.fillTables(cd) ret == false"))
		err := lsm.levels.runCompactDef(0, 6, *cd)
		// Clear global state to isolate downstream tests.
		require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
		require.NoError(t, err)
		ok = false
		if hasTable(lsm.levels.levels[6], prevMax+1) {
			ok = true
		} else {
			level := lsm.levels.levels[6]
			level.RLock()
			for _, tbl := range level.tables {
				if tbl.FID() > prevMax {
					ok = true
					break
				}
			}
			if !ok {
				for _, tbl := range level.landing.AllTables() {
					if tbl != nil && tbl.FID() > prevMax {
						ok = true
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
		wg.Go(func() {
			errCh <- lsm.levels.runCompactDef(0, 0, *cd)
		})
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

func TestLandingMergeStaysInLanding(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	// Generate enough data to create multiple L0 tables.
	baseTest(t, lsm, 256)

	// Move one L0 table to the max level landing buffer.
	l0 := lsm.levels.levels[0]
	tables := l0.tablesSnapshot()
	if len(tables) == 0 {
		t.Fatalf("expected L0 tables before landing merge test")
	}
	cd := buildCompactDef(lsm, 0, 0, 6)
	cd.top = []*table{tables[0]}
	cd.spec.ThisRange = getKeyRange(cd.top...)
	cd.spec.NextRange = cd.spec.ThisRange
	if err := lsm.levels.moveToLanding(cd); err != nil {
		t.Fatalf("moveToLanding: %v", err)
	}

	target := lsm.levels.levels[6]
	beforeLanding := target.numLandingTables()
	if beforeLanding == 0 {
		t.Fatalf("expected landing tables after moveToLanding")
	}
	beforeMain := target.numTables()

	pri := plan.Priority{
		Level:       6,
		Score:       5.0,
		Adjusted:    5.0,
		Target:      lsm.levels.levelTargets(),
		LandingMode: plan.LandingKeep,
	}
	if err := lsm.levels.doCompact(0, pri); err != nil {
		t.Fatalf("landing merge compact failed: %v", err)
	}

	afterLanding := target.numLandingTables()
	if afterLanding == 0 {
		t.Fatalf("expected landing tables to remain after merge")
	}
	if target.numTables() != beforeMain {
		t.Fatalf("main table count changed unexpectedly: before=%d after=%d", beforeMain, target.numTables())
	}
}

// Concurrent shard compaction should not violate compactState and should keep landing merge output in landing.
func TestLandingShardParallelSafety(t *testing.T) {
	clearDir()
	opt.NumCompactors = 4
	opt.LandingShardParallelism = 4
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	// Write enough data to spawn multiple L0 tables, then move to landing.
	for range 4 {
		baseTest(t, lsm, 512)
	}
	l0 := lsm.levels.levels[0]
	tables := l0.tablesSnapshot()
	if len(tables) == 0 {
		t.Fatalf("expected L0 tables for parallel landing test")
	}
	cd := buildCompactDef(lsm, 0, 0, 6)
	cd.top = []*table{tables[0]}
	cd.spec.ThisRange = getKeyRange(cd.top...)
	cd.spec.NextRange = cd.spec.ThisRange
	if err := lsm.levels.moveToLanding(cd); err != nil {
		t.Fatalf("moveToLanding: %v", err)
	}

	// Trigger parallel landing-only compactions across shards.
	pri := plan.Priority{
		Level:       6,
		Score:       6.0,
		Adjusted:    6.0,
		Target:      lsm.levels.levelTargets(),
		LandingMode: plan.LandingDrain,
	}
	if err := lsm.levels.doCompact(0, pri); err != nil {
		t.Fatalf("parallel landing compaction failed: %v", err)
	}

	// Ensure manifest/lists are consistent even if landing drained.
	target := lsm.levels.levels[6]
	_ = target.numLandingTables()

	// Simulate restart and ensure landing state can be recovered (may be empty if fully drained).
	require.NoError(t, lsm.Close())
	lsm = buildLSM()
	defer func() { _ = lsm.Close() }()
	_ = lsm.levels.levels[6].numLandingTables()
}

// baseTest performs correctness checks.
func baseTest(t *testing.T, lsm *LSM, n int) {
	// Tracking entry for debugging.
	e := kv.NewInternalEntry(kv.CFDefault, []byte("CRTS😁NoKVMrGSBtL"), kv.MaxVersion, []byte("debug-tracker"), 0, 123)
	defer e.DecrRef()
	//caseList := make([]*kv.Entry, 0)
	//caseList = append(caseList, e)

	// Randomized data to exercise write paths.
	require.NoError(t, lsm.Set(e))
	for i := 1; i < n; i++ {
		ee := buildInternalTestEntry()
		defer ee.DecrRef()
		require.NoError(t, lsm.Set(ee))
		// caseList = append(caseList, ee)
	}
	// Read back from the levels.
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
	// Verified bounded range-scans in TestLSMBoundedRangeMultiLevel.
}

// buildLSM is the test harness helper.
func buildLSM() *LSM {
	// init DB Basic Test
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	if err != nil {
		panic(err)
	}
	lsm, err := NewLSM(opt, []*wal.Manager{wlog})
	if err != nil {
		panic(err)
	}
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
	t := plan.Targets{
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
		spec: plan.Plan{
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
func buildCompactionPriority(lsm *LSM, thisLevel int, t plan.Targets) plan.Priority {
	return plan.Priority{
		Level:    thisLevel,
		Score:    8.6,
		Adjusted: 860,
		Target:   t,
	}
}

func tricky(tables []*table) {
	// Use a test-only tweak to satisfy validation checks across branches.
	for _, table := range tables {
		table.SetTestStaleDataSize(uint32(10 << 20))
		t, _ := time.Parse("2006-01-02 15:04:05", "1995-08-10 00:00:00")
		table.SetTestCreatedAt(t)
	}
}

func waitForL0(t *testing.T, lsm *LSM) {
	waitForL0Tables(t, lsm, 1)
}

func waitForL0Tables(t *testing.T, lsm *LSM, atLeast int) {
	t.Helper()
	if atLeast <= 0 {
		atLeast = 1
	}
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if lsm.FlushPending() == 0 && lsm.levels.levels[0].numTables() >= atLeast {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for L0 table (pending=%d tables=%d, need>=%d)",
		lsm.FlushPending(), lsm.levels.levels[0].numTables(), atLeast)
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

func TestImportExternalSST(t *testing.T) {
	workDir, err := os.MkdirTemp("", "nokv-import-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(workDir)) }()

	opt := &Options{
		WorkDir:       workDir,
		MemTableSize:  1024 * 1024,
		SSTableMaxSz:  1024 * 1024,
		BlockSize:     4096,
		NumCompactors: 1,
		BaseLevelSize: 1024 * 1024,
		MaxLevelNum:   7,
		FS:            vfs.OSFS{},
	}
	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	testFilePath := opt.WorkDir + "/99999.sst"
	builder := tablepkg.NewBuilder(tableOptionsFor(opt))
	builder.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, []byte("key"), 1),
		Value: []byte("value"),
	})
	testTable, err := builder.Flush(lsm.levels, testFilePath)
	if err != nil {
		t.Fatalf("Failed to build SST file: %v", err)
	}
	require.NoError(t, testTable.CloseHandle())
	builder.Close()

	_, err = lsm.ImportExternalSST([]string{testFilePath})
	require.NoError(t, err)
	entry, err := lsm.Get(kv.InternalKey(kv.CFDefault, []byte("key"), 1))
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("value"), entry.Value)
	entry.DecrRef()
}

func TestImportExternalSSTValidationFailure(t *testing.T) {
	workDir, err := os.MkdirTemp("", "nokv-import-validation-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(workDir)) }()

	opt := &Options{
		WorkDir:       workDir,
		MemTableSize:  1024 * 1024,
		SSTableMaxSz:  1024 * 1024,
		BlockSize:     4096,
		NumCompactors: 1,
		BaseLevelSize: 1024 * 1024,
		MaxLevelNum:   7,
		FS:            vfs.OSFS{},
	}
	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	// Test 1: Import file without .sst suffix (invalid file type)
	nonSSTFile := workDir + "/99999.txt"
	require.NoError(t, os.WriteFile(nonSSTFile, []byte("test"), 0644))
	_, err = lsm.ImportExternalSST([]string{nonSSTFile})
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing .sst suffix")

	// Test 2: Import directory path (not a file)
	dirPath := workDir + "/test_dir"
	require.NoError(t, os.Mkdir(dirPath, 0755))
	_, err = lsm.ImportExternalSST([]string{dirPath})
	require.Error(t, err)
	require.Contains(t, err.Error(), "is a directory")

	// Test 3: Import non-existent SST file
	nonExistFile := workDir + "/99998.sst"
	_, err = lsm.ImportExternalSST([]string{nonExistFile})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid external SST")

	// Test 4: Import multiple SSTs with overlapping key ranges
	sst1Path := workDir + "/99997.sst"
	builder1 := tablepkg.NewBuilder(tableOptionsFor(opt))
	builder1.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, []byte("a"), 1),
		Value: []byte("val1"),
	})
	tbl1, err := builder1.Flush(lsm.levels, sst1Path)
	require.NoError(t, err)
	require.NoError(t, tbl1.CloseHandle())
	builder1.Close()

	sst2Path := workDir + "/99996.sst"
	builder2 := tablepkg.NewBuilder(tableOptionsFor(opt))
	builder2.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, []byte("a"), 2),
		Value: []byte("val2"),
	})
	tbl2, err := builder2.Flush(lsm.levels, sst2Path)
	require.NoError(t, err)
	require.NoError(t, tbl2.CloseHandle())
	builder2.Close()

	_, err = lsm.ImportExternalSST([]string{sst1Path, sst2Path})
	require.Error(t, err)
	require.Contains(t, err.Error(), "imported SSTs have key range overlap")

	// Test 5: Verify valid non-overlapping SST can be imported successfully
	validSSTPath := workDir + "/99995.sst"
	builderValid := tablepkg.NewBuilder(tableOptionsFor(opt))
	builderValid.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, []byte("b"), 1),
		Value: []byte("valid"),
	})
	tblValid, err := builderValid.Flush(lsm.levels, validSSTPath)
	require.NoError(t, err)
	require.NoError(t, tblValid.CloseHandle())
	builderValid.Close()
	_, err = lsm.ImportExternalSST([]string{validSSTPath})
	require.NoError(t, err)
	entry, err := lsm.Get(kv.InternalKey(kv.CFDefault, []byte("b"), 1))
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("valid"), entry.Value)
	entry.DecrRef()

	// Test 6: Import SST that overlaps with existing L0 table
	overlapSSTPath := workDir + "/99994.sst"
	builderOverlap := tablepkg.NewBuilder(tableOptionsFor(opt))
	builderOverlap.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, []byte("b"), 2),
		Value: []byte("overlap"),
	})
	tblOverlap, err := builderOverlap.Flush(lsm.levels, overlapSSTPath)
	require.NoError(t, err)
	require.NoError(t, tblOverlap.CloseHandle())
	builderOverlap.Close()

	_, err = lsm.ImportExternalSST([]string{overlapSSTPath})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps with L0 existing table")
}

func TestImportExternalSSTAtomicityOnManifestWriteFailure(t *testing.T) {
	workDir, err := os.MkdirTemp("", "nokv-import-atomicity-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(workDir)) }()

	faultPolicy := vfs.NewFaultPolicy(
		vfs.FailOnceRule(vfs.OpFileSync, fmt.Sprintf("%s/MANIFEST-000001", workDir), errors.New("manifest write failed")),
	)
	faultFS := vfs.NewFaultFSWithPolicy(vfs.OSFS{}, faultPolicy)

	opt := &Options{
		WorkDir:       workDir,
		MemTableSize:  1024 * 1024,
		SSTableMaxSz:  1024 * 1024,
		BlockSize:     4096,
		NumCompactors: 1,
		BaseLevelSize: 1024 * 1024,
		MaxLevelNum:   7,
		FS:            faultFS,
		ManifestSync:  true,
	}
	lsm1 := buildTestLSM(t, opt)
	shouldCloseLsm1 := true
	defer func() {
		if shouldCloseLsm1 {
			require.NoError(t, lsm1.Close())
		}
	}()

	testSSTPath := workDir + "/99999.sst"
	builder := tablepkg.NewBuilder(tableOptionsFor(opt))
	builder.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, []byte("key"), 1),
		Value: []byte("value"),
	})
	tbl, err := builder.Flush(lsm1.levels, testSSTPath)
	require.NoError(t, err)
	require.NoError(t, tbl.CloseHandle())
	builder.Close()

	_, err = lsm1.ImportExternalSST([]string{testSSTPath})
	require.Error(t, err)
	require.Contains(t, err.Error(), "log manifest edits failed")

	// Verify original SST file still exists
	_, err = os.Stat(testSSTPath)
	require.NoError(t, err)

	// Verify no temporary SST file was left behind
	tempFID := lsm1.levels.maxFID.Load()
	tempSSTPath := vfs.FileNameSSTable(workDir, tempFID)
	_, err = os.Stat(tempSSTPath)
	require.True(t, os.IsNotExist(err))

	// Verify imported key is not accessible (import rolled back completely)
	entry, err := lsm1.Get(kv.InternalKey(kv.CFDefault, []byte("key"), 1))
	require.Nil(t, entry)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	require.NoError(t, lsm1.Close())
	shouldCloseLsm1 = false
	lsm2 := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm2.Close()) }()

	// Verify key remains inaccessible after LSM reinitialization
	entry, err = lsm2.Get(kv.InternalKey(kv.CFDefault, []byte("key"), 1))
	require.Nil(t, entry)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)

	// Verify SST can be imported successfully after crash recovery
	_, err = lsm2.ImportExternalSST([]string{testSSTPath})
	require.NoError(t, err)
	entry, err = lsm2.Get(kv.InternalKey(kv.CFDefault, []byte("key"), 1))
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("value"), entry.Value)
	entry.DecrRef()
}

func TestImportExternalSSTIdempotency(t *testing.T) {
	workDir, err := os.MkdirTemp("", "nokv-import-idempotency-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(workDir)) }()

	opt := &Options{
		WorkDir:       workDir,
		MemTableSize:  1024 * 1024,
		SSTableMaxSz:  1024 * 1024,
		BlockSize:     4096,
		NumCompactors: 1,
		BaseLevelSize: 1024 * 1024,
		MaxLevelNum:   7,
		FS:            vfs.OSFS{},
	}
	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	testSSTPath := workDir + "/99999.sst"
	builder := tablepkg.NewBuilder(tableOptionsFor(opt))
	testKey := []byte("key")
	builder.AddKey(&kv.Entry{
		Key:   kv.InternalKey(kv.CFDefault, testKey, 1),
		Value: []byte("value"),
	})
	tbl, err := builder.Flush(lsm.levels, testSSTPath)
	require.NoError(t, err)
	require.NoError(t, tbl.CloseHandle())
	builder.Close()

	// Test 1: First import should succeed and key should be accessible
	_, err = lsm.ImportExternalSST([]string{testSSTPath})
	require.NoError(t, err)

	entry, err := lsm.Get(kv.InternalKey(kv.CFDefault, testKey, 1))
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("value"), entry.Value)
	entry.DecrRef()

	// Test 2: Re-importing the same SST file should fail
	_, err = lsm.ImportExternalSST([]string{testSSTPath})
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid external SST")

	// Test 3: Importing duplicate SST (same content) should fail due to key overlap
	dupSSTPath := workDir + "/99998.sst"
	importedSSTPath := vfs.FileNameSSTable(workDir, lsm.levels.maxFID.Load())
	content, err := os.ReadFile(importedSSTPath)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(dupSSTPath, content, 0644))

	_, err = lsm.ImportExternalSST([]string{dupSSTPath})
	require.Error(t, err)
	require.Contains(t, err.Error(), "overlaps with L0 existing table")
}

// -----------------------------------------------------------------------------
// Negative-cache integration tests (folded in from the now-deleted
// engine/lsm/negative_cache_test.go after the cache itself moved to
// engine/slab/negativecache/. These tests verify the LSM's USE of the
// cache — miss-path Remember in lsm.Get, Invalidate on Set, range-
// tombstone interaction, Clear semantics, and the persistence wiring
// through Open/Close. Pure cache semantics live in
// engine/slab/negativecache/cache_test.go.)
// -----------------------------------------------------------------------------

func TestNegativeCacheRemembersAndInvalidatesMiss(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	query := kv.InternalKey(kv.CFDefault, []byte("negative-key"), kv.MaxVersion)
	got, err := lsm.Get(query)
	require.Nil(t, got)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.True(t, lsm.negativeHit(query))

	entry := kv.NewInternalEntry(kv.CFDefault, []byte("negative-key"), 1, []byte("visible"), 0, 0)
	_, err = lsm.SetBatchGroup(2, [][]*kv.Entry{{entry}})
	require.NoError(t, err)
	require.False(t, lsm.negativeHit(query))

	got, err = lsm.Get(query)
	require.NoError(t, err)
	require.Equal(t, []byte("visible"), got.Value)
	got.DecrRef()
}

func TestNegativeCacheKeysIncludeReadVersion(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	userKey := []byte("versioned-negative")
	missAtOne := kv.InternalKey(kv.CFDefault, userKey, 1)
	missAtTwo := kv.InternalKey(kv.CFDefault, userKey, 2)

	_, err := lsm.Get(missAtOne)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.True(t, lsm.negativeHit(missAtOne))
	require.False(t, lsm.negativeHit(missAtTwo))
}

func TestNegativeCacheDisabledByRangeTombstones(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	rt := kv.NewEntry(kv.InternalKey(kv.CFDefault, []byte("a"), 10), []byte("z"))
	rt.Meta = kv.BitRangeDelete
	_, err := lsm.SetBatchGroup(1, [][]*kv.Entry{{rt}})
	require.NoError(t, err)

	query := kv.InternalKey(kv.CFDefault, []byte("m"), kv.MaxVersion)
	got, err := lsm.Get(query)
	require.Nil(t, got)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.False(t, lsm.negativeHit(query))
}

func TestNegativeCacheClearDropsRememberedMisses(t *testing.T) {
	lsm, wals := openShardHintTestLSM(t, 4)
	defer closeShardHintTestLSM(t, lsm, wals)

	query := kv.InternalKey(kv.CFDefault, []byte("clear-negative"), kv.MaxVersion)
	_, err := lsm.Get(query)
	require.ErrorIs(t, err, utils.ErrKeyNotFound)
	require.True(t, lsm.negativeHit(query))

	lsm.clearNegativeCache()
	require.False(t, lsm.negativeHit(query))
}

// TestNegativeCachePersistsAcrossOpen exercises Phase 3 of the slab
// substrate redesign: with NegativeCachePersistent enabled, a process
// restart should replay the slab snapshot back into the in-memory cache
// so previously-known not-found keys do not have to re-warm via the LSM.
func TestNegativeCachePersistsAcrossOpen(t *testing.T) {
	dir := t.TempDir()

	queries := [][]byte{
		kv.InternalKey(kv.CFDefault, []byte("missing-1"), kv.MaxVersion),
		kv.InternalKey(kv.CFDefault, []byte("missing-2"), kv.MaxVersion),
		kv.InternalKey(kv.CFDefault, []byte("missing-3"), kv.MaxVersion),
	}

	openLSM := func() (*LSM, []*walManagerHandle) {
		t.Helper()
		opts := newTestLSMOptions(dir, nil)
		opts.NegativeCachePersistent = true
		opts.NegativeCacheSlabMaxSize = 1 << 20
		walDir := filepath.Join(dir, "wal-00")
		require.NoError(t, os.MkdirAll(walDir, 0o755))
		mgr, err := wal.Open(wal.Config{Dir: walDir})
		require.NoError(t, err)
		lsm, err := NewLSM(opts, []*wal.Manager{mgr})
		require.NoError(t, err)
		return lsm, []*walManagerHandle{{mgr: mgr}}
	}
	closeLSM := func(lsm *LSM, wals []*walManagerHandle) {
		t.Helper()
		require.NoError(t, lsm.Close())
		for _, w := range wals {
			require.NoError(t, w.mgr.Close())
		}
	}

	lsm1, wals1 := openLSM()
	for _, q := range queries {
		_, err := lsm1.Get(q)
		require.ErrorIs(t, err, utils.ErrKeyNotFound)
		require.True(t, lsm1.negativeHit(q))
	}
	closeLSM(lsm1, wals1)

	_, err := os.Stat(filepath.Join(dir, "negative-slab", "negative.slab"))
	require.NoError(t, err, "snapshot file should exist after Close")

	lsm2, wals2 := openLSM()
	defer closeLSM(lsm2, wals2)
	for _, q := range queries {
		require.True(t, lsm2.negativeHit(q),
			"key %q should be warm in negative cache after restore", q)
	}
}

type walManagerHandle struct {
	mgr *wal.Manager
}

// TestLSMBoundedRangeMultiLevel exercises bounded merge iteration with data
// interleaved across physical tiers: even keys in the deepest level, odd keys
// one level above, newer versions of a subset in L0, and the newest writes in
// the mutable MemTable. A range tombstone hides a slice across multiple tiers.
//
// Invariants protected:
//   - MergeIterator yields correct ascending/descending order when keys from
//     different tiers interleave (even/odd alternation).
//   - LowerBound/UpperBound correctly restrict the visible key range.
//   - Range tombstones hide covered keys regardless of which tier they reside in.
//   - Newer versions in upper tiers shadow older versions in lower tiers.
func TestLSMBoundedRangeMultiLevel(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { require.NoError(t, lsm.Close()) }()

	maxLevel := lsm.option.MaxLevelNum - 1

	// compactL0To forces a compaction from L0 to the specified level.
	// This gives us deterministic control over which physical tier data lands in.
	compactL0To := func(level int) {
		t.Helper()
		cd := buildCompactDef(lsm, 0, 0, level)
		if ok := lsm.levels.fillTables(cd); !ok {
			t.Fatalf("expected L0->L%d compaction plan", level)
		}
		if err := lsm.levels.runCompactDef(0, 0, *cd); err != nil {
			t.Fatalf("runCompactDef L0->L%d: %v", level, err)
		}
		require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	}

	// --- Even keys (key00, key02, ..., key98) @ version 10 -> maxLevel ---
	for i := 0; i < 100; i += 2 {
		k := fmt.Appendf(nil, "key%02d", i)
		e := kv.NewInternalEntry(kv.CFDefault, k, 10, []byte("even"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)
	compactL0To(maxLevel)

	// --- Odd keys (key01, key03, ..., key99) @ version 20 -> maxLevel-1 ---
	for i := 1; i < 100; i += 2 {
		k := fmt.Appendf(nil, "key%02d", i)
		e := kv.NewInternalEntry(kv.CFDefault, k, 20, []byte("odd"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)
	compactL0To(maxLevel - 1)

	// --- Newer versions of key50~key59 @ version 30 -> L0 ---
	for i := 50; i < 60; i++ {
		k := fmt.Appendf(nil, "key%02d", i)
		e := kv.NewInternalEntry(kv.CFDefault, k, 30, []byte("l0-new"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)

	// --- Newest versions of key90~key99 @ version 40 -> mutable MemTable ---
	for i := 90; i < 100; i++ {
		k := fmt.Appendf(nil, "key%02d", i)
		e := kv.NewInternalEntry(kv.CFDefault, k, 40, []byte("mem-new"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Range tombstone [key30, key50) @ version 50.
	// Hides key30~key49 across both maxLevel (even) and maxLevel-1 (odd).
	rtEntry := kv.NewInternalEntry(kv.CFDefault, []byte("key30"), 50, []byte("key50"), kv.BitRangeDelete, 0)
	require.NoError(t, lsm.Set(rtEntry))
	rtEntry.DecrRef()

	rtv := lsm.PinRangeTombstoneView()
	defer rtv.Close()

	// Verify tombstone coverage boundaries.
	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key30"), 10))
	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key49"), 20))
	require.False(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key29"), 10))
	require.False(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key50"), 30))

	// Bounded ascending scan [key10, key95).
	lower := []byte("key10")
	upper := []byte("key95")

	iters := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { require.NoError(t, mit.Close()) }()

	var resultsAsc []string
	for mit.Rewind(); mit.Valid(); mit.Next() {
		e := mit.Item().Entry()
		userKey := splitIterUserKey(t, e.Key)
		if bytes.Compare(userKey, upper) >= 0 {
			break
		}
		if bytes.Compare(userKey, lower) < 0 {
			continue
		}
		if e.IsRangeDelete() {
			continue
		}
		if rtv.IsKeyCovered(kv.CFDefault, userKey, e.Version) {
			continue
		}
		if len(resultsAsc) > 0 && resultsAsc[len(resultsAsc)-1] == string(userKey) {
			continue
		}
		resultsAsc = append(resultsAsc, string(userKey))
	}

	// Expected: key10~key29 (interleaved even/odd), key30~key49 hidden,
	// key50~key94 visible (with newer versions from L0/Mem shadowing old ones).
	var expectAsc []string
	for i := 10; i < 30; i++ {
		expectAsc = append(expectAsc, fmt.Sprintf("key%02d", i))
	}
	for i := 50; i < 95; i++ {
		expectAsc = append(expectAsc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectAsc, resultsAsc)

	// Bounded descending scan [key10, key95).
	itersDesc := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: false})
	mitDesc := NewMergeIterator(itersDesc, true)
	defer func() { require.NoError(t, mitDesc.Close()) }()

	var resultsDesc []string
	for mitDesc.Rewind(); mitDesc.Valid(); mitDesc.Next() {
		e := mitDesc.Item().Entry()
		userKey := splitIterUserKey(t, e.Key)
		if bytes.Compare(userKey, lower) < 0 {
			break
		}
		if bytes.Compare(userKey, upper) >= 0 {
			continue
		}
		if e.IsRangeDelete() {
			continue
		}
		if rtv.IsKeyCovered(kv.CFDefault, userKey, e.Version) {
			continue
		}
		if len(resultsDesc) > 0 && resultsDesc[len(resultsDesc)-1] == string(userKey) {
			continue
		}
		resultsDesc = append(resultsDesc, string(userKey))
	}

	var expectDesc []string
	for i := 94; i >= 50; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	for i := 29; i >= 10; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectDesc, resultsDesc)
}

// TestLSMBoundedRangeSeek verifies Seek behavior on the raw MergeIterator.
// Invariant: Seek positions the iterator at the first element >= target key.
// Bounds enforcement is at the runtime/DBIterator layer, not the raw iterator.
func TestLSMBoundedRangeSeek(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { require.NoError(t, lsm.Close()) }()

	for i := 10; i <= 20; i++ {
		k := fmt.Appendf(nil, "key%02d", i)
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, fmt.Appendf(nil, "val%02d", i), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	iters := lsm.NewIterators(&index.Options{LowerBound: []byte("key12"), UpperBound: []byte("key18"), IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { require.NoError(t, mit.Close()) }()

	// Seek below data range — lands on first key (>= semantic).
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key09"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key10", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek inside bounds.
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key15"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key15", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek to last key.
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key20"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key20", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek beyond all data — iterator exhausted.
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key21"), kv.MaxVersion))
	require.False(t, mit.Valid())
}

// TestLSMBoundedRangeEmptyResult verifies that scanning a range with no
// matching keys yields zero results without panics.
// Invariant: Out-of-data-range bounds produce an empty result set cleanly.
func TestLSMBoundedRangeEmptyResult(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { require.NoError(t, lsm.Close()) }()

	const n = 9
	for i := range n {
		k := fmt.Appendf(nil, "key%02d", i)
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, []byte("val"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Data is key00~key08. Test empty ranges at different positions.
	emptyRanges := []struct {
		lower []byte
		upper []byte
	}{
		{[]byte("key09"), []byte("key19")}, // after all data
		{[]byte("key03"), []byte("key03")}, // zero-width range
	}

	for _, r := range emptyRanges {
		iters := lsm.NewIterators(&index.Options{LowerBound: r.lower, UpperBound: r.upper, IsAsc: true})
		mit := NewMergeIterator(iters, false)

		count := 0
		for mit.Rewind(); mit.Valid(); mit.Next() {
			userKey := splitIterUserKey(t, mit.Item().Entry().Key)
			if bytes.Compare(userKey, r.lower) >= 0 &&
				bytes.Compare(userKey, r.upper) < 0 {
				count++
			}
		}
		require.Equal(t, 0, count, "range [%s, %s) should be empty", r.lower, r.upper)
		require.NoError(t, mit.Close())
	}
}

// ---- Recovery tests (merged from lsm_recovery_test.go) ----
//
// These tests exercise LSM recovery semantics by injecting manifest
// corruption and re-opening the engine, verifying restart behavior.

// TestBaseManifest validates manifest integrity across restarts.
func TestBaseManifest(t *testing.T) {
	clearDir()
	recovery := func() {
		// Each run simulates an unexpected restart.
		lsm := buildLSM()
		// Validate correctness after recovery.
		baseTest(t, lsm, 128)
		_ = lsm.Close()
	}
	// Run the closure multiple times to exercise recovery.
	runTest(5, recovery)
}

func TestManifestMagic(t *testing.T) {
	helpTestManifestFileCorruption(t, 3, "bad magic")
}

func TestManifestVersion(t *testing.T) {
	helpTestManifestFileCorruption(t, 4, "")
}

func TestManifestChecksum(t *testing.T) {
	helpTestManifestFileCorruption(t, 15, "")
}

func helpTestManifestFileCorruption(t *testing.T, off int64, errorContent string) {
	clearDir()
	// Create the LSM and close it to generate a manifest.
	{
		lsm := buildLSM()
		require.NoError(t, lsm.Close())
	}
	currentData, err := os.ReadFile(filepath.Join(opt.WorkDir, "CURRENT"))
	require.NoError(t, err)
	manifestName := strings.TrimSpace(string(currentData))
	fp, err := os.OpenFile(filepath.Join(opt.WorkDir, manifestName), os.O_RDWR, 0)
	require.NoError(t, err)
	// Inject a bad byte at the given offset.
	_, err = fp.WriteAt([]byte{'X'}, off)
	require.NoError(t, err)
	require.NoError(t, fp.Close())
	defer func() {
		if err := recover(); err != nil && errorContent != "" {
			require.Contains(t, err.(error).Error(), errorContent)
		}
	}()
	// Re-open LSM; it should panic on corruption.
	lsm := buildLSM()
	require.NoError(t, lsm.Close())
}

// buildTestLSM is the canonical helper for tests that need a live LSM with a
// fresh WAL Manager. Used by iterator/external_sst/level_handler tests.
func buildTestLSM(t *testing.T, opt *Options) *LSM {
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	require.NoError(t, err)
	lsm, err := NewLSM(opt, []*wal.Manager{wlog})
	require.NoError(t, err)
	return lsm
}
