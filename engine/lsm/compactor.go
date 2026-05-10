package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/iterator"
	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/feichai0017/NoKV/engine/lsm/tombstone"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"github.com/feichai0017/NoKV/utils"
)

// compactionMetrics holds per-compaction-run duration counters that drive
// the compaction-related fields of the LSM Diagnostics snapshot.
type compactionMetrics struct {
	LastNs atomic.Int64
	MaxNs  atomic.Int64
	Runs   atomic.Uint64
}

// compactor owns all compaction state — the planner conflict-state, the
// write-rate pacer, the per-run metrics, and the scheduler that drives the
// worker pool. It holds a back-reference to its levelManager because
// compaction must read and mutate level state.
type compactor struct {
	lm      *levelManager
	state   *plan.State
	pacer   *pacer.Pacer
	metrics compactionMetrics
	sched   *scheduler
}

func newCompactor(lm *levelManager, opt *Options) *compactor {
	c := &compactor{
		lm:    lm,
		state: plan.NewState(opt.MaxLevelNum),
		pacer: pacer.New(opt.CompactionWriteBytesPerSec),
	}
	c.sched = newScheduler(c, opt.NumCompactors, opt.CompactionPolicy, lm.getLogger())
	return c
}

// === metrics (compaction-run counters) ===

func (c *compactor) priorityStats() (int64, float64) {
	if c == nil {
		return 0, 0
	}
	prios := c.pickCompactLevels()
	var max float64
	for _, p := range prios {
		if p.Adjusted > max {
			max = p.Adjusted
		}
	}
	return int64(len(prios)), max
}

func (c *compactor) runDurations() (float64, float64, uint64) {
	if c == nil {
		return 0, 0, 0
	}
	lastNs := c.metrics.LastNs.Load()
	maxNs := c.metrics.MaxNs.Load()
	runs := c.metrics.Runs.Load()
	return float64(lastNs) / 1e6, float64(maxNs) / 1e6, runs
}

func (c *compactor) recordRun(duration time.Duration) {
	c.metrics.Runs.Add(1)
	last := duration.Nanoseconds()
	c.metrics.LastNs.Store(last)
	for {
		prev := c.metrics.MaxNs.Load()
		if last <= prev {
			break
		}
		if c.metrics.MaxNs.CompareAndSwap(prev, last) {
			break
		}
	}
}

// === picker glue ===

// needsCompaction reports whether any level currently exceeds compaction thresholds.
func (c *compactor) needsCompaction() bool {
	return len(c.pickCompactLevels()) > 0
}

// pickCompactLevels collects per-level compaction inputs from live state and
// asks the plan picker for a priority-ordered candidate list.
func (c *compactor) pickCompactLevels() []plan.Priority {
	if c == nil || c.lm.opt == nil {
		return nil
	}
	levels := make([]plan.LevelInput, len(c.lm.levels))
	for i, lvl := range c.lm.levels {
		if lvl == nil {
			continue
		}
		li := plan.LevelInput{
			Level:               i,
			NumTables:           lvl.numTables(),
			TotalSize:           lvl.getTotalSize(),
			TotalValueBytes:     lvl.getTotalValueSize(),
			MainValueBytes:      lvl.mainValueBytes(),
			LandingTables:       lvl.numLandingTables(),
			LandingSize:         lvl.landingDataSize(),
			LandingValueBytes:   lvl.landingValueBytes(),
			LandingValueDensity: lvl.landingValueDensity(),
			LandingAgeSeconds:   lvl.maxLandingAgeSeconds(),
			KeyCount:            lvl.keyCount(),
			RangeTombstones:     lvl.rangeTombstoneCount(),
		}
		if c.state != nil {
			li.DelSize = c.state.DelSize(i)
		}
		levels[i] = li
	}
	if len(levels) == 0 {
		return nil
	}
	return plan.PickPriorities(plan.PickerInput{
		Levels:                    levels,
		Targets:                   c.levelTargets(),
		NumLevelZeroTables:        c.lm.opt.NumLevelZeroTables,
		BaseTableSize:             c.lm.opt.BaseTableSize,
		BaseLevelSize:             c.lm.opt.BaseLevelSize,
		LandingBacklogMergeScore:  c.lm.opt.LandingBacklogMergeScore,
		CompactionValueWeight:     c.lm.opt.CompactionValueWeight,
		CompactionTombstoneWeight: c.lm.opt.CompactionTombstoneWeight,
	})
}

// levelTargets builds the per-level compaction size targets from the
// current level sizes and option knobs.
func (c *compactor) levelTargets() plan.Targets {
	if c == nil || c.lm.opt == nil || len(c.lm.levels) == 0 {
		return plan.Targets{}
	}
	sizes := make([]int64, len(c.lm.levels))
	for i, lvl := range c.lm.levels {
		if lvl == nil {
			continue
		}
		sizes[i] = lvl.getTotalSize()
	}
	return plan.BuildTargets(sizes, plan.TargetOptions{
		BaseLevelSize:       c.lm.opt.BaseLevelSize,
		LevelSizeMultiplier: c.lm.opt.LevelSizeMultiplier,
		BaseTableSize:       c.lm.opt.BaseTableSize,
		TableSizeMultiplier: c.lm.opt.TableSizeMultiplier,
		MemTableSize:        c.lm.opt.MemTableSize,
	})
}

// === pacer accessors ===

// pacerForBuild returns the active pacer for a new build, or nil
// when pacing is disabled or L0 backlog suggests bypass is appropriate.
//
// L0 bypass:
//
//	When L0 table count crosses CompactionPacingBypassL0, the next compaction
//	build skips pacing entirely. The reasoning: if L0 is approaching stall,
//	the foreground latency cost of unpaced compaction is cheaper than the
//	write stall a paced compaction would not prevent. Bypass is decided at
//	build start; an in-progress compaction does not switch mid-flight, which
//	keeps the per-block charge() hot path branch-free past the nil check.
func (c *compactor) pacerForBuild() *pacer.Pacer {
	if c == nil || c.pacer == nil {
		return nil
	}
	if c.pacerBypassActive() {
		return nil
	}
	return c.pacer
}

// pacerBypassActive reports whether L0 has reached the configured
// bypass threshold. When true, new compaction builds run unpaced so that L0
// can be drained quickly enough to avoid foreground write stalls.
func (c *compactor) pacerBypassActive() bool {
	if c == nil || c.lm.opt == nil || c.lm.opt.CompactionPacingBypassL0 <= 0 || len(c.lm.levels) == 0 || c.lm.levels[0] == nil {
		return false
	}
	return c.lm.levels[0].numTables() >= c.lm.opt.CompactionPacingBypassL0
}

// === throttle decider ===

// adjustThrottle updates write admission state using a two-stage model:
// slowdown (pace writes) and stop (block writes). Hysteresis is applied to
// avoid oscillation under heavy compaction pressure.
func (c *compactor) adjustThrottle() {
	if c == nil || c.lm.lsm == nil || len(c.lm.levels) == 0 {
		return
	}
	l0Tables := c.lm.levels[0].numTables()
	_, maxScore := c.priorityStats()

	l0Slow := c.lm.opt.L0SlowdownWritesTrigger
	l0Stop := c.lm.opt.L0StopWritesTrigger
	l0Resume := c.lm.opt.L0ResumeWritesTrigger

	scoreSlow := c.lm.opt.CompactionSlowdownTrigger
	scoreStop := c.lm.opt.CompactionStopTrigger
	scoreResume := c.lm.opt.CompactionResumeTrigger

	stopCond := l0Tables >= l0Stop
	slowCond := l0Tables >= l0Slow || maxScore >= scoreSlow
	resumeCond := l0Tables <= l0Resume && maxScore <= scoreResume

	cur := c.lm.lsm.ThrottleState()
	target := cur
	switch cur {
	case WriteThrottleStop:
		if stopCond {
			target = WriteThrottleStop
		} else if slowCond {
			target = WriteThrottleSlowdown
		} else if resumeCond {
			target = WriteThrottleNone
		}
	case WriteThrottleSlowdown:
		if stopCond {
			target = WriteThrottleStop
		} else if resumeCond {
			target = WriteThrottleNone
		}
	default:
		if stopCond {
			target = WriteThrottleStop
		} else if slowCond {
			target = WriteThrottleSlowdown
		} else {
			target = WriteThrottleNone
		}
	}
	l0Pressure := normalizedThrottlePressure(float64(l0Tables), float64(l0Slow), float64(l0Stop))
	scorePressure := normalizedThrottlePressure(maxScore, scoreSlow, scoreStop)
	pressure := max(l0Pressure, scorePressure)
	switch target {
	case WriteThrottleNone:
		pressure = 0
	case WriteThrottleStop:
		pressure = 1000
	case WriteThrottleSlowdown:
		if pressure == 0 {
			pressure = 1
		}
	}
	rate := uint64(0)
	if target == WriteThrottleSlowdown {
		rate = throttleRateForPressure(
			uint32(pressure),
			c.lm.opt.WriteThrottleMinRate,
			c.lm.opt.WriteThrottleMaxRate,
		)
	}
	c.lm.lsm.throttle.Apply(target, uint32(pressure), rate)
}

// === throttle helpers (free funcs) ===

func normalizedThrottlePressure(value, slowdown, stop float64) int {
	if stop <= slowdown {
		if value >= stop {
			return 1000
		}
		return 0
	}
	if value <= slowdown {
		return 0
	}
	if value >= stop {
		return 1000
	}
	ratio := (value - slowdown) / (stop - slowdown)
	if ratio <= 0 {
		return 0
	}
	if ratio >= 1 {
		return 1000
	}
	return int(ratio*1000 + 0.5)
}

func throttleRateForPressure(pressure uint32, minRate, maxRate int64) uint64 {
	if pressure == 0 || maxRate <= 0 {
		return 0
	}
	if minRate <= 0 {
		minRate = maxRate
	}
	if maxRate < minRate {
		maxRate = minRate
	}
	ratio := float64(pressure) / 1000
	if ratio < 0 {
		ratio = 0
	}
	if ratio > 1 {
		ratio = 1
	}
	curve := ratio * ratio
	rate := float64(maxRate) - (float64(maxRate-minRate) * curve)
	if rate < float64(minRate) {
		rate = float64(minRate)
	}
	return uint64(rate + 0.5)
}

// === executor ===

// =====  Planner glue (was planner.go) =====

// resolvePlanLocked binds plan tables; caller must hold cd level locks.
func (c *compactor) resolvePlanLocked(cd *compactDef) bool {
	if cd == nil || cd.thisLevel == nil || cd.nextLevel == nil {
		return false
	}
	topFromLanding := cd.spec.LandingMode.UsesLanding()
	top := resolveTablesLocked(cd.thisLevel, cd.spec.TopIDs, topFromLanding)
	if len(cd.spec.TopIDs) != len(top) {
		return false
	}
	bot := resolveTablesLocked(cd.nextLevel, cd.spec.BotIDs, false)
	if len(cd.spec.BotIDs) != len(bot) {
		return false
	}
	cd.top = top
	cd.bot = bot
	cd.thisSize = 0
	for _, t := range cd.top {
		if t != nil {
			cd.thisSize += t.Size()
		}
	}
	return true
}

// fillTables selects SSTables for this compaction and registers the plan in
// compactState. Returns true if a valid plan was produced.
func (c *compactor) fillTables(cd *compactDef) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	if cd.thisLevel.numTablesLocked() == 0 {
		if cd.thisLevel.isLastLevel() && cd.thisLevel.numLandingTablesLocked() > 0 {
			cd.thisLevel.landing.EnsureInit()
			meta := tableMetaSnapshot(cd.thisLevel.landing.AllTables())
			if len(meta) == 0 {
				return false
			}
			p, ok := plan.ForLandingFallback(cd.thisLevel.levelNum, meta)
			if !ok {
				return false
			}
			cd.spec.LandingMode = plan.LandingKeep
			cd.applyPlan(p)
			if !c.resolvePlanLocked(cd) {
				return false
			}
			return c.state.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
		}
		return false
	}
	tables := make([]*table.Table, cd.thisLevel.numTablesLocked())
	copy(tables, cd.thisLevel.tables)
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return c.fillMaxLevelTables(tables, cd)
	}
	p, ok := plan.ForRegular(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), c.state)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !c.resolvePlanLocked(cd) {
		return false
	}
	return c.state.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

func (c *compactor) fillTablesLandingShard(cd *compactDef, shardIdx int) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	totalLanding := cd.thisLevel.numLandingTablesLocked()
	if totalLanding == 0 {
		return false
	}
	batchSize := c.lm.opt.LandingCompactBatchSize
	if batchSize <= 0 || batchSize > totalLanding {
		batchSize = totalLanding
	}
	if shardIdx < 0 {
		shardIdx = cd.thisLevel.landingShardByBacklog()
	}
	shTables := cd.thisLevel.landing.ShardTablesByIndex(shardIdx)
	if len(shTables) == 0 {
		return false
	}
	shMeta := tableMetaSnapshot(shTables)
	p, ok := plan.ForLandingShard(cd.thisLevel.levelNum, shMeta, cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), cd.targetFileSize(), batchSize, c.state)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !c.resolvePlanLocked(cd) {
		return false
	}
	return c.state.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// resolveTablesLocked maps IDs to tables; caller must hold lh lock.
func resolveTablesLocked(lh *levelHandler, ids []uint64, landing bool) []*table.Table {
	if lh == nil || len(ids) == 0 {
		return nil
	}
	var tables []*table.Table
	if landing {
		tables = lh.landing.AllTables()
	} else {
		tables = lh.tables
	}
	if len(tables) == 0 {
		return nil
	}
	byID := make(map[uint64]*table.Table, len(tables))
	for _, t := range tables {
		if t != nil {
			byID[t.FID()] = t
		}
	}
	out := make([]*table.Table, 0, len(ids))
	for _, id := range ids {
		t, ok := byID[id]
		if !ok {
			return nil
		}
		out = append(out, t)
	}
	return out
}

func tableMetaSnapshot(tables []*table.Table) []plan.TableMeta {
	if len(tables) == 0 {
		return nil
	}
	out := make([]plan.TableMeta, 0, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		meta := plan.TableMeta{
			ID:         t.FID(),
			MinKey:     t.MinKey(),
			MaxKey:     t.MaxKey(),
			Size:       t.Size(),
			StaleSize:  int64(t.StaleDataSize()),
			MaxVersion: t.MaxVersionVal(),
		}
		if created := t.GetCreatedAt(); created != nil {
			meta.CreatedAt = *created
		}
		out = append(out, meta)
	}
	return out
}

func findTableByID(tables []*table.Table, fid uint64) *table.Table {
	for _, t := range tables {
		if t.FID() == fid {
			return t
		}
	}
	return nil
}

// addSplits prepares key ranges for parallel sub-compactions.
//
// Cap the split count at max(NumCompactors, 5): more splits == more
// concurrent builder goroutines per compaction, but each builder needs
// its own table builder buffer (~SSTable block size × bloom × index),
// so an unbounded split count exhausts memory. NumCompactors is the
// natural upper bound — beyond that, extra splits sit in the
// utils.Throttle queue without saving wall time.
func (c *compactor) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	maxSplits := 5
	if c != nil && c.lm.opt != nil && c.lm.opt.NumCompactors > maxSplits {
		maxSplits = c.lm.opt.NumCompactors
	}
	width := max(int(math.Ceil(float64(len(cd.bot))/float64(maxSplits))), 3)
	skr := cd.spec.ThisRange
	skr.Extend(cd.spec.NextRange)

	addRange := func(right []byte) {
		skr.Right = slices.Clone(right)
		cd.splits = append(cd.splits, skr)
		skr.Left = skr.Right
	}

	for i, t := range cd.bot {
		// last entry in bottom table.
		if i == len(cd.bot)-1 {
			addRange([]byte{})
			return
		}
		if i%width == width-1 {
			// Set the right bound to the max key.
			cf, userKey, _, ok := kv.SplitInternalKey(t.MaxKey())
			utils.CondPanicFunc(!ok, func() error {
				return fmt.Errorf("addSplits expects internal max key: %x", t.MaxKey())
			})
			right := kv.InternalKey(cf, userKey, math.MaxUint64)
			addRange(right)
		}
	}
}

// fillMaxLevelTables handles max-level compaction.
func (c *compactor) fillMaxLevelTables(tables []*table.Table, cd *compactDef) bool {
	var ttlMinAge time.Duration
	if c != nil && c.lm.opt != nil {
		ttlMinAge = c.lm.opt.TTLCompactionMinAge
	}
	p, ok := plan.ForMaxLevel(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.spec.ThisFileSize, c.state, time.Now(), ttlMinAge)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !c.resolvePlanLocked(cd) {
		return false
	}
	return c.state.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// fillTablesL0 tries L0->Lbase first, then falls back to L0->L0.
func (c *compactor) fillTablesL0(cd *compactDef) bool {
	if ok := c.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return c.fillTablesL0ToL0(cd)
}

func (c *compactor) moveToLanding(cd *compactDef) error {
	if cd == nil || cd.thisLevel == nil || cd.nextLevel == nil {
		return errors.New("invalid compaction definition for landing move")
	}
	if len(cd.top) == 0 {
		return nil
	}
	var edits []manifest.Edit
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		del := manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &manifest.FileMeta{FileID: tbl.FID(), Level: cd.thisLevel.levelNum},
		}
		edits = append(edits, del)
		add := manifest.Edit{
			Type: manifest.EditAddFile,
			File: &manifest.FileMeta{
				Level:     cd.nextLevel.levelNum,
				FileID:    tbl.FID(),
				Size:      uint64(tbl.Size()),
				Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
				Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
				CreatedAt: uint64(time.Now().Unix()),
				ValueSize: tbl.ValueSize(),
				Landing:   true,
			},
		}
		edits = append(edits, add)
	}
	if err := c.lm.manifestMgr.LogEdits(edits...); err != nil {
		return err
	}

	toDel := make(map[uint64]struct{}, len(cd.top))
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		toDel[tbl.FID()] = struct{}{}
	}

	// Update in-memory state atomically across the source and target levels to avoid
	// a visibility gap for readers walking L0 -> Ln.
	first, second := cd.thisLevel, cd.nextLevel
	if first.levelNum > second.levelNum {
		first, second = second, first
	}
	first.Lock()
	second.Lock()
	var remaining []*table.Table
	for _, tbl := range cd.thisLevel.tables {
		if _, found := toDel[tbl.FID()]; found {
			cd.thisLevel.subtractSize(tbl)
			continue
		}
		remaining = append(remaining, tbl)
	}
	cd.thisLevel.tables = remaining
	cd.thisLevel.refreshTableIndexesLocked()

	cd.nextLevel.landing.EnsureInit()
	for _, t := range cd.top {
		if t == nil {
			continue
		}
		t.SetLevel(cd.nextLevel.levelNum)
	}
	cd.nextLevel.landing.AddBatch(cd.top)
	cd.nextLevel.landing.SortShards()
	second.Unlock()
	first.Unlock()

	if c.sched != nil {
		c.sched.Trigger()
	}
	return nil
}

func (c *compactor) fillTablesL0ToLbase(cd *compactDef) bool {
	if cd.nextLevel.levelNum == 0 {
		return false
	}
	// Skip if priority is below 1.
	if cd.adjusted > 0.0 && cd.adjusted < 1.0 {
		// Do not compact to Lbase if adjusted score is less than 1.0.
		return false
	}
	cd.lockLevels()
	defer cd.unlockLevels()

	top := cd.thisLevel.tables
	if len(top) == 0 {
		return false
	}
	p, ok := plan.ForL0ToLbase(tableMetaSnapshot(top), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), c.state)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !c.resolvePlanLocked(cd) {
		return false
	}
	return c.state.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// fillTablesL0ToL0 performs L0->L0 compaction.
//
// Multiple compactor workers may invoke this concurrently — plan.ForL0ToL0
// caps each call at l0ToL0MaxTablesPerWorker tables and skips tables
// already claimed by state.HasTable, so workers naturally partition the
// available L0 SSTs. The plan is marked IntraLevel so the state machine
// claims by table ID without registering an InfRange that would block
// peer L0→Lbase compactions.
func (c *compactor) fillTablesL0ToL0(cd *compactDef) bool {
	cd.nextLevel = c.lm.levels[0]
	cd.spec.NextLevel = cd.spec.ThisLevel
	cd.spec.NextFileSize = cd.spec.ThisFileSize
	cd.spec.NextRange = plan.KeyRange{}
	cd.bot = nil

	// We intentionally avoid calling compactDef.lockLevels here. Both thisLevel and nextLevel
	// point at L0, so grabbing the RLock twice would violate RWMutex semantics and can deadlock
	// once another goroutine attempts a write lock. Taking the shared lock exactly once matches
	// Badger's approach and keeps lock acquisition order (level -> compactState) consistent.
	utils.CondPanicFunc(cd.thisLevel.levelNum != 0, func() error { return errors.New("cd.thisLevel.levelNum != 0") })
	utils.CondPanicFunc(cd.nextLevel.levelNum != 0, func() error { return errors.New("cd.nextLevel.levelNum != 0") })
	c.lm.levels[0].RLock()
	defer c.lm.levels[0].RUnlock()

	top := cd.thisLevel.tables
	now := time.Now()
	p, ok := plan.ForL0ToL0(cd.thisLevel.levelNum, tableMetaSnapshot(top), cd.spec.ThisFileSize, c.state, now)
	if !ok {
		// Skip when fewer than four tables qualify.
		return false
	}
	cd.applyPlan(p)
	if !c.resolvePlanLocked(cd) {
		return false
	}

	// L0->L0 compaction collapses into a single file, reducing L0 count and read amplification.
	cd.spec.ThisFileSize = math.MaxUint32
	cd.spec.NextFileSize = cd.spec.ThisFileSize
	return c.state.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// getKeyRange returns the merged min/max key range for a set of live tables.
func getKeyRange(tables ...*table.Table) plan.KeyRange {
	if len(tables) == 0 {
		return plan.KeyRange{}
	}
	minKey := tables[0].MinKey()
	maxKey := tables[0].MaxKey()
	for i := 1; i < len(tables); i++ {
		if kv.CompareInternalKeys(tables[i].MinKey(), minKey) < 0 {
			minKey = tables[i].MinKey()
		}
		if kv.CompareInternalKeys(tables[i].MaxKey(), maxKey) > 0 {
			maxKey = tables[i].MaxKey()
		}
	}

	// We pick all the versions of the smallest and the biggest key. Note that version zero would
	// be the rightmost key, considering versions are default sorted in descending order.
	leftCF, leftUserKey, _, leftOK := kv.SplitInternalKey(minKey)
	utils.CondPanicFunc(!leftOK, func() error {
		return fmt.Errorf("getKeyRange expects internal min key: %x", minKey)
	})
	rightCF, rightUserKey, _, rightOK := kv.SplitInternalKey(maxKey)
	utils.CondPanicFunc(!rightOK, func() error {
		return fmt.Errorf("getKeyRange expects internal max key: %x", maxKey)
	})
	return plan.KeyRange{
		Left:  kv.InternalKey(leftCF, leftUserKey, math.MaxUint64),
		Right: kv.InternalKey(rightCF, rightUserKey, 0),
	}
}

// =====  Compaction executor (was compaction_executor.go) =====

// doCompact selects tables from a level and merges them into the target level.
func (c *compactor) doCompact(id int, p plan.Priority) (retErr error) {
	l := p.Level
	utils.CondPanicFunc(l >= c.lm.opt.MaxLevelNum, func() error { return errors.New("[doCompact] Sanity check. l >= c.lm.opt.MaxLevelNum") }) // Sanity check.
	t := p.Target
	if t.BaseLevel == 0 {
		t = c.levelTargets()
	}
	// Build the concrete compaction plan.
	cd := compactDef{
		compactorId: id,
		spec: plan.Plan{
			ThisLevel:    l,
			ThisFileSize: t.FileSizeForLevel(l),
			LandingMode:  p.LandingMode,
			DropPrefixes: p.DropPrefixes,
			StatsTag:     p.StatsTag,
		},
		thisLevel: c.lm.levels[l],
		adjusted:  p.Adjusted,
	}

	var cleanup bool
	defer func() {
		if cleanup {
			if err := c.state.Delete(cd.stateEntry()); err != nil {
				c.lm.getLogger().Warn("failed to cleanup compaction state", "worker", id, "err", err)
				retErr = errors.Join(retErr, err)
			}
		}
	}()

	if p.LandingMode.UsesLanding() && l > 0 {
		cd.setNextLevel(t, cd.thisLevel)
		order := cd.thisLevel.landingShardOrderBySize()
		if len(order) == 0 {
			return ErrFillTables
		}
		baseLimit := c.lm.opt.LandingShardParallelism
		if baseLimit <= 0 {
			baseLimit = max(c.lm.opt.NumCompactors/2, 1)
		}
		if baseLimit > len(order) {
			baseLimit = len(order)
		}
		// Adaptive bump: more backlog => allow more shards, capped by shard count.
		shardLimit := baseLimit
		if p.Score > 1.0 {
			shardLimit += int(math.Ceil(p.Score / 2))
			if shardLimit > len(order) {
				shardLimit = len(order)
			}
		}
		var ran bool
		for i := 0; i < shardLimit; i++ {
			sub := cd
			if !c.fillTablesLandingShard(&sub, order[i]) {
				continue
			}
			sub.spec.LandingMode = p.LandingMode
			sub.spec.StatsTag = p.StatsTag
			if err := c.runCompactDef(id, l, sub); err != nil {
				c.lm.getLogger().Error("landing compaction failed", "worker", id, "err", err, "def", sub)
				if stateDelErr := c.state.Delete(sub.stateEntry()); stateDelErr != nil {
					return errors.Join(err, stateDelErr)
				}
				return err
			}
			if err := c.state.Delete(sub.stateEntry()); err != nil {
				return err
			}
			ran = true
			c.lm.getLogger().Info("landing compaction complete", "worker", id, "level", sub.thisLevel.levelNum, "shard", order[i])
		}
		if !ran {
			return ErrFillTables
		}
		return nil
	}

	// L0 uses a dedicated selection path.
	if l == 0 {
		cd.setNextLevel(t, c.lm.levels[t.BaseLevel])
		if !c.fillTablesL0(&cd) {
			return ErrFillTables
		}
		cleanup = true
		if cd.nextLevel.levelNum != 0 {
			if err := c.moveToLanding(&cd); err != nil {
				c.lm.getLogger().Error("move to landing failed", "worker", id, "err", err, "def", cd)
				return err
			}
			c.lm.getLogger().Info("moved L0 tables to landing buffer", "worker", id, "tables", len(cd.top), "target_level", cd.nextLevel.levelNum)
			return nil
		}
	} else {
		cd.setNextLevel(t, cd.thisLevel)
		// For non-last levels, compact into the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.setNextLevel(t, c.lm.levels[l+1])
		}
		if !c.fillTables(&cd) {
			return ErrFillTables
		}
		cleanup = true
		if c.canMoveToNextLevel(&cd) {
			if err := c.moveToNextLevel(&cd); err != nil {
				c.lm.getLogger().Error("trivial move failed", "worker", id, "err", err, "def", cd)
				return err
			}
			c.lm.getLogger().Info("trivial move complete", "worker", id, "from_level", cd.thisLevel.levelNum, "to_level", cd.nextLevel.levelNum, "tables", len(cd.top))
			return nil
		}
		// Continue with the normal merge path.
		if err := c.runCompactDef(id, l, cd); err != nil {
			c.lm.getLogger().Error("compaction failed", "worker", id, "err", err, "def", cd)
			return err
		}
		c.lm.getLogger().Info("compaction complete", "worker", id, "level", cd.thisLevel.levelNum)
		return nil
	}

	// Execute the merge plan.
	if err := c.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		c.lm.getLogger().Error("compaction failed", "worker", id, "err", err, "def", cd)
		return err
	}
	c.lm.getLogger().Info("compaction complete", "worker", id, "level", cd.thisLevel.levelNum)
	return nil
}

func (c *compactor) runCompactDef(id, l int, cd compactDef) (err error) {
	if cd.spec.NextFileSize <= 0 {
		return errors.New("Next file size cannot be zero. Targets are not set")
	}
	timeStart := time.Now()

	thisLevel := cd.thisLevel
	nextLevel := cd.nextLevel

	utils.CondPanicFunc(len(cd.splits) != 0, func() error { return errors.New("len(cd.splits) != 0") })
	if thisLevel == nextLevel {
		// No special handling for L0->L0 and Lmax->Lmax.
	} else {
		c.addSplits(&cd)
	}
	// Append an empty range placeholder when no split is found.
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, plan.KeyRange{})
	}

	newTables, decr, err := c.compactBuildTables(l, cd)
	if err != nil {
		return err
	}
	cleanupNeeded := true
	defer func() {
		if !cleanupNeeded {
			return
		}
		// Only assign to err, if it's not already nil.
		if decErr := decr(); err == nil {
			err = decErr
		}
	}()
	changeSet := buildChangeSet(&cd, newTables)

	// Update the manifest.
	var manifestEdits []manifest.Edit
	levelByID := make(map[uint64]int, len(cd.top)+len(cd.bot))
	for _, t := range cd.top {
		levelByID[t.FID()] = cd.thisLevel.levelNum
	}
	for _, t := range cd.bot {
		levelByID[t.FID()] = cd.nextLevel.levelNum
	}
	for _, ch := range changeSet.Changes {
		switch ch.Op {
		case storagepb.ManifestChange_CREATE:
			tbl := findTableByID(newTables, ch.Id)
			if tbl == nil {
				continue
			}
			add := manifest.Edit{
				Type: manifest.EditAddFile,
				File: &manifest.FileMeta{
					Level:     int(ch.Level),
					FileID:    tbl.FID(),
					Size:      uint64(tbl.Size()),
					Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
					Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
					CreatedAt: uint64(time.Now().Unix()),
					ValueSize: tbl.ValueSize(),
					Landing:   cd.spec.LandingMode == plan.LandingKeep,
				},
			}
			manifestEdits = append(manifestEdits, add)
		case storagepb.ManifestChange_DELETE:
			level := levelByID[ch.Id]
			del := manifest.Edit{
				Type: manifest.EditDeleteFile,
				File: &manifest.FileMeta{FileID: ch.Id, Level: level},
			}
			manifestEdits = append(manifestEdits, del)
		}
	}
	if err := c.lm.manifestMgr.LogEdits(manifestEdits...); err != nil {
		return err
	}
	cleanupNeeded = false

	if cd.spec.LandingMode == plan.LandingKeep {
		if err := thisLevel.replaceLandingTables(cd.top, newTables); err != nil {
			return err
		}
		if thisLevel.levelNum > 0 {
			thisLevel.sort()
		}
	} else {
		if err := nextLevel.replaceTables(cd.bot, newTables); err != nil {
			return err
		}
		switch cd.spec.LandingMode {
		case plan.LandingDrain:
			if err := thisLevel.deleteLandingTables(cd.top); err != nil {
				return err
			}
		default:
			// plan.LandingNone (and unknown modes) own top tables in the main level list.
			if err := thisLevel.deleteTables(cd.top); err != nil {
				return err
			}
		}
	}

	from := append(tablesToString(cd.top), tablesToString(cd.bot)...)
	to := tablesToString(newTables)
	if dur := time.Since(timeStart); dur > 2*time.Second {
		c.lm.getLogger().Info(
			"compaction detail",
			"worker", id,
			"expensive", dur > time.Second,
			"from_level", thisLevel.levelNum,
			"to_level", nextLevel.levelNum,
			"top_tables", len(cd.top),
			"bottom_tables", len(cd.bot),
			"new_tables", len(newTables),
			"splits", len(cd.splits),
			"from", strings.Join(from, " "),
			"to", strings.Join(to, " "),
			"duration", dur.Round(time.Millisecond).String(),
		)
	}
	// Record landing metrics if applicable.
	if cd.spec.LandingMode.UsesLanding() {
		tablesCompacted := len(cd.top) + len(cd.bot)
		cd.thisLevel.recordLandingMetrics(cd.spec.LandingMode == plan.LandingKeep, time.Since(timeStart), tablesCompacted)
	}
	c.recordRun(time.Since(timeStart))
	// After max-level compaction, range tombstone layout may change.
	// Rebuild the in-memory range tombstone index to keep read visibility correct.
	if cd.nextLevel != nil && cd.nextLevel.levelNum == c.lm.opt.MaxLevelNum-1 && c.lm.rtCollector != nil {
		c.lm.rebuildRangeTombstones()
	}
	return nil
}

func (c *compactor) canMoveToNextLevel(cd *compactDef) bool {
	if cd == nil || cd.thisLevel == nil || cd.nextLevel == nil {
		return false
	}
	if cd.spec.LandingMode != plan.LandingNone {
		return false
	}
	if cd.thisLevel == cd.nextLevel {
		return false
	}
	if len(cd.top) == 0 || len(cd.bot) != 0 {
		return false
	}
	if cd.thisLevel.levelNum == 0 {
		// L0 trivial move is only safe when the chosen group has no overlap
		// with any other L0 table. Otherwise promoting it would leave older
		// L0 tables masking newer keys at the destination level.
		if !l0GroupHasNoOtherOverlap(cd.top, cd.thisLevel.tables) {
			return false
		}
	}
	return true
}

func (c *compactor) moveToNextLevel(cd *compactDef) error {
	if !c.canMoveToNextLevel(cd) {
		return errors.New("invalid compaction definition for trivial move")
	}
	var edits []manifest.Edit
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		edits = append(edits, manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &manifest.FileMeta{FileID: tbl.FID(), Level: cd.thisLevel.levelNum},
		})
		add := manifest.Edit{
			Type: manifest.EditAddFile,
			File: &manifest.FileMeta{
				Level:     cd.nextLevel.levelNum,
				FileID:    tbl.FID(),
				Size:      uint64(tbl.Size()),
				Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
				Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
				ValueSize: tbl.ValueSize(),
			},
		}
		if created := tbl.GetCreatedAt(); created != nil {
			add.File.CreatedAt = uint64(created.Unix())
		}
		edits = append(edits, add)
	}
	if len(edits) == 0 {
		return nil
	}
	if err := c.lm.manifestMgr.LogEdits(edits...); err != nil {
		return err
	}

	toMove := make(map[uint64]*table.Table, len(cd.top))
	for _, tbl := range cd.top {
		if tbl != nil {
			toMove[tbl.FID()] = tbl
		}
	}

	first, second := cd.thisLevel, cd.nextLevel
	if first.levelNum > second.levelNum {
		first, second = second, first
	}
	first.Lock()
	second.Lock()

	remaining := cd.thisLevel.tables[:0]
	for _, tbl := range cd.thisLevel.tables {
		if _, found := toMove[tbl.FID()]; found {
			cd.thisLevel.subtractSize(tbl)
			continue
		}
		remaining = append(remaining, tbl)
	}
	cd.thisLevel.tables = remaining
	cd.thisLevel.refreshTableIndexesLocked()

	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		tbl.SetLevel(cd.nextLevel.levelNum)
		cd.nextLevel.addSize(tbl)
		cd.nextLevel.tables = append(cd.nextLevel.tables, tbl)
	}
	cd.nextLevel.refreshTableIndexesLocked()

	second.Unlock()
	first.Unlock()
	return nil
}

// tablesToString
func tablesToString(tables []*table.Table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.FID()))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table.Table) storagepb.ManifestChangeSet {
	changes := []*storagepb.ManifestChange{}
	for _, tbl := range newTables {
		changes = append(changes, newCreateChange(tbl.FID(), cd.nextLevel.levelNum))
	}
	for _, tbl := range cd.top {
		changes = append(changes, newDeleteChange(tbl.FID()))
	}
	for _, tbl := range cd.bot {
		changes = append(changes, newDeleteChange(tbl.FID()))
	}
	return storagepb.ManifestChangeSet{Changes: changes}
}

func newDeleteChange(id uint64) *storagepb.ManifestChange {
	return &storagepb.ManifestChange{
		Id: id,
		Op: storagepb.ManifestChange_DELETE,
	}
}

// newCreateChange
func newCreateChange(id uint64, level int) *storagepb.ManifestChange {
	return &storagepb.ManifestChange{
		Id:    id,
		Op:    storagepb.ManifestChange_CREATE,
		Level: uint32(level),
	}
}

// compactBuildTables merges SSTables from two levels.
func (c *compactor) compactBuildTables(lev int, cd compactDef) ([]*table.Table, func() error, error) {

	topTables := append([]*table.Table(nil), cd.top...)
	botTables := append([]*table.Table(nil), cd.bot...)
	// Ensure concat/merge inputs are in ascending key-range order.
	// Some planning paths may preserve selection order but not strict key order.
	if len(topTables) > 1 {
		sort.Slice(topTables, func(i, j int) bool {
			return kv.CompareInternalKeys(topTables[i].MinKey(), topTables[j].MinKey()) < 0
		})
	}
	if len(botTables) > 1 {
		sort.Slice(botTables, func(i, j int) bool {
			return kv.CompareInternalKeys(botTables[i].MinKey(), botTables[j].MinKey()) < 0
		})
	}
	iterOpt := &index.Options{
		IsAsc:          true,
		AccessPattern:  utils.AccessPatternSequential,
		PrefetchBlocks: 1,
	}
	botCanConcat := tablesStrictlyOrdered(botTables)
	//numTables := int64(len(topTables) + len(botTables))
	newIterator := func() []index.Iterator {
		// Create iterators across all the tables involved first.
		var iters []index.Iterator
		switch {
		case lev == 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		case len(topTables) > 0:
			iters = append(iters, iteratorsReversed(topTables, iterOpt)...)
		}
		if len(botTables) == 0 {
			return iters
		}
		if botCanConcat {
			return append(iters, iterator.NewConcatIterator(botTables, iterOpt))
		}
		// Fallback for overlapping/out-of-order next-level windows.
		// ConcatIterator assumes strict non-overlap; merge keeps global key ordering.
		return append(iters, iteratorsReversed(botTables, iterOpt)...)
	}

	// Start parallel compaction tasks.
	res := make(chan *table.Table, 3)
	// Throttle inflight builders to bound memory and file handles.
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		if err := inflightBuilders.Go(func() error {
			it := iterator.NewMergeIterator(newIterator(), false)
			defer func() { _ = it.Close() }()
			c.subcompact(it, kr, cd, inflightBuilders, res)
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
	}

	// Collect table handles via fan-in.
	var newTables []*table.Table
	var wg sync.WaitGroup
	wg.Go(func() {
		for t := range res {
			newTables = append(newTables, t)
		}
	})

	// Wait for all compaction tasks to finish.
	err := inflightBuilders.Finish()
	// Release channel resources.
	close(res)
	// Wait for all builders to flush to disk.
	wg.Wait()

	if err == nil && c.lm.opt.ManifestSync {
		// Strict durability mode: persist new SST directory entries before manifest edits.
		err = vfs.SyncDir(c.lm.opt.FS, c.lm.opt.WorkDir)
	}

	if err != nil {
		// On error, delete newly created files.
		_ = table.DecrAll(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return kv.CompareInternalKeys(newTables[i].MaxKey(), newTables[j].MaxKey()) < 0
	})
	return newTables, func() error { return table.DecrAll(newTables) }, nil
}

func iteratorsReversed(th []*table.Table, opt *index.Options) []index.Iterator {
	out := make([]index.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

// tablesStrictlyOrdered reports whether consecutive tables are in strictly
// increasing, non-overlapping user-key order.
func tablesStrictlyOrdered(tables []*table.Table) bool {
	if len(tables) <= 1 {
		return true
	}
	prev := tables[0]
	if prev == nil {
		return false
	}
	for i := 1; i < len(tables); i++ {
		cur := tables[i]
		if cur == nil {
			return false
		}
		// Non-overlap requires prev.max base key < cur.min base key.
		if kv.CompareBaseKeys(prev.MaxKey(), cur.MinKey()) >= 0 {
			return false
		}
		prev = cur
	}
	return true
}

// subcompact runs a single parallel compaction over a key range.
func (c *compactor) subcompact(it index.Iterator, kr plan.KeyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table.Table) {
	var lastKey []byte

	// Keep tombstone state across builder splits.
	rtTracker := tombstone.NewCompactionTracker()

	addKeys := func(builder *table.Builder) {
		var tableKr plan.KeyRange

		for ; it.Valid(); it.Next() {
			entry := it.Item().Entry()
			key := entry.Key
			isExpired := entry.IsDeletedOrExpired()

			if entry.IsRangeDelete() {
				// Preserve range tombstones even at max level. Dropping them during
				// partial Lmax->Lmax rewrites can resurrect older covered keys that
				// remain in untouched tables outside this compaction unit.
				// Copy range tombstone data to avoid iterator reuse issues.
				cf, rtStart, rtVersion, ok := kv.SplitInternalKey(entry.Key)
				if !ok {
					continue
				}
				rt := tombstone.Range{
					CF:      cf,
					Start:   kv.SafeCopy(nil, rtStart),
					End:     kv.SafeCopy(nil, entry.RangeEnd()),
					Version: rtVersion,
				}
				rtTracker.Add(rt)
			}
			if !kv.SameBaseKey(key, lastKey) {
				if len(kr.Right) > 0 && kv.CompareInternalKeys(key, kr.Right) >= 0 {
					break
				}
				if builder.ReachedCapacity() {
					break
				}
				lastKey = kv.SafeCopy(lastKey, key)
				if len(tableKr.Left) == 0 {
					tableKr.Left = kv.SafeCopy(tableKr.Left, key)
				}
				tableKr.Right = lastKey
			}

			if !entry.IsRangeDelete() {
				cf, userKey, version, ok := kv.SplitInternalKey(key)
				if !ok {
					continue
				}
				if rtTracker.Covers(cf, userKey, version) {
					// Covered point versions become stale once a newer range
					// tombstone is active at this key.
					continue
				}
			}

			valueLen := entryValueLen(entry)
			if isExpired {
				builder.AddStaleEntryWithLen(entry, valueLen)
			} else {
				builder.AddKeyWithLen(entry, valueLen)
			}
		}
	}

	// If the left bound remains, seek there to resume a partial scan.
	if len(kr.Left) > 0 {
		it.Seek(kr.Left)
	} else {
		//
		it.Rewind()
	}
	for it.Valid() {
		key := it.Item().Entry().Key
		if len(kr.Right) > 0 && kv.CompareInternalKeys(key, kr.Right) >= 0 {
			break
		}
		// Copy Options so background tuning does not affect the active compaction.
		builderOpt := c.lm.opt.Clone()
		builder := table.NewBuilderWithSize(tableOptionsFor(builderOpt), cd.spec.NextFileSize)
		builder.SetPacer(c.pacerForBuild())

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.Empty() {
			// Cleanup builder resources:
			_, _ = builder.Finish()
			builder.Close()
			continue
		}
		// Leverage SSD parallel write throughput.
		b := builder
		if err := inflightBuilders.Go(func() error {
			defer b.Close()
			newFID := c.lm.maxFID.Add(1) // Compaction does not allocate memtables; advance maxFID.
			sstName := vfs.FileNameSSTable(c.lm.opt.WorkDir, newFID)
			tbl, err := table.Open(c.lm, sstName, b)
			if err != nil || tbl == nil {
				slog.Default().Error("open compacted table", "path", sstName, "error", err)
				return nil
			}
			res <- tbl
			return nil
		}); err != nil {
			// Can't return from here, until I decrRef all the tables that I built so far.
			break
		}
	}
}
