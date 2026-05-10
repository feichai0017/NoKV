package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	cachepkg "github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/feichai0017/NoKV/engine/lsm/tombstone"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/metrics"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"github.com/feichai0017/NoKV/utils"
)

// initLevelManager initialize the levels runtime
func (lsm *LSM) initLevelManager(opt *Options) (_ *levelManager, err error) {
	lm := &levelManager{lsm: lsm} // attach lsm owner
	defer func() {
		if err != nil {
			_ = lm.close()
		}
	}()
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	// read the manifest file to build the levels runtime
	if err := lm.loadManifest(); err != nil {
		return nil, err
	}
	if lm.manifestMgr != nil {
		lm.manifestMgr.SetSync(opt.ManifestSync)
		lm.manifestMgr.SetRewriteThreshold(opt.ManifestRewriteThreshold)
	}
	if err := lm.build(); err != nil {
		return nil, err
	}
	lm.rtCollector = tombstone.NewCollector()
	lm.compactionPacer = pacer.New(opt.CompactionWriteBytesPerSec)
	lm.compaction = newCompaction(lm, lm.opt.NumCompactors, lm.opt.CompactionPolicy, lsm.getLogger())
	return lm, nil
}

type levelManager struct {
	maxFID           atomic.Uint64
	opt              *Options
	cache            *cachepkg.Cache
	manifestMgr      *manifest.Manager
	levels           []*levelHandler
	lsm              *LSM
	compactState     *plan.State
	compaction       *compaction
	compactionPacer  *pacer.Pacer
	rtCollector      *tombstone.Collector
	compactionLastNs atomic.Int64
	compactionMaxNs  atomic.Int64
	compactionRuns   atomic.Uint64
	rangeFilter      rangeFilterMetrics
}

func (lm *levelManager) close() error {
	var closeErr error
	if lm.cache != nil {
		closeErr = errors.Join(closeErr, lm.cache.Close())
	}
	if lm.manifestMgr != nil {
		closeErr = errors.Join(closeErr, lm.manifestMgr.Close())
	}
	for i := range lm.levels {
		if lm.levels[i] == nil {
			continue
		}
		closeErr = errors.Join(closeErr, lm.levels[i].close())
	}
	return closeErr
}

func (lm *levelManager) getLogger() *slog.Logger {
	if lm == nil || lm.lsm == nil {
		return slog.Default()
	}
	return lm.lsm.getLogger()
}

func (lm *levelManager) iterators(opt *index.Options) []index.Iterator {
	itrs := make([]index.Iterator, 0, len(lm.levels))
	for _, level := range lm.levels {
		itrs = append(itrs, level.iterators(opt)...)
	}
	return itrs
}

// Get searches every level and returns the highest visible version for key.
func (lm *levelManager) Get(key []byte) (*kv.Entry, error) {
	var best *kv.Entry
	for level := 0; level < lm.opt.MaxLevelNum; level++ {
		entry, err := lm.levels[level].Get(key)
		if err != nil && err != utils.ErrKeyNotFound {
			if best != nil {
				best.DecrRef()
			}
			return nil, err
		}
		if entry == nil {
			continue
		}
		if best == nil || entry.Version > best.Version {
			if best != nil {
				best.DecrRef()
			}
			best = entry
			continue
		}
		entry.DecrRef()
	}
	if best != nil {
		return best, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestMgr, err = manifest.Open(lm.opt.WorkDir, lm.opt.FS)
	return err
}
func (lm *levelManager) build() error {
	fs := vfs.Ensure(lm.opt.FS)
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lh := &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		}
		lh.landing.EnsureInit()
		lm.levels = append(lm.levels, lh)
	}

	version := lm.manifestMgr.Current()
	lm.cache = cachepkg.New(cachepkg.Options{
		IndexBytes: lm.opt.IndexCacheBytes,
		BlockBytes: lm.opt.BlockCacheBytes,
	})
	var maxFID uint64
	for level, files := range version.Levels {
		for _, meta := range files {
			t, err := lm.openManifestTable(fs, level, meta)
			if err != nil {
				return err
			}
			if meta.FileID > maxFID {
				maxFID = meta.FileID
			}
			if meta.Landing {
				lm.levels[level].addLanding(t)
				continue
			}
			lm.levels[level].add(t)
		}
	}
	// sort each level
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// get the maximum fid value
	lm.maxFID.Store(maxFID)
	return nil
}

func (lm *levelManager) openManifestTable(fs vfs.FS, level int, meta manifest.FileMeta) (*table, error) {
	fileName := vfs.FileNameSSTable(lm.opt.WorkDir, meta.FileID)
	if _, err := fs.Stat(fileName); err != nil {
		return nil, fmt.Errorf("lsm startup: manifest references missing sstable L%d F%d (%s): %w", level, meta.FileID, fileName, err)
	}
	t, err := openTable(lm, fileName, nil)
	if err != nil {
		return nil, fmt.Errorf("lsm startup: open sstable L%d F%d (%s): %w", level, meta.FileID, fileName, err)
	}
	if t == nil {
		return nil, fmt.Errorf("lsm startup: open sstable L%d F%d (%s): nil table", level, meta.FileID, fileName)
	}
	return t, nil
}

// flush a sstable to L0 layer
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// allocate a fid
	fid := uint64(immutable.segmentID)
	sstName := vfs.FileNameSSTable(lm.opt.WorkDir, fid)

	iter := immutable.NewIterator(&index.Options{IsAsc: true})
	if iter == nil {
		return nil
	}
	defer func() { _ = iter.Close() }()

	iter.Rewind()
	if !iter.Valid() {
		if err := immutable.shard.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, wal.ErrSegmentRetained) {
			return err
		}
		return nil
	}

	// build a builder and collect range tombstones
	builder := newTableBuiler(lm.opt)
	var newTombstones []tombstone.Range
	for ; iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		if entry != nil && entry.IsRangeDelete() && lm.rtCollector != nil {
			cf, start, version, ok := kv.SplitInternalKey(entry.Key)
			if !ok {
				continue
			}
			newTombstones = append(newTombstones, tombstone.Range{
				CF:      cf,
				Start:   kv.SafeCopy(nil, start),
				End:     kv.SafeCopy(nil, entry.RangeEnd()),
				Version: version,
			})
		}
		builder.AddKey(entry)
	}
	table, err := openTable(lm, sstName, builder)
	if err != nil {
		return fmt.Errorf("failed to build sstable %s: %w", sstName, err)
	}
	if table == nil {
		return fmt.Errorf("failed to build sstable %s: nil table", sstName)
	}
	meta := &manifest.FileMeta{
		Level:     0,
		FileID:    fid,
		Size:      uint64(table.Size()),
		Smallest:  kv.SafeCopy(nil, table.MinKey()),
		Largest:   kv.SafeCopy(nil, table.MaxKey()),
		CreatedAt: uint64(time.Now().Unix()),
		ValueSize: table.ValueSize(),
	}
	fileEdit := manifest.Edit{
		Type:   manifest.EditAddFile,
		File:   meta,
		LogSeg: immutable.segmentID,
	}
	// Strict durability mode: persist SST directory entries before manifest references.
	if lm.opt.ManifestSync {
		if err := vfs.SyncDir(lm.opt.FS, lm.opt.WorkDir); err != nil {
			return err
		}
	}
	// EditAddFile carries LogSeg per-table, which is the recovery
	// anchor recovery actually consults. EditLogPointer used to encode
	// a global "WAL replay starts here" tuple but became dead state
	// once the data plane sharded — recovery now drives WAL replay
	// per-shard via wal.Manager.Replay. We no longer emit
	// EditLogPointer from the flush path; the manifest type and read
	// path keep the apply branch only so older manifests round-trip
	// cleanly. See manifest/types.go::Version.
	if err := lm.manifestMgr.LogEdits(fileEdit); err != nil {
		return err
	}
	if shard := immutable.shard; shard != nil {
		// Monotonic per-shard high-water. Per-shard flush serialization
		// is enforced by flushRuntime (see flush_runtime.go: per-shard
		// queue + inFlight flag) so this Store cannot race against a
		// later same-shard flush. The `> cur` guard is kept as a belt-
		// and-braces against future runtime regressions; without
		// flushRuntime serialization the WAL retention mark below
		// would advance out of order and recovery could lose segments.
		if cur := shard.highestFlushedSeg.Load(); immutable.segmentID > cur {
			shard.highestFlushedSeg.Store(immutable.segmentID)
		}
	}
	lm.levels[0].add(table)
	// Register any range tombstones discovered during this flush.
	if lm.rtCollector != nil {
		for _, rt := range newTombstones {
			lm.rtCollector.Add(rt)
		}
	}
	if err := immutable.shard.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, wal.ErrSegmentRetained) {
		return err
	}
	if lm.compaction != nil {
		lm.compaction.Trigger()
	}
	return nil
}

func (lm *levelManager) compactionStats() (int64, float64) {
	if lm == nil {
		return 0, 0
	}
	prios := lm.pickCompactLevels()
	var max float64
	for _, p := range prios {
		if p.Adjusted > max {
			max = p.Adjusted
		}
	}
	return int64(len(prios)), max
}

func (lm *levelManager) levelMetricsSnapshot() []metrics.LevelMetrics {
	if lm == nil {
		return nil
	}
	out := make([]metrics.LevelMetrics, 0, len(lm.levels))
	for _, lh := range lm.levels {
		if lh == nil {
			continue
		}
		out = append(out, lh.metricsSnapshot())
	}
	return out
}

func (lm *levelManager) compactionDurations() (float64, float64, uint64) {
	if lm == nil {
		return 0, 0, 0
	}
	lastNs := lm.compactionLastNs.Load()
	maxNs := lm.compactionMaxNs.Load()
	runs := lm.compactionRuns.Load()
	return float64(lastNs) / 1e6, float64(maxNs) / 1e6, runs
}

func (lm *levelManager) recordCompactionMetrics(duration time.Duration) {
	lm.compactionRuns.Add(1)
	last := duration.Nanoseconds()
	lm.compactionLastNs.Store(last)
	for {
		prev := lm.compactionMaxNs.Load()
		if last <= prev {
			break
		}
		if lm.compactionMaxNs.CompareAndSwap(prev, last) {
			break
		}
	}
}

func (lm *levelManager) cacheMetrics() metrics.CacheSnapshot {
	if lm == nil || lm.cache == nil {
		return metrics.CacheSnapshot{}
	}
	return lm.cache.MetricsSnapshot()
}

func (lm *levelManager) maxVersion() uint64 {
	if lm == nil {
		return 0
	}

	var max uint64
	for _, lh := range lm.levels {
		if lh == nil {
			continue
		}
		lh.RLock()
		for _, tbl := range lh.tables {
			if tbl == nil {
				continue
			}
			if v := tbl.MaxVersionVal(); v > max {
				max = v
			}
		}
		lh.RUnlock()
	}
	return max
}

// =====  Picker (was picker.go) =====

// needsCompaction reports whether any level currently exceeds compaction thresholds.
func (lm *levelManager) needsCompaction() bool {
	return len(lm.pickCompactLevels()) > 0
}

// pickCompactLevels collects per-level compaction inputs from live state and
// asks the plan picker for a priority-ordered candidate list.
func (lm *levelManager) pickCompactLevels() []plan.Priority {
	if lm == nil || lm.opt == nil {
		return nil
	}
	levels := make([]plan.LevelInput, len(lm.levels))
	for i, lvl := range lm.levels {
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
		if lm.compactState != nil {
			li.DelSize = lm.compactState.DelSize(i)
		}
		levels[i] = li
	}
	if len(levels) == 0 {
		return nil
	}
	return plan.PickPriorities(plan.PickerInput{
		Levels:                    levels,
		Targets:                   lm.levelTargets(),
		NumLevelZeroTables:        lm.opt.NumLevelZeroTables,
		BaseTableSize:             lm.opt.BaseTableSize,
		BaseLevelSize:             lm.opt.BaseLevelSize,
		LandingBacklogMergeScore:  lm.opt.LandingBacklogMergeScore,
		CompactionValueWeight:     lm.opt.CompactionValueWeight,
		CompactionTombstoneWeight: lm.opt.CompactionTombstoneWeight,
	})
}

// levelTargets builds the per-level compaction size targets from the
// current level sizes and option knobs.
func (lm *levelManager) levelTargets() plan.Targets {
	if lm == nil || lm.opt == nil || len(lm.levels) == 0 {
		return plan.Targets{}
	}
	sizes := make([]int64, len(lm.levels))
	for i, lvl := range lm.levels {
		if lvl == nil {
			continue
		}
		sizes[i] = lvl.getTotalSize()
	}
	return plan.BuildTargets(sizes, plan.TargetOptions{
		BaseLevelSize:       lm.opt.BaseLevelSize,
		LevelSizeMultiplier: lm.opt.LevelSizeMultiplier,
		BaseTableSize:       lm.opt.BaseTableSize,
		TableSizeMultiplier: lm.opt.TableSizeMultiplier,
		MemTableSize:        lm.opt.MemTableSize,
	})
}

// =====  Compaction pacer (was compaction_pacer.go) =====

// compactionPacerForBuild returns the active pacer for a new build, or nil
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
func (lm *levelManager) compactionPacerForBuild() *pacer.Pacer {
	if lm == nil || lm.compactionPacer == nil {
		return nil
	}
	if lm.compactionPacerBypassActive() {
		return nil
	}
	return lm.compactionPacer
}

// compactionPacerBypassActive reports whether L0 has reached the configured
// bypass threshold. When true, new compaction builds run unpaced so that L0
// can be drained quickly enough to avoid foreground write stalls.
func (lm *levelManager) compactionPacerBypassActive() bool {
	if lm == nil || lm.opt == nil || lm.opt.CompactionPacingBypassL0 <= 0 || len(lm.levels) == 0 || lm.levels[0] == nil {
		return false
	}
	return lm.levels[0].numTables() >= lm.opt.CompactionPacingBypassL0
}

// CompactionPacerStats exposes pacer observability for diagnostics. Returns
// zero stats when pacing is disabled.
func (lm *levelManager) CompactionPacerStats() pacer.Stats {
	if lm == nil {
		return pacer.Stats{}
	}
	return lm.compactionPacer.Stats()
}

// =====  Planner glue (was planner.go) =====

// resolvePlanLocked binds plan tables; caller must hold cd level locks.
func (lm *levelManager) resolvePlanLocked(cd *compactDef) bool {
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
func (lm *levelManager) fillTables(cd *compactDef) bool {
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
			if !lm.resolvePlanLocked(cd) {
				return false
			}
			return lm.compactState.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
		}
		return false
	}
	tables := make([]*table, cd.thisLevel.numTablesLocked())
	copy(tables, cd.thisLevel.tables)
	// We're doing a maxLevel to maxLevel compaction. Pick tables based on the stale data size.
	if cd.thisLevel.isLastLevel() {
		return lm.fillMaxLevelTables(tables, cd)
	}
	p, ok := plan.ForRegular(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

func (lm *levelManager) fillTablesLandingShard(cd *compactDef, shardIdx int) bool {
	cd.lockLevels()
	defer cd.unlockLevels()

	totalLanding := cd.thisLevel.numLandingTablesLocked()
	if totalLanding == 0 {
		return false
	}
	batchSize := lm.opt.LandingCompactBatchSize
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
	p, ok := plan.ForLandingShard(cd.thisLevel.levelNum, shMeta, cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), cd.targetFileSize(), batchSize, lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// resolveTablesLocked maps IDs to tables; caller must hold lh lock.
func resolveTablesLocked(lh *levelHandler, ids []uint64, landing bool) []*table {
	if lh == nil || len(ids) == 0 {
		return nil
	}
	var tables []*table
	if landing {
		tables = lh.landing.AllTables()
	} else {
		tables = lh.tables
	}
	if len(tables) == 0 {
		return nil
	}
	byID := make(map[uint64]*table, len(tables))
	for _, t := range tables {
		if t != nil {
			byID[t.fid] = t
		}
	}
	out := make([]*table, 0, len(ids))
	for _, id := range ids {
		t, ok := byID[id]
		if !ok {
			return nil
		}
		out = append(out, t)
	}
	return out
}

func tableMetaSnapshot(tables []*table) []plan.TableMeta {
	if len(tables) == 0 {
		return nil
	}
	out := make([]plan.TableMeta, 0, len(tables))
	for _, t := range tables {
		if t == nil {
			continue
		}
		meta := plan.TableMeta{
			ID:         t.fid,
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

func findTableByID(tables []*table, fid uint64) *table {
	for _, t := range tables {
		if t.fid == fid {
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
func (lm *levelManager) addSplits(cd *compactDef) {
	cd.splits = cd.splits[:0]

	maxSplits := 5
	if lm != nil && lm.opt != nil && lm.opt.NumCompactors > maxSplits {
		maxSplits = lm.opt.NumCompactors
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
func (lm *levelManager) fillMaxLevelTables(tables []*table, cd *compactDef) bool {
	var ttlMinAge time.Duration
	if lm != nil && lm.opt != nil {
		ttlMinAge = lm.opt.TTLCompactionMinAge
	}
	p, ok := plan.ForMaxLevel(cd.thisLevel.levelNum, tableMetaSnapshot(tables), cd.spec.ThisFileSize, lm.compactState, time.Now(), ttlMinAge)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// fillTablesL0 tries L0->Lbase first, then falls back to L0->L0.
func (lm *levelManager) fillTablesL0(cd *compactDef) bool {
	if ok := lm.fillTablesL0ToLbase(cd); ok {
		return true
	}
	return lm.fillTablesL0ToL0(cd)
}

func (lm *levelManager) moveToLanding(cd *compactDef) error {
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
			File: &manifest.FileMeta{FileID: tbl.fid, Level: cd.thisLevel.levelNum},
		}
		edits = append(edits, del)
		add := manifest.Edit{
			Type: manifest.EditAddFile,
			File: &manifest.FileMeta{
				Level:     cd.nextLevel.levelNum,
				FileID:    tbl.fid,
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
	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		return err
	}

	toDel := make(map[uint64]struct{}, len(cd.top))
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		toDel[tbl.fid] = struct{}{}
	}

	// Update in-memory state atomically across the source and target levels to avoid
	// a visibility gap for readers walking L0 -> Ln.
	first, second := cd.thisLevel, cd.nextLevel
	if first.levelNum > second.levelNum {
		first, second = second, first
	}
	first.Lock()
	second.Lock()
	var remaining []*table
	for _, tbl := range cd.thisLevel.tables {
		if _, found := toDel[tbl.fid]; found {
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
		t.setLevel(cd.nextLevel.levelNum)
	}
	cd.nextLevel.landing.AddBatch(cd.top)
	cd.nextLevel.landing.SortShards()
	second.Unlock()
	first.Unlock()

	if lm.compaction != nil {
		lm.compaction.Trigger()
	}
	return nil
}

func (lm *levelManager) fillTablesL0ToLbase(cd *compactDef) bool {
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
	p, ok := plan.ForL0ToLbase(tableMetaSnapshot(top), cd.nextLevel.levelNum, tableMetaSnapshot(cd.nextLevel.tables), lm.compactState)
	if !ok {
		return false
	}
	cd.applyPlan(p)
	if !lm.resolvePlanLocked(cd) {
		return false
	}
	return lm.compactState.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// fillTablesL0ToL0 performs L0->L0 compaction.
//
// Multiple compactor workers may invoke this concurrently — plan.ForL0ToL0
// caps each call at l0ToL0MaxTablesPerWorker tables and skips tables
// already claimed by state.HasTable, so workers naturally partition the
// available L0 SSTs. The plan is marked IntraLevel so the state machine
// claims by table ID without registering an InfRange that would block
// peer L0→Lbase compactions.
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
	cd.nextLevel = lm.levels[0]
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
	lm.levels[0].RLock()
	defer lm.levels[0].RUnlock()

	top := cd.thisLevel.tables
	now := time.Now()
	p, ok := plan.ForL0ToL0(cd.thisLevel.levelNum, tableMetaSnapshot(top), cd.spec.ThisFileSize, lm.compactState, now)
	if !ok {
		// Skip when fewer than four tables qualify.
		return false
	}
	cd.applyPlan(p)
	if !lm.resolvePlanLocked(cd) {
		return false
	}

	// L0->L0 compaction collapses into a single file, reducing L0 count and read amplification.
	cd.spec.ThisFileSize = math.MaxUint32
	cd.spec.NextFileSize = cd.spec.ThisFileSize
	return lm.compactState.CompareAndAdd(plan.LevelsLocked{}, cd.stateEntry())
}

// getKeyRange returns the merged min/max key range for a set of live tables.
func getKeyRange(tables ...*table) plan.KeyRange {
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
func (lm *levelManager) doCompact(id int, p plan.Priority) (retErr error) {
	l := p.Level
	utils.CondPanicFunc(l >= lm.opt.MaxLevelNum, func() error { return errors.New("[doCompact] Sanity check. l >= lm.opt.MaxLevelNum") }) // Sanity check.
	t := p.Target
	if t.BaseLevel == 0 {
		t = lm.levelTargets()
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
		thisLevel: lm.levels[l],
		adjusted:  p.Adjusted,
	}

	var cleanup bool
	defer func() {
		if cleanup {
			if err := lm.compactState.Delete(cd.stateEntry()); err != nil {
				lm.getLogger().Warn("failed to cleanup compaction state", "worker", id, "err", err)
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
		baseLimit := lm.opt.LandingShardParallelism
		if baseLimit <= 0 {
			baseLimit = max(lm.opt.NumCompactors/2, 1)
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
			if !lm.fillTablesLandingShard(&sub, order[i]) {
				continue
			}
			sub.spec.LandingMode = p.LandingMode
			sub.spec.StatsTag = p.StatsTag
			if err := lm.runCompactDef(id, l, sub); err != nil {
				lm.getLogger().Error("landing compaction failed", "worker", id, "err", err, "def", sub)
				if stateDelErr := lm.compactState.Delete(sub.stateEntry()); stateDelErr != nil {
					return errors.Join(err, stateDelErr)
				}
				return err
			}
			if err := lm.compactState.Delete(sub.stateEntry()); err != nil {
				return err
			}
			ran = true
			lm.getLogger().Info("landing compaction complete", "worker", id, "level", sub.thisLevel.levelNum, "shard", order[i])
		}
		if !ran {
			return ErrFillTables
		}
		return nil
	}

	// L0 uses a dedicated selection path.
	if l == 0 {
		cd.setNextLevel(t, lm.levels[t.BaseLevel])
		if !lm.fillTablesL0(&cd) {
			return ErrFillTables
		}
		cleanup = true
		if cd.nextLevel.levelNum != 0 {
			if err := lm.moveToLanding(&cd); err != nil {
				lm.getLogger().Error("move to landing failed", "worker", id, "err", err, "def", cd)
				return err
			}
			lm.getLogger().Info("moved L0 tables to landing buffer", "worker", id, "tables", len(cd.top), "target_level", cd.nextLevel.levelNum)
			return nil
		}
	} else {
		cd.setNextLevel(t, cd.thisLevel)
		// For non-last levels, compact into the next level.
		if !cd.thisLevel.isLastLevel() {
			cd.setNextLevel(t, lm.levels[l+1])
		}
		if !lm.fillTables(&cd) {
			return ErrFillTables
		}
		cleanup = true
		if lm.canMoveToNextLevel(&cd) {
			if err := lm.moveToNextLevel(&cd); err != nil {
				lm.getLogger().Error("trivial move failed", "worker", id, "err", err, "def", cd)
				return err
			}
			lm.getLogger().Info("trivial move complete", "worker", id, "from_level", cd.thisLevel.levelNum, "to_level", cd.nextLevel.levelNum, "tables", len(cd.top))
			return nil
		}
		// Continue with the normal merge path.
		if err := lm.runCompactDef(id, l, cd); err != nil {
			lm.getLogger().Error("compaction failed", "worker", id, "err", err, "def", cd)
			return err
		}
		lm.getLogger().Info("compaction complete", "worker", id, "level", cd.thisLevel.levelNum)
		return nil
	}

	// Execute the merge plan.
	if err := lm.runCompactDef(id, l, cd); err != nil {
		// This compaction couldn't be done successfully.
		lm.getLogger().Error("compaction failed", "worker", id, "err", err, "def", cd)
		return err
	}
	lm.getLogger().Info("compaction complete", "worker", id, "level", cd.thisLevel.levelNum)
	return nil
}

func (lm *levelManager) runCompactDef(id, l int, cd compactDef) (err error) {
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
		lm.addSplits(&cd)
	}
	// Append an empty range placeholder when no split is found.
	if len(cd.splits) == 0 {
		cd.splits = append(cd.splits, plan.KeyRange{})
	}

	newTables, decr, err := lm.compactBuildTables(l, cd)
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
		levelByID[t.fid] = cd.thisLevel.levelNum
	}
	for _, t := range cd.bot {
		levelByID[t.fid] = cd.nextLevel.levelNum
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
					FileID:    tbl.fid,
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
	if err := lm.manifestMgr.LogEdits(manifestEdits...); err != nil {
		return err
	}
	cleanupNeeded = false

	if cd.spec.LandingMode == plan.LandingKeep {
		if err := thisLevel.replaceLandingTables(cd.top, newTables); err != nil {
			return err
		}
		if thisLevel.levelNum > 0 {
			thisLevel.Sort()
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
		lm.getLogger().Info(
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
	lm.recordCompactionMetrics(time.Since(timeStart))
	// After max-level compaction, range tombstone layout may change.
	// Rebuild the in-memory range tombstone index to keep read visibility correct.
	if cd.nextLevel != nil && cd.nextLevel.levelNum == lm.opt.MaxLevelNum-1 && lm.rtCollector != nil {
		lm.rebuildRangeTombstones()
	}
	return nil
}

func (lm *levelManager) canMoveToNextLevel(cd *compactDef) bool {
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

func (lm *levelManager) moveToNextLevel(cd *compactDef) error {
	if !lm.canMoveToNextLevel(cd) {
		return errors.New("invalid compaction definition for trivial move")
	}
	var edits []manifest.Edit
	for _, tbl := range cd.top {
		if tbl == nil {
			continue
		}
		edits = append(edits, manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &manifest.FileMeta{FileID: tbl.fid, Level: cd.thisLevel.levelNum},
		})
		add := manifest.Edit{
			Type: manifest.EditAddFile,
			File: &manifest.FileMeta{
				Level:     cd.nextLevel.levelNum,
				FileID:    tbl.fid,
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
	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		return err
	}

	toMove := make(map[uint64]*table, len(cd.top))
	for _, tbl := range cd.top {
		if tbl != nil {
			toMove[tbl.fid] = tbl
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
		if _, found := toMove[tbl.fid]; found {
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
		tbl.setLevel(cd.nextLevel.levelNum)
		cd.nextLevel.addSize(tbl)
		cd.nextLevel.tables = append(cd.nextLevel.tables, tbl)
	}
	cd.nextLevel.refreshTableIndexesLocked()

	second.Unlock()
	first.Unlock()
	return nil
}

// tablesToString
func tablesToString(tables []*table) []string {
	var res []string
	for _, t := range tables {
		res = append(res, fmt.Sprintf("%05d", t.fid))
	}
	res = append(res, ".")
	return res
}

// buildChangeSet _
func buildChangeSet(cd *compactDef, newTables []*table) storagepb.ManifestChangeSet {
	changes := []*storagepb.ManifestChange{}
	for _, table := range newTables {
		changes = append(changes, newCreateChange(table.fid, cd.nextLevel.levelNum))
	}
	for _, table := range cd.top {
		changes = append(changes, newDeleteChange(table.fid))
	}
	for _, table := range cd.bot {
		changes = append(changes, newDeleteChange(table.fid))
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
func (lm *levelManager) compactBuildTables(lev int, cd compactDef) ([]*table, func() error, error) {

	topTables := append([]*table(nil), cd.top...)
	botTables := append([]*table(nil), cd.bot...)
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
			return append(iters, NewConcatIterator(botTables, iterOpt))
		}
		// Fallback for overlapping/out-of-order next-level windows.
		// ConcatIterator assumes strict non-overlap; merge keeps global key ordering.
		return append(iters, iteratorsReversed(botTables, iterOpt)...)
	}

	// Start parallel compaction tasks.
	res := make(chan *table, 3)
	// Throttle inflight builders to bound memory and file handles.
	inflightBuilders := utils.NewThrottle(8 + len(cd.splits))
	for _, kr := range cd.splits {
		if err := inflightBuilders.Go(func() error {
			it := NewMergeIterator(newIterator(), false)
			defer func() { _ = it.Close() }()
			lm.subcompact(it, kr, cd, inflightBuilders, res)
			return nil
		}); err != nil {
			return nil, nil, fmt.Errorf("cannot start subcompaction: %+v", err)
		}
	}

	// Collect table handles via fan-in.
	var newTables []*table
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

	if err == nil && lm.opt.ManifestSync {
		// Strict durability mode: persist new SST directory entries before manifest edits.
		err = vfs.SyncDir(lm.opt.FS, lm.opt.WorkDir)
	}

	if err != nil {
		// On error, delete newly created files.
		_ = decrRefs(newTables)
		return nil, nil, fmt.Errorf("while running compactions for: %+v, %v", cd, err)
	}

	sort.Slice(newTables, func(i, j int) bool {
		return kv.CompareInternalKeys(newTables[i].MaxKey(), newTables[j].MaxKey()) < 0
	})
	return newTables, func() error { return decrRefs(newTables) }, nil
}

func iteratorsReversed(th []*table, opt *index.Options) []index.Iterator {
	out := make([]index.Iterator, 0, len(th))
	for i := len(th) - 1; i >= 0; i-- {
		// This will increment the reference of the table handler.
		out = append(out, th[i].NewIterator(opt))
	}
	return out
}

// tablesStrictlyOrdered reports whether consecutive tables are in strictly
// increasing, non-overlapping user-key order.
func tablesStrictlyOrdered(tables []*table) bool {
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
func (lm *levelManager) subcompact(it index.Iterator, kr plan.KeyRange, cd compactDef,
	inflightBuilders *utils.Throttle, res chan<- *table) {
	var lastKey []byte

	// Keep tombstone state across builder splits.
	rtTracker := tombstone.NewCompactionTracker()

	addKeys := func(builder *tableBuilder) {
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
		builderOpt := lm.opt.Clone()
		builder := newTableBuilerWithSSTSize(builderOpt, cd.spec.NextFileSize)
		builder.pacer = lm.compactionPacerForBuild()

		// This would do the iteration and add keys to builder.
		addKeys(builder)

		// It was true that it.Valid() at least once in the loop above, which means we
		// called Add() at least once, and builder is not Empty().
		if builder.empty() {
			// Cleanup builder resources:
			_, _ = builder.finish()
			builder.Close()
			continue
		}
		// Leverage SSD parallel write throughput.
		b := builder
		if err := inflightBuilders.Go(func() error {
			defer b.Close()
			newFID := lm.maxFID.Add(1) // Compaction does not allocate memtables; advance maxFID.
			sstName := vfs.FileNameSSTable(lm.opt.WorkDir, newFID)
			tbl, err := openTable(lm, sstName, b)
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
