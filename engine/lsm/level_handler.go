package lsm

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/iterator"
	"github.com/feichai0017/NoKV/engine/lsm/landing"
	"github.com/feichai0017/NoKV/engine/lsm/plan"
	"github.com/feichai0017/NoKV/engine/lsm/rangefilter"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
)

// landingBuffer is the concrete landing buffer instantiation used by
// levelHandler. It binds landing.Buffer to the lsm package's *table.
type landingBuffer = landing.Buffer[*table.Table]

// landingShardCount mirrors landing.ShardCount for callers that index shards
// directly inside the lsm package.
const landingShardCount = landing.ShardCount

// landingPickInput converts the landing-package shard summaries into the
// shape expected by the compaction-plan picker. Both types are structurally
// identical; the conversion stays in the lsm adapter so the landing package
// does not need to import plan.
func landingPickInput(views []landing.ShardView) plan.LandingPickInput {
	out := make([]plan.LandingShardView, 0, len(views))
	for _, v := range views {
		out = append(out, plan.LandingShardView(v))
	}
	return plan.LandingPickInput{Shards: out}
}

type levelHandler struct {
	sync.RWMutex
	levelNum                    int
	tables                      []*table.Table
	filter                      rangefilter.Filter[*table.Table]
	landing                     landingBuffer
	totalSize                   int64
	totalStaleSize              int64
	totalValueSize              int64
	lm                          *levelManager
	landingRuns                 atomic.Uint64
	landingMergeRuns            atomic.Uint64
	landingDurationNs           atomic.Int64
	landingMergeDurationNs      atomic.Int64
	landingTablesCompactedCount atomic.Uint64
	landingMergeTables          atomic.Uint64

	// l0Sublevels groups L0 tables into non-overlapping sublevels for point
	// reads. Only populated when levelNum == 0; nil otherwise. Rebuilt by
	// sortTablesLocked().
	l0Sublevels []l0Sublevel
}

type tableRange struct {
	min []byte
	max []byte
	tbl *table.Table
}

func (lh *levelHandler) close() error {
	lh.RLock()
	tables := append([]*table.Table(nil), lh.tables...)
	landingTables := append([]*table.Table(nil), lh.landing.AllTables()...)
	lh.RUnlock()

	var closeErr error
	for _, t := range tables {
		if t == nil {
			continue
		}
		closeErr = errors.Join(closeErr, t.CloseHandle())
	}
	for _, t := range landingTables {
		if t == nil {
			continue
		}
		closeErr = errors.Join(closeErr, t.CloseHandle())
	}
	return closeErr
}
func (lh *levelHandler) add(t *table.Table) {
	if t == nil {
		return
	}
	lh.Lock()
	defer lh.Unlock()
	t.SetLevel(lh.levelNum)
	lh.tables = append(lh.tables, t)
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
	lh.totalValueSize += int64(t.ValueSize())
	lh.refreshTableIndexesLocked()
}

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize + lh.landing.TotalSize()
}

func (lh *levelHandler) getTotalValueSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalValueSize + lh.landing.TotalValueSize()
}

func (lh *levelHandler) keyCount() uint64 {
	lh.RLock()
	defer lh.RUnlock()
	var total uint64
	for _, t := range lh.tables {
		if t != nil {
			total += uint64(t.CachedKeyCount())
		}
	}
	for _, t := range lh.landing.AllTables() {
		if t != nil {
			total += uint64(t.CachedKeyCount())
		}
	}
	return total
}

func (lh *levelHandler) rangeTombstoneCount() uint64 {
	lh.RLock()
	defer lh.RUnlock()
	var total uint64
	for _, t := range lh.tables {
		if t != nil {
			total += uint64(t.RangeTombstoneCount())
		}
	}
	for _, t := range lh.landing.AllTables() {
		if t != nil {
			total += uint64(t.RangeTombstoneCount())
		}
	}
	return total
}

func (lh *levelHandler) addSize(t *table.Table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
	lh.totalValueSize += int64(t.ValueSize())
}

func (lh *levelHandler) subtractSize(t *table.Table) {
	lh.totalSize -= t.Size()
	lh.totalStaleSize -= int64(t.StaleDataSize())
	lh.totalValueSize -= int64(t.ValueSize())
	if lh.totalValueSize < 0 {
		lh.totalValueSize = 0
	}
}

func (lh *levelHandler) mainValueBytes() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalValueSize
}

func (lh *levelHandler) metricsSnapshot() metrics.LevelMetrics {
	if lh == nil {
		return metrics.LevelMetrics{}
	}
	lh.RLock()
	defer lh.RUnlock()
	return metrics.LevelMetrics{
		Level:                  lh.levelNum,
		TableCount:             len(lh.tables),
		SizeBytes:              lh.totalSize,
		ValueBytes:             lh.totalValueSize,
		StaleBytes:             lh.totalStaleSize,
		LandingTableCount:      lh.landing.TableCount(),
		LandingSizeBytes:       lh.landing.TotalSize(),
		LandingValueBytes:      lh.landing.TotalValueSize(),
		ValueDensity:           lh.densityLocked(),
		LandingValueDensity:    lh.landingDensityLocked(),
		LandingRuns:            int64(lh.landingRuns.Load()),
		LandingMs:              float64(lh.landingDurationNs.Load()) / 1e6,
		LandingTablesCompacted: int64(lh.landingTablesCompactedCount.Load()),
		LandingMergeRuns:       int64(lh.landingMergeRuns.Load()),
		LandingMergeMs:         float64(lh.landingMergeDurationNs.Load()) / 1e6,
		LandingMergeTables:     int64(lh.landingMergeTables.Load()),
	}
}

// densityLocked computes value density; caller must hold lh lock.
func (lh *levelHandler) densityLocked() float64 {
	if lh.totalSize <= 0 {
		return 0
	}
	return float64(lh.totalValueSize) / float64(lh.totalSize)
}

func (lh *levelHandler) numTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return len(lh.tables)
}

// numTablesLocked returns len(lh.tables) without acquiring the lock.
// Caller must already hold at least a read lock.
func (lh *levelHandler) numTablesLocked() int {
	return len(lh.tables)
}

// Get finds key inside this level, considering landing shards and level semantics.
func (lh *levelHandler) Get(key []byte) (*kv.Entry, error) {
	lh.RLock()
	defer lh.RUnlock()
	if lh.levelNum == 0 {
		return lh.searchL0SST(key)
	}
	var (
		best   *kv.Entry
		maxVer uint64
	)
	if entry, err := lh.landing.Search(key, &maxVer); err == nil {
		best = entry
	} else if err != utils.ErrKeyNotFound {
		return nil, err
	}
	if entry, err := lh.searchLNSST(key, &maxVer); err == nil {
		if best != nil {
			best.DecrRef()
		}
		best = entry
	} else if err != utils.ErrKeyNotFound {
		if best != nil {
			best.DecrRef()
		}
		return nil, err
	}
	if best != nil {
		return best, nil
	}
	return nil, utils.ErrKeyNotFound
}

// Sort orders tables for lookup/compaction; L0 by file id, Ln by key range.
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	lh.refreshTableIndexesLocked()
	lh.landing.SortShards()
}

// sortTablesLocked sorts lh.tables using level-specific ordering semantics.
// Caller must hold lh's mutex.
func (lh *levelHandler) sortTablesLocked() {
	if lh.levelNum == 0 {
		// L0 key ranges may overlap, so ordering follows file creation order.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].FID() < lh.tables[j].FID()
		})
		// Rebuild sublevel layout for the read path. Compaction picker still
		// reads lh.tables directly; sublevels exist only to accelerate Get.
		lh.l0Sublevels = buildL0Sublevels(lh.tables)
		return
	}
	// L1+ tables are non-overlapping by key range.
	sort.Slice(lh.tables, func(i, j int) bool {
		return kv.CompareInternalKeys(lh.tables[i].MinKey(), lh.tables[j].MinKey()) < 0
	})
}

func (lh *levelHandler) searchL0SST(key []byte) (*kv.Entry, error) {
	var (
		version uint64
		best    *kv.Entry
	)
	candidates := append([]*table.Table(nil), lh.selectTablesForKey(key, true)...)
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i] == nil {
			return false
		}
		if candidates[j] == nil {
			return true
		}
		return candidates[i].FID() > candidates[j].FID()
	})
	for _, tbl := range candidates {
		if tbl == nil {
			continue
		}
		if kv.CompareBaseKeys(key, tbl.MinKey()) < 0 ||
			kv.CompareBaseKeys(key, tbl.MaxKey()) > 0 {
			continue
		}
		if tbl.MaxVersionVal() <= version {
			continue
		}
		if entry, err := tbl.Search(key, &version); err == nil {
			if best != nil {
				best.DecrRef()
			}
			best = entry
			continue
		} else if err != utils.ErrKeyNotFound {
			if best != nil {
				best.DecrRef()
			}
			return nil, err
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) searchLNSST(key []byte, maxVersion *uint64) (*kv.Entry, error) {
	if maxVersion == nil {
		var tmp uint64
		maxVersion = &tmp
	}
	if lh.levelNum > 0 && lh.filter.SpanCount() >= rangefilter.MinSpanCount && lh.filter.NonOverlapping() {
		total := len(lh.tables)
		tbl, ok := lh.filter.TableForPoint(key)
		if lh.lm != nil {
			candidates := 0
			if ok {
				candidates = 1
			}
			lh.lm.recordRangeFilterPoint(total, candidates, false)
		}
		if !ok {
			return nil, utils.ErrKeyNotFound
		}
		if tbl.MaxVersionVal() <= *maxVersion {
			return nil, utils.ErrKeyNotFound
		}
		return tbl.SearchExactCandidate(key, maxVersion)
	}
	tables := lh.selectTablesForKey(key, true)
	if len(tables) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	var best *kv.Entry
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		if tbl.MaxVersionVal() <= *maxVersion {
			continue
		}
		var (
			entry *kv.Entry
			err   error
		)
		entry, err = tbl.Search(key, maxVersion)
		if err == nil {
			if best != nil {
				best.DecrRef()
			}
			best = entry
			continue
		}
		if err != utils.ErrKeyNotFound {
			if best != nil {
				best.DecrRef()
			}
			return nil, err
		}
	}
	if best != nil {
		return best, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (lh *levelHandler) getTableForKey(key []byte) *table.Table {
	if lh.levelNum > 0 && lh.filter.SpanCount() >= rangefilter.MinSpanCount && lh.filter.NonOverlapping() {
		tbl, _ := lh.filter.TableForPoint(key)
		return tbl
	}
	tables := lh.selectTablesForKey(key, false)
	if len(tables) == 0 {
		return nil
	}
	return tables[0]
}

func (lh *levelHandler) selectTablesForKey(key []byte, record bool) []*table.Table {
	if len(lh.tables) == 0 {
		return nil
	}
	total := len(lh.tables)
	fallback := false
	var tables []*table.Table
	if lh.levelNum == 0 {
		// L0 first tries the sublevel index, falling back to a linear scan
		// if sublevels have not been built yet (e.g. between mutations).
		tables = l0CandidateTables(lh.l0Sublevels, key)
		if tables == nil {
			fallback = true
			tables = lh.getTablesForKeyLinear(key)
		}
	} else if lh.filter.SpanCount() < rangefilter.MinSpanCount {
		fallback = true
		tables = lh.getTablesForKeyLinear(key)
	} else {
		if !lh.filter.NonOverlapping() {
			fallback = true
		}
		tables = lh.filter.TablesForPoint(key)
	}
	if record && lh.lm != nil {
		lh.lm.recordRangeFilterPoint(total, len(tables), fallback)
	}
	return tables
}

func (lh *levelHandler) getTablesForKeyLinear(key []byte) []*table.Table {
	if len(lh.tables) == 0 {
		return nil
	}
	if lh.levelNum > 0 && kv.CompareBaseKeys(key, lh.tables[0].MinKey()) < 0 {
		return nil
	}
	out := make([]*table.Table, 0, 1)
	for _, t := range lh.tables {
		if t == nil {
			continue
		}
		if lh.levelNum > 0 && kv.CompareBaseKeys(t.MinKey(), key) > 0 {
			break
		}
		if kv.CompareBaseKeys(key, t.MaxKey()) <= 0 &&
			kv.CompareBaseKeys(key, t.MinKey()) >= 0 {
			out = append(out, t)
		}
	}
	return out
}

func (lh *levelHandler) selectTablesForBounds(lower, upper []byte, record bool) []*table.Table {
	if len(lh.tables) == 0 {
		if record && lh.lm != nil && (len(lower) > 0 || len(upper) > 0) {
			lh.lm.recordRangeFilterBounded(0, 0, false)
		}
		return nil
	}
	total := len(lh.tables)
	fallback := false
	var tables []*table.Table
	if lh.levelNum == 0 || lh.filter.SpanCount() < rangefilter.MinSpanCount {
		fallback = true
		tables = rangefilter.FilterByBounds(lh.tables, lower, upper)
	} else {
		if lh.levelNum > 0 && !lh.filter.NonOverlapping() {
			fallback = true
		}
		tables = lh.filter.TablesForBounds(lower, upper)
	}
	if record && lh.lm != nil && (len(lower) > 0 || len(upper) > 0) {
		lh.lm.recordRangeFilterBounded(total, len(tables), fallback)
	}
	return tables
}

func (lh *levelHandler) rebuildRangeFilterLocked() {
	lh.filter = rangefilter.Build(lh.levelNum, lh.tables)
}

func (lh *levelHandler) refreshTableIndexesLocked() {
	lh.sortTablesLocked()
	lh.rebuildRangeFilterLocked()
}

func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (lh *levelHandler) replaceTables(toDel, toAdd []*table.Table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	lh.Lock() // We s.Unlock() below.

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.FID()] = struct{}{}
	}
	var removed []*table.Table
	var newTables []*table.Table
	for _, t := range lh.tables {
		_, found := toDelMap[t.FID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		removed = append(removed, t)
		lh.subtractSize(t)
	}

	// Increase totalSize first.
	for _, t := range toAdd {
		lh.addSize(t)
		t.SetLevel(lh.levelNum)
		newTables = append(newTables, t)
	}

	// Assign tables.
	lh.tables = newTables
	lh.refreshTableIndexesLocked()
	lh.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return table.DecrAll(removed)
}

// deleteTables remove tables idx0, ..., idx1-1.
func (lh *levelHandler) deleteTables(toDel []*table.Table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.FID()] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var removed []*table.Table
	var newTables []*table.Table
	for _, t := range lh.tables {
		_, found := toDelMap[t.FID()]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		removed = append(removed, t)
		lh.subtractSize(t)
	}
	lh.tables = newTables
	lh.refreshTableIndexesLocked()

	lh.landing.Remove(toDelMap)

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return table.DecrAll(removed)
}

func (lh *levelHandler) deleteLandingTables(toDel []*table.Table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.FID()] = struct{}{}
	}
	removed := lh.collectLandingTablesLocked(toDelMap)

	lh.landing.Remove(toDelMap)

	lh.Unlock()

	return table.DecrAll(removed)
}

func (lh *levelHandler) replaceLandingTables(toDel, toAdd []*table.Table) error {
	lh.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		if t == nil {
			continue
		}
		toDelMap[t.FID()] = struct{}{}
	}
	removed := lh.collectLandingTablesLocked(toDelMap)
	lh.landing.Remove(toDelMap)
	if len(toAdd) > 0 {
		lh.landing.AddBatch(toAdd)
	}

	lh.Unlock()

	return table.DecrAll(removed)
}

func (lh *levelHandler) collectLandingTablesLocked(fidSet map[uint64]struct{}) []*table.Table {
	if len(fidSet) == 0 {
		return nil
	}
	var out []*table.Table
	for _, t := range lh.landing.AllTables() {
		if t == nil {
			continue
		}
		if _, ok := fidSet[t.FID()]; ok {
			out = append(out, t)
		}
	}
	return out
}

func (lh *levelHandler) recordLandingMetrics(merge bool, duration time.Duration, tables int) {
	if tables < 0 {
		tables = 0
	}
	if merge {
		lh.landingMergeRuns.Add(1)
		lh.landingMergeDurationNs.Add(duration.Nanoseconds())
		if tables > 0 {
			lh.landingMergeTables.Add(uint64(tables))
		}
		return
	}
	lh.landingRuns.Add(1)
	lh.landingDurationNs.Add(duration.Nanoseconds())
	if tables > 0 {
		lh.landingTablesCompactedCount.Add(uint64(tables))
	}
}

func (lh *levelHandler) iterators(opt *index.Options) []index.Iterator {
	topt := &index.Options{}
	if opt != nil {
		*topt = *opt
	}
	lh.RLock()
	defer lh.RUnlock()
	bounded := len(topt.LowerBound) > 0 || len(topt.UpperBound) > 0
	mainTables := lh.selectTablesForBounds(topt.LowerBound, topt.UpperBound, false)
	if lh.levelNum == 0 {
		if bounded && lh.lm != nil {
			lh.lm.recordRangeFilterBounded(len(lh.tables), len(mainTables), true)
		}
		return iteratorsReversed(mainTables, topt)
	}

	var itrs []index.Iterator
	landingTables := lh.landing.TablesWithinBounds(topt.LowerBound, topt.UpperBound)
	itrs = append(itrs, iteratorsReversed(landingTables, topt)...)
	if len(mainTables) == 1 {
		itrs = append(itrs, mainTables[0].NewIterator(topt))
	} else if len(mainTables) > 1 {
		itrs = append(itrs, iterator.NewConcatIterator(mainTables, topt))
	}
	if bounded && lh.lm != nil {
		total := len(lh.tables) + lh.landing.TableCount()
		candidates := len(mainTables) + len(landingTables)
		fallback := lh.filter.SpanCount() == 0
		if lh.levelNum > 0 && lh.filter.SpanCount() > 0 && !lh.filter.NonOverlapping() {
			fallback = true
		}
		lh.lm.recordRangeFilterBounded(total, candidates, fallback)
	}
	return itrs
}

// ---- Landing-buffer accessors on levelHandler ----
//
// All landing accessors live here because the landing buffer is owned by
// levelHandler and shares its RWMutex. The underlying landing.Buffer has
// no internal locking; these methods encapsulate that convention.

func (lh *levelHandler) landingShardByBacklog() int {
	lh.landing.EnsureInit()
	return plan.PickShardByBacklog(landingPickInput(lh.landing.ShardViews()))
}

func (lh *levelHandler) landingShardOrderBySize() []int {
	lh.landing.EnsureInit()
	return plan.PickShardOrder(landingPickInput(lh.landing.ShardViews()))
}

// addLanding registers a table into the landing buffer under lh's write lock.
func (lh *levelHandler) addLanding(t *table.Table) {
	if t == nil {
		return
	}
	lh.Lock()
	defer lh.Unlock()
	lh.landing.EnsureInit()
	t.SetLevel(lh.levelNum)
	lh.landing.Add(t)
}

func (lh *levelHandler) landingValueBytes() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.TotalValueSize()
}

func (lh *levelHandler) landingValueDensity() float64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landingDensityLocked()
}

// landingDensityLocked computes landing value density; caller must hold lh lock.
func (lh *levelHandler) landingDensityLocked() float64 {
	total := lh.landing.TotalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.landing.TotalValueSize()) / float64(total)
}

func (lh *levelHandler) maxLandingAgeSeconds() float64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.MaxAgeSeconds()
}

func (lh *levelHandler) numLandingTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.TableCount()
}

// numLandingTablesLocked returns the landing table count without acquiring the
// lock. Caller must already hold at least a read lock.
func (lh *levelHandler) numLandingTablesLocked() int {
	return lh.landing.TableCount()
}

func (lh *levelHandler) landingDataSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.landing.TotalSize()
}

// ---- L0 sublevels ----
//
// L0 sublevels group overlapping L0 tables into stripes of non-overlapping
// ranges so a point read can binary-search to a single candidate per stripe
// instead of scanning every L0 table. Compaction picker still treats L0 as
// one physical level; sublevels exist only to accelerate Get.

// l0Sublevel groups L0 tables whose key ranges do not overlap. Within one
// sublevel the tables are sorted ascending by MinKey, so a point read can
// binary-search to a single candidate per sublevel instead of scanning every
// L0 table.
//
// Sublevels are a Phase A read-path optimization: compaction picker and
// trivial move still treat L0 as a single physical level. The sublevel layout
// is rebuilt eagerly inside sortTablesLocked() each time L0 mutates so reads
// always see a consistent snapshot.
type l0Sublevel []*table.Table

// buildL0Sublevels arranges tables into the minimum number of sublevels such
// that each sublevel contains only non-overlapping ranges. The greedy
// placement is order-stable: tables sort by (MinKey asc, fid asc), and each
// table goes into the first sublevel whose tail MaxKey strictly precedes the
// table MinKey. New tables (higher fid) tend to fall into higher sublevels.
//
// Complexity: O(N log N) for the sort plus O(N * S) for placement where S is
// the resulting sublevel count. For typical L0 sizes (10-30 tables, 3-6
// sublevels), this is trivially cheap.
func buildL0Sublevels(tables []*table.Table) []l0Sublevel {
	if len(tables) == 0 {
		return nil
	}

	// Copy to avoid mutating the caller slice and filter nil entries.
	sorted := make([]*table.Table, 0, len(tables))
	for _, t := range tables {
		if t != nil {
			sorted = append(sorted, t)
		}
	}
	if len(sorted) == 0 {
		return nil
	}

	sort.Slice(sorted, func(i, j int) bool {
		if c := kv.CompareBaseKeys(sorted[i].MinKey(), sorted[j].MinKey()); c != 0 {
			return c < 0
		}
		return sorted[i].FID() < sorted[j].FID()
	})

	sublevels := make([]l0Sublevel, 0, 4)
	for _, t := range sorted {
		placed := false
		for i := range sublevels {
			tail := sublevels[i][len(sublevels[i])-1]
			if kv.CompareBaseKeys(tail.MaxKey(), t.MinKey()) < 0 {
				sublevels[i] = append(sublevels[i], t)
				placed = true
				break
			}
		}
		if !placed {
			sublevels = append(sublevels, l0Sublevel{t})
		}
	}
	return sublevels
}

// candidate returns the at-most-one table within this sublevel whose key
// range covers key, or nil. Sublevel tables are sorted by MinKey so a binary
// search by MinKey followed by a MaxKey check is enough.
func (s l0Sublevel) candidate(key []byte) *table.Table {
	if len(s) == 0 {
		return nil
	}
	idx := sort.Search(len(s), func(i int) bool {
		return kv.CompareBaseKeys(s[i].MinKey(), key) > 0
	})
	if idx == 0 {
		return nil
	}
	candidate := s[idx-1]
	if kv.CompareBaseKeys(key, candidate.MaxKey()) > 0 {
		return nil
	}
	return candidate
}

// l0GroupHasNoOtherOverlap reports whether the union range of group has no
// overlap with any L0 table outside group. When true, the group can be safely
// promoted to the next level via trivial move because nothing else in L0
// shadows or extends its range.
//
// group is expected to be a set of L0 tables already chosen by the picker
// (typically a contiguous overlapping batch). all is the full set of L0
// tables, including group. Both are read without taking lh's mutex; callers
// must already hold the appropriate level lock.
func l0GroupHasNoOtherOverlap(group, all []*table.Table) bool {
	if len(group) == 0 {
		return false
	}

	groupIDs := make(map[uint64]struct{}, len(group))
	minKey := group[0].MinKey()
	maxKey := group[0].MaxKey()
	for _, t := range group {
		if t == nil {
			return false
		}
		groupIDs[t.FID()] = struct{}{}
		if kv.CompareBaseKeys(t.MinKey(), minKey) < 0 {
			minKey = t.MinKey()
		}
		if kv.CompareBaseKeys(t.MaxKey(), maxKey) > 0 {
			maxKey = t.MaxKey()
		}
	}

	for _, t := range all {
		if t == nil {
			continue
		}
		if _, in := groupIDs[t.FID()]; in {
			continue
		}
		// Non-overlap iff t.MaxKey < group.MinKey or t.MinKey > group.MaxKey.
		if kv.CompareBaseKeys(t.MaxKey(), minKey) < 0 {
			continue
		}
		if kv.CompareBaseKeys(t.MinKey(), maxKey) > 0 {
			continue
		}
		return false
	}
	return true
}

// l0CandidateTables returns up to one candidate table per sublevel whose
// range covers key. The returned slice contains at most len(sublevels) entries
// and may be empty if no sublevel covers the key.
func l0CandidateTables(sublevels []l0Sublevel, key []byte) []*table.Table {
	if len(sublevels) == 0 {
		return nil
	}
	out := make([]*table.Table, 0, len(sublevels))
	for _, sub := range sublevels {
		if t := sub.candidate(key); t != nil {
			out = append(out, t)
		}
	}
	return out
}
