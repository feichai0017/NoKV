package lsm

import (
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

type levelHandler struct {
	sync.RWMutex
	levelNum                    int
	tables                      []*table
	filter                      rangeFilter
	staging                     stagingBuffer
	totalSize                   int64
	totalStaleSize              int64
	totalValueSize              int64
	lm                          *levelManager
	stagingRuns                 atomic.Uint64
	stagingMergeRuns            atomic.Uint64
	stagingDurationNs           atomic.Int64
	stagingMergeDurationNs      atomic.Int64
	stagingTablesCompactedCount atomic.Uint64
	stagingMergeTables          atomic.Uint64

	// l0Sublevels groups L0 tables into non-overlapping sublevels for point
	// reads. Only populated when levelNum == 0; nil otherwise. Rebuilt by
	// sortTablesLocked().
	l0Sublevels []l0Sublevel
}

type tableRange struct {
	min []byte
	max []byte
	tbl *table
}

func (lh *levelHandler) close() error {
	lh.RLock()
	tables := append([]*table(nil), lh.tables...)
	stagingTables := append([]*table(nil), lh.staging.allTables()...)
	lh.RUnlock()

	var closeErr error
	for _, t := range tables {
		if t == nil {
			continue
		}
		closeErr = errors.Join(closeErr, t.closeHandle())
	}
	for _, t := range stagingTables {
		if t == nil {
			continue
		}
		closeErr = errors.Join(closeErr, t.closeHandle())
	}
	return closeErr
}
func (lh *levelHandler) add(t *table) {
	if t == nil {
		return
	}
	lh.Lock()
	defer lh.Unlock()
	t.setLevel(lh.levelNum)
	lh.tables = append(lh.tables, t)
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
	lh.totalValueSize += int64(t.ValueSize())
}

func (lh *levelHandler) getTotalSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalSize + lh.staging.totalSize()
}

func (lh *levelHandler) getTotalValueSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalValueSize + lh.staging.totalValueSize()
}

func (lh *levelHandler) keyCount() uint64 {
	lh.RLock()
	defer lh.RUnlock()
	var total uint64
	for _, t := range lh.tables {
		if t != nil {
			total += uint64(t.keyCount)
		}
	}
	for _, t := range lh.staging.allTables() {
		if t != nil {
			total += uint64(t.keyCount)
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
	for _, t := range lh.staging.allTables() {
		if t != nil {
			total += uint64(t.RangeTombstoneCount())
		}
	}
	return total
}

func (lh *levelHandler) addSize(t *table) {
	lh.totalSize += t.Size()
	lh.totalStaleSize += int64(t.StaleDataSize())
	lh.totalValueSize += int64(t.ValueSize())
}

func (lh *levelHandler) subtractSize(t *table) {
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

func (lh *levelHandler) valueDensity() float64 {
	lh.RLock()
	defer lh.RUnlock()
	if lh.totalSize <= 0 {
		return 0
	}
	return float64(lh.totalValueSize) / float64(lh.totalSize)
}

func (lh *levelHandler) valueBias(weight float64) float64 {
	if weight <= 0 {
		return 1.0
	}
	density := lh.valueDensity()
	bias := 1.0 + weight*density
	if bias > 4.0 {
		return 4.0
	}
	if bias < 1.0 {
		return 1.0
	}
	return bias
}

func (lh *levelHandler) metricsSnapshot() LevelMetrics {
	if lh == nil {
		return LevelMetrics{}
	}
	lh.RLock()
	defer lh.RUnlock()
	return LevelMetrics{
		Level:                  lh.levelNum,
		TableCount:             len(lh.tables),
		SizeBytes:              lh.totalSize,
		ValueBytes:             lh.totalValueSize,
		StaleBytes:             lh.totalStaleSize,
		StagingTableCount:      lh.staging.tableCount(),
		StagingSizeBytes:       lh.staging.totalSize(),
		StagingValueBytes:      lh.staging.totalValueSize(),
		ValueDensity:           lh.densityLocked(),
		StagingValueDensity:    lh.stagingDensityLocked(),
		StagingRuns:            int64(lh.stagingRuns.Load()),
		StagingMs:              float64(lh.stagingDurationNs.Load()) / 1e6,
		StagingTablesCompacted: int64(lh.stagingTablesCompactedCount.Load()),
		StagingMergeRuns:       int64(lh.stagingMergeRuns.Load()),
		StagingMergeMs:         float64(lh.stagingMergeDurationNs.Load()) / 1e6,
		StagingMergeTables:     int64(lh.stagingMergeTables.Load()),
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

// Get finds key inside this level, considering staging shards and level semantics.
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
	if entry, err := lh.staging.search(key, &maxVer); err == nil {
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
	lh.sortTablesLocked()
	lh.rebuildRangeFilterLocked()
	lh.staging.sortShards()
}

// sortTablesLocked sorts lh.tables using level-specific ordering semantics.
// Caller must hold lh's mutex.
func (lh *levelHandler) sortTablesLocked() {
	if lh.levelNum == 0 {
		// L0 key ranges may overlap, so ordering follows file creation order.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
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
	for _, table := range lh.selectTablesForKey(key, true) {
		if table == nil {
			continue
		}
		if kv.CompareBaseKeys(key, table.MinKey()) < 0 ||
			kv.CompareBaseKeys(key, table.MaxKey()) > 0 {
			continue
		}
		if table.MaxVersionVal() <= version {
			continue
		}
		if entry, err := table.Search(key, &version); err == nil {
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
	if lh.levelNum > 0 && len(lh.filter.spans) >= rangeFilterMinSpanCount && lh.filter.nonOverlapping {
		total := len(lh.tables)
		table := lh.filter.tableForPoint(key)
		if lh.lm != nil {
			candidates := 0
			if table != nil {
				candidates = 1
			}
			lh.lm.recordRangeFilterPoint(total, candidates, false)
		}
		if table == nil {
			return nil, utils.ErrKeyNotFound
		}
		if table.MaxVersionVal() <= *maxVersion {
			return nil, utils.ErrKeyNotFound
		}
		return table.searchExactCandidate(key, maxVersion)
	}
	tables := lh.selectTablesForKey(key, true)
	if len(tables) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	var best *kv.Entry
	for _, table := range tables {
		if table == nil {
			continue
		}
		if table.MaxVersionVal() <= *maxVersion {
			continue
		}
		var (
			entry *kv.Entry
			err   error
		)
		entry, err = table.Search(key, maxVersion)
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

func (lh *levelHandler) getTableForKey(key []byte) *table {
	if lh.levelNum > 0 && len(lh.filter.spans) >= rangeFilterMinSpanCount && lh.filter.nonOverlapping {
		return lh.filter.tableForPoint(key)
	}
	tables := lh.selectTablesForKey(key, false)
	if len(tables) == 0 {
		return nil
	}
	return tables[0]
}

func (lh *levelHandler) selectTablesForKey(key []byte, record bool) []*table {
	if len(lh.tables) == 0 {
		return nil
	}
	total := len(lh.tables)
	fallback := false
	var tables []*table
	if lh.levelNum == 0 {
		// L0 first tries the sublevel index, falling back to a linear scan
		// if sublevels have not been built yet (e.g. between mutations).
		tables = l0CandidateTables(lh.l0Sublevels, key)
		if tables == nil {
			fallback = true
			tables = lh.getTablesForKeyLinear(key)
		}
	} else if len(lh.filter.spans) < rangeFilterMinSpanCount {
		fallback = true
		tables = lh.getTablesForKeyLinear(key)
	} else {
		if !lh.filter.nonOverlapping {
			fallback = true
		}
		tables = lh.filter.tablesForPoint(key)
	}
	if record && lh.lm != nil {
		lh.lm.recordRangeFilterPoint(total, len(tables), fallback)
	}
	return tables
}

func (lh *levelHandler) getTablesForKeyLinear(key []byte) []*table {
	if len(lh.tables) == 0 {
		return nil
	}
	if lh.levelNum > 0 && kv.CompareBaseKeys(key, lh.tables[0].MinKey()) < 0 {
		return nil
	}
	out := make([]*table, 0, 1)
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

func (lh *levelHandler) selectTablesForBounds(lower, upper []byte, record bool) []*table {
	if len(lh.tables) == 0 {
		if record && lh.lm != nil && (len(lower) > 0 || len(upper) > 0) {
			lh.lm.recordRangeFilterBounded(0, 0, false)
		}
		return nil
	}
	total := len(lh.tables)
	fallback := false
	var tables []*table
	if lh.levelNum == 0 || len(lh.filter.spans) < rangeFilterMinSpanCount {
		fallback = true
		tables = filterTablesByBounds(lh.tables, lower, upper)
	} else {
		if lh.levelNum > 0 && !lh.filter.nonOverlapping {
			fallback = true
		}
		tables = lh.filter.tablesForBounds(lower, upper)
	}
	if record && lh.lm != nil && (len(lower) > 0 || len(upper) > 0) {
		lh.lm.recordRangeFilterBounded(total, len(tables), fallback)
	}
	return tables
}

func (lh *levelHandler) rebuildRangeFilterLocked() {
	lh.filter = buildRangeFilter(lh.levelNum, lh.tables)
}
func (lh *levelHandler) isLastLevel() bool {
	return lh.levelNum == lh.lm.opt.MaxLevelNum-1
}

// replaceTables will replace tables[left:right] with newTables. Note this EXCLUDES tables[right].
// You must call decr() to delete the old tables _after_ writing the update to the manifest.
func (lh *levelHandler) replaceTables(toDel, toAdd []*table) error {
	// Need to re-search the range of tables in this level to be replaced as other goroutines might
	// be changing it as well.  (They can't touch our tables, but if they add/remove other tables,
	// the indices get shifted around.)
	lh.Lock() // We s.Unlock() below.

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	var removed []*table
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
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
		t.setLevel(lh.levelNum)
		newTables = append(newTables, t)
	}

	// Assign tables.
	lh.tables = newTables
	lh.sortTablesLocked()
	lh.rebuildRangeFilterLocked()
	lh.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(removed)
}

// deleteTables remove tables idx0, ..., idx1-1.
func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var removed []*table
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		removed = append(removed, t)
		lh.subtractSize(t)
	}
	lh.tables = newTables
	lh.rebuildRangeFilterLocked()

	lh.staging.remove(toDelMap)

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(removed)
}

func (lh *levelHandler) deleteStagingTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	removed := lh.collectStagingTablesLocked(toDelMap)

	lh.staging.remove(toDelMap)

	lh.Unlock()

	return decrRefs(removed)
}

func (lh *levelHandler) replaceStagingTables(toDel, toAdd []*table) error {
	lh.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		if t == nil {
			continue
		}
		toDelMap[t.fid] = struct{}{}
	}
	removed := lh.collectStagingTablesLocked(toDelMap)
	lh.staging.remove(toDelMap)
	if len(toAdd) > 0 {
		lh.staging.addBatch(toAdd)
	}

	lh.Unlock()

	return decrRefs(removed)
}

func (lh *levelHandler) collectStagingTablesLocked(fidSet map[uint64]struct{}) []*table {
	if len(fidSet) == 0 {
		return nil
	}
	var out []*table
	for _, sh := range lh.staging.shards {
		for _, t := range sh.tables {
			if t == nil {
				continue
			}
			if _, ok := fidSet[t.fid]; ok {
				out = append(out, t)
			}
		}
	}
	return out
}

func (lh *levelHandler) recordStagingMetrics(merge bool, duration time.Duration, tables int) {
	if tables < 0 {
		tables = 0
	}
	if merge {
		lh.stagingMergeRuns.Add(1)
		lh.stagingMergeDurationNs.Add(duration.Nanoseconds())
		if tables > 0 {
			lh.stagingMergeTables.Add(uint64(tables))
		}
		return
	}
	lh.stagingRuns.Add(1)
	lh.stagingDurationNs.Add(duration.Nanoseconds())
	if tables > 0 {
		lh.stagingTablesCompactedCount.Add(uint64(tables))
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
	stagingTables := lh.staging.tablesWithinBounds(topt.LowerBound, topt.UpperBound)
	itrs = append(itrs, iteratorsReversed(stagingTables, topt)...)
	if len(mainTables) == 1 {
		itrs = append(itrs, mainTables[0].NewIterator(topt))
	} else if len(mainTables) > 1 {
		itrs = append(itrs, NewConcatIterator(mainTables, topt))
	}
	if bounded && lh.lm != nil {
		total := len(lh.tables) + lh.staging.tableCount()
		candidates := len(mainTables) + len(stagingTables)
		fallback := len(lh.filter.spans) == 0
		if lh.levelNum > 0 && len(lh.filter.spans) > 0 && !lh.filter.nonOverlapping {
			fallback = true
		}
		lh.lm.recordRangeFilterBounded(total, candidates, fallback)
	}
	return itrs
}
