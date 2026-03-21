package lsm

import (
	"bytes"
	"errors"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

type levelHandler struct {
	sync.RWMutex
	levelNum              int
	tables                []*table
	filter                rangeFilter
	ingest                ingestBuffer
	totalSize             int64
	totalStaleSize        int64
	totalValueSize        int64
	lm                    *levelManager
	ingestRuns            atomic.Uint64
	ingestMergeRuns       atomic.Uint64
	ingestDurationNs      atomic.Int64
	ingestMergeDurationNs atomic.Int64
	ingestTablesCompacted atomic.Uint64
	ingestMergeTables     atomic.Uint64
}

type tableRange struct {
	min []byte
	max []byte
	tbl *table
}

func (lh *levelHandler) close() error {
	lh.RLock()
	tables := append([]*table(nil), lh.tables...)
	ingestTables := append([]*table(nil), lh.ingest.allTables()...)
	lh.RUnlock()

	var closeErr error
	for _, t := range tables {
		if t == nil {
			continue
		}
		closeErr = errors.Join(closeErr, t.closeHandle())
	}
	for _, t := range ingestTables {
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
	return lh.totalSize + lh.ingest.totalSize()
}

func (lh *levelHandler) getTotalValueSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.totalValueSize + lh.ingest.totalValueSize()
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
		Level:                 lh.levelNum,
		TableCount:            len(lh.tables),
		SizeBytes:             lh.totalSize,
		ValueBytes:            lh.totalValueSize,
		StaleBytes:            lh.totalStaleSize,
		IngestTableCount:      lh.ingest.tableCount(),
		IngestSizeBytes:       lh.ingest.totalSize(),
		IngestValueBytes:      lh.ingest.totalValueSize(),
		ValueDensity:          lh.densityLocked(),
		IngestValueDensity:    lh.ingestDensityLocked(),
		IngestRuns:            int64(lh.ingestRuns.Load()),
		IngestMs:              float64(lh.ingestDurationNs.Load()) / 1e6,
		IngestTablesCompacted: int64(lh.ingestTablesCompacted.Load()),
		IngestMergeRuns:       int64(lh.ingestMergeRuns.Load()),
		IngestMergeMs:         float64(lh.ingestMergeDurationNs.Load()) / 1e6,
		IngestMergeTables:     int64(lh.ingestMergeTables.Load()),
	}
}

// densityLocked computes value density; caller must hold lh lock.
func (lh *levelHandler) densityLocked() float64 {
	if lh.totalSize <= 0 {
		return 0
	}
	return float64(lh.totalValueSize) / float64(lh.totalSize)
}

func baseKeyInRange(min, max, key []byte) bool {
	if len(min) == 0 || len(max) == 0 || len(key) == 0 {
		return false
	}
	minBase := kv.InternalToBaseKey(min)
	maxBase := kv.InternalToBaseKey(max)
	keyBase := kv.InternalToBaseKey(key)
	return bytes.Compare(keyBase, minBase) >= 0 && bytes.Compare(keyBase, maxBase) <= 0
}

// hotOverlapScore returns the fraction of hotKeys overlapping this level.
// When ingestOnly is true, only ingest buffers are considered.
func (lh *levelHandler) hotOverlapScore(hotKeys [][]byte, ingestOnly bool) float64 {
	if lh == nil || len(hotKeys) == 0 {
		return 0
	}
	lh.RLock()
	defer lh.RUnlock()
	hit := 0
	checkMain := func(key []byte) bool {
		for _, t := range lh.tables {
			if t == nil {
				continue
			}
			if baseKeyInRange(t.MinKey(), t.MaxKey(), key) {
				return true
			}
		}
		return false
	}
	checkIngest := func(key []byte) bool {
		for _, sh := range lh.ingest.shards {
			for _, rng := range sh.ranges {
				if baseKeyInRange(rng.min, rng.max, key) {
					return true
				}
			}
		}
		return false
	}
	for _, hk := range hotKeys {
		if len(hk) == 0 {
			continue
		}
		if ingestOnly {
			if checkIngest(hk) {
				hit++
			}
			continue
		}
		if checkMain(hk) || checkIngest(hk) {
			hit++
		}
	}
	return float64(hit) / float64(len(hotKeys))
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

// Get finds key inside this level, considering ingest shards and level semantics.
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
	if entry, err := lh.searchIngestSST(key, &maxVer); err == nil {
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

func (lh *levelHandler) prefetch(key []byte) bool {
	if lh == nil || len(key) == 0 {
		return false
	}
	lh.RLock()
	defer lh.RUnlock()
	if lh.levelNum == 0 {
		var hit bool
		for _, table := range lh.tables {
			if table == nil {
				continue
			}
			if utils.CompareBaseKeys(key, table.MinKey()) < 0 ||
				utils.CompareBaseKeys(key, table.MaxKey()) > 0 {
				continue
			}
			if table.prefetchBlockForKey(key) {
				hit = true
			}
		}
		return hit
	}
	if lh.ingest.prefetch(key) {
		return true
	}
	table := lh.getTableForKey(key)
	if table == nil {
		return false
	}
	return table.prefetchBlockForKey(key)
}

// Sort orders tables for lookup/compaction; L0 by file id, Ln by key range.
func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	lh.sortTablesLocked()
	lh.rebuildRangeFilterLocked()
	lh.ingest.sortShards()
}

// sortTablesLocked sorts lh.tables using level-specific ordering semantics.
// Caller must hold lh's mutex.
func (lh *levelHandler) sortTablesLocked() {
	if lh.levelNum == 0 {
		// L0 key ranges may overlap, so ordering follows file creation order.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
		return
	}
	// L1+ tables are non-overlapping by key range.
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareInternalKeys(lh.tables[i].MinKey(), lh.tables[j].MinKey()) < 0
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
		if utils.CompareBaseKeys(key, table.MinKey()) < 0 ||
			utils.CompareBaseKeys(key, table.MaxKey()) > 0 {
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
	if lh.levelNum == 0 || len(lh.filter.spans) < rangeFilterMinSpanCount {
		fallback = true
		tables = lh.getTablesForKeyLinear(key)
	} else {
		if lh.levelNum > 0 && !lh.filter.nonOverlapping {
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
	if lh.levelNum > 0 && utils.CompareBaseKeys(key, lh.tables[0].MinKey()) < 0 {
		return nil
	}
	out := make([]*table, 0, 1)
	for _, t := range lh.tables {
		if t == nil {
			continue
		}
		if lh.levelNum > 0 && utils.CompareBaseKeys(t.MinKey(), key) > 0 {
			break
		}
		if utils.CompareBaseKeys(key, t.MaxKey()) <= 0 &&
			utils.CompareBaseKeys(key, t.MinKey()) >= 0 {
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

	lh.ingest.remove(toDelMap)

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(removed)
}

func (lh *levelHandler) deleteIngestTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}
	removed := lh.collectIngestTablesLocked(toDelMap)

	lh.ingest.remove(toDelMap)

	lh.Unlock()

	return decrRefs(removed)
}

func (lh *levelHandler) replaceIngestTables(toDel, toAdd []*table) error {
	lh.Lock()

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		if t == nil {
			continue
		}
		toDelMap[t.fid] = struct{}{}
	}
	removed := lh.collectIngestTablesLocked(toDelMap)
	lh.ingest.remove(toDelMap)
	if len(toAdd) > 0 {
		lh.ingest.addBatch(toAdd)
	}

	lh.Unlock()

	return decrRefs(removed)
}

func (lh *levelHandler) collectIngestTablesLocked(fidSet map[uint64]struct{}) []*table {
	if len(fidSet) == 0 {
		return nil
	}
	var out []*table
	for _, sh := range lh.ingest.shards {
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

func (lh *levelHandler) recordIngestMetrics(merge bool, duration time.Duration, tables int) {
	if tables < 0 {
		tables = 0
	}
	if merge {
		lh.ingestMergeRuns.Add(1)
		lh.ingestMergeDurationNs.Add(duration.Nanoseconds())
		if tables > 0 {
			lh.ingestMergeTables.Add(uint64(tables))
		}
		return
	}
	lh.ingestRuns.Add(1)
	lh.ingestDurationNs.Add(duration.Nanoseconds())
	if tables > 0 {
		lh.ingestTablesCompacted.Add(uint64(tables))
	}
}

func (lh *levelHandler) iterators(opt *utils.Options) []utils.Iterator {
	topt := &utils.Options{}
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

	var itrs []utils.Iterator
	ingestTables := lh.ingest.tablesWithinBounds(topt.LowerBound, topt.UpperBound)
	itrs = append(itrs, iteratorsReversed(ingestTables, topt)...)
	if len(mainTables) == 1 {
		itrs = append(itrs, mainTables[0].NewIterator(topt))
	} else if len(mainTables) > 1 {
		itrs = append(itrs, NewConcatIterator(mainTables, topt))
	}
	if bounded && lh.lm != nil {
		total := len(lh.tables) + lh.ingest.tableCount()
		candidates := len(mainTables) + len(ingestTables)
		fallback := len(lh.filter.spans) == 0
		if lh.levelNum > 0 && len(lh.filter.spans) > 0 && !lh.filter.nonOverlapping {
			fallback = true
		}
		lh.lm.recordRangeFilterBounded(total, candidates, fallback)
	}
	return itrs
}
