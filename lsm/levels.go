package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/compact"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
)

// initLevelManager initialize the levelManager
func (lsm *LSM) initLevelManager(opt *Options) *levelManager {
	lm := &levelManager{lsm: lsm} // dereference lsm
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	if lm.opt.IngestCompactBatchSize <= 0 {
		lm.opt.IngestCompactBatchSize = 4
	}
	// read the manifest file to build the manager
	if err := lm.loadManifest(); err != nil {
		panic(err)
	}
	if lm.manifestMgr != nil {
		lm.manifestMgr.SetSync(opt.ManifestSync)
		lm.manifestMgr.SetRewriteThreshold(opt.ManifestRewriteThreshold)
	}
	if err := lm.build(); err != nil {
		panic(err)
	}
	lm.compaction = compact.NewManager(lm, lm.opt.NumCompactors)
	if opt != nil && opt.HotKeyProvider != nil {
		lm.setHotKeyProvider(opt.HotKeyProvider)
	}
	return lm
}

type levelManager struct {
	maxFID           uint64
	opt              *Options
	cache            *cache
	manifestMgr      *manifest.Manager
	levels           []*levelHandler
	lsm              *LSM
	compactState     *compact.State
	compaction       *compact.Manager
	logPtrMu         sync.RWMutex
	logPtrSeg        uint32
	logPtrOffset     uint64
	compactionLastNs int64
	compactionMaxNs  int64
	compactionRuns   uint64
	hotProvider      atomic.Value // func() [][]byte
}

// LevelMetrics captures aggregated statistics for a single LSM level.
type LevelMetrics struct {
	Level                 int
	TableCount            int
	SizeBytes             int64
	ValueBytes            int64
	StaleBytes            int64
	IngestTableCount      int
	IngestSizeBytes       int64
	IngestValueBytes      int64
	ValueDensity          float64
	IngestValueDensity    float64
	IngestRuns            int64
	IngestMs              float64
	IngestTablesCompacted int64
	IngestMergeRuns       int64
	IngestMergeMs         float64
	IngestMergeTables     int64
}

func (lm *levelManager) close() error {
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifestMgr.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func (lm *levelManager) setHotKeyProvider(fn func() [][]byte) {
	if lm == nil {
		return
	}
	if fn == nil {
		return
	}
	lm.hotProvider.Store(fn)
}

func (lm *levelManager) iterators(opt *utils.Options) []utils.Iterator {
	itrs := make([]utils.Iterator, 0, len(lm.levels))
	for _, level := range lm.levels {
		itrs = append(itrs, level.iterators(opt)...)
	}
	return itrs
}

func (lm *levelManager) Get(key []byte) (*kv.Entry, error) {
	var (
		entry *kv.Entry
		err   error
	)
	// L0 layer query
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	// L1-7 layer query
	for level := 1; level < lm.opt.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err = ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, utils.ErrKeyNotFound
}

func (lm *levelManager) loadManifest() (err error) {
	lm.manifestMgr, err = manifest.Open(lm.opt.WorkDir)
	return err
}
func (lm *levelManager) build() error {
	lm.levels = make([]*levelHandler, 0, lm.opt.MaxLevelNum)
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lh := &levelHandler{
			levelNum: i,
			tables:   make([]*table, 0),
			lm:       lm,
		}
		lh.ingest.ensureInit()
		lm.levels = append(lm.levels, lh)
	}

	version := lm.manifestMgr.Current()
	lm.setLogPointer(version.LogSegment, version.LogOffset)
	lm.cache = newCache(lm.opt)
	var maxFID uint64
	var missing []manifest.FileMeta
	for level, files := range version.Levels {
		for _, meta := range files {
			fileName := utils.FileNameSSTable(lm.opt.WorkDir, meta.FileID)
			if _, err := os.Stat(fileName); err != nil {
				_ = utils.Err(fmt.Errorf("missing sstable %s: %v", fileName, err))
				missing = append(missing, meta)
				continue
			}
			if meta.FileID > maxFID {
				maxFID = meta.FileID
			}
			t := openTable(lm, fileName, nil)
			if meta.Ingest {
				lm.levels[level].addIngest(t)
			} else {
				lm.levels[level].add(t)
			}
		}
	}
	// sort each level
	for i := 0; i < lm.opt.MaxLevelNum; i++ {
		lm.levels[i].Sort()
	}
	// get the maximum fid value
	atomic.AddUint64(&lm.maxFID, maxFID)

	for _, meta := range missing {
		metaCopy := meta
		_ = lm.manifestMgr.LogEdit(manifest.Edit{
			Type: manifest.EditDeleteFile,
			File: &metaCopy,
		})
	}
	return nil
}

// flush a sstable to L0 layer
func (lm *levelManager) flush(immutable *memTable) (err error) {
	// allocate a fid
	fid := uint64(immutable.segmentID)
	sstName := utils.FileNameSSTable(lm.opt.WorkDir, fid)

	iter := immutable.NewIterator(&utils.Options{})
	if iter == nil {
		return nil
	}
	defer iter.Close()

	iter.Rewind()
	if !iter.Valid() {
		if err := lm.lsm.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		return nil
	}

	// build a builder
	builder := newTableBuiler(lm.opt)
	for ; iter.Valid(); iter.Next() {
		entry := iter.Item().Entry()
		builder.AddKey(entry)
	}
	table := openTable(lm, sstName, builder)
	if table == nil {
		return fmt.Errorf("failed to build sstable %s", sstName)
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
	pointerEdit := manifest.Edit{
		Type:      manifest.EditLogPointer,
		LogSeg:    immutable.segmentID,
		LogOffset: uint64(atomic.LoadInt64(&immutable.walSize)),
	}
	if err := lm.manifestMgr.LogEdits(fileEdit, pointerEdit); err != nil {
		return err
	}
	lm.setLogPointer(immutable.segmentID, uint64(atomic.LoadInt64(&immutable.walSize)))
	lm.levels[0].add(table)
	if lm.canRemoveWalSegment(uint32(fid)) {
		if err := lm.lsm.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if lm.compaction != nil {
		lm.compaction.Trigger("flush")
	}
	return nil
}

func (lm *levelManager) LogValueLogHead(ptr *kv.ValuePtr) error {
	if ptr == nil {
		return nil
	}
	return lm.manifestMgr.LogValueLogHead(ptr.Bucket, ptr.Fid, uint64(ptr.Offset))
}

func (lm *levelManager) LogValueLogDelete(bucket uint32, fid uint32) error {
	return lm.manifestMgr.LogValueLogDelete(bucket, fid)
}

func (lm *levelManager) LogValueLogUpdate(meta *manifest.ValueLogMeta) error {
	if meta == nil {
		return nil
	}
	return lm.manifestMgr.LogValueLogUpdate(*meta)
}

func (lm *levelManager) ValueLogHead() map[uint32]manifest.ValueLogMeta {
	return lm.manifestMgr.ValueLogHead()
}

func (lm *levelManager) ValueLogStatus() map[manifest.ValueLogID]manifest.ValueLogMeta {
	return lm.manifestMgr.ValueLogStatus()
}

func (lm *levelManager) setLogPointer(seg uint32, offset uint64) {
	lm.logPtrMu.Lock()
	lm.logPtrSeg = seg
	lm.logPtrOffset = offset
	lm.logPtrMu.Unlock()
}

func (lm *levelManager) logPointer() (uint32, uint64) {
	lm.logPtrMu.RLock()
	defer lm.logPtrMu.RUnlock()
	return lm.logPtrSeg, lm.logPtrOffset
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

func (lm *levelManager) levelMetricsSnapshot() []LevelMetrics {
	if lm == nil {
		return nil
	}
	metrics := make([]LevelMetrics, 0, len(lm.levels))
	for _, lh := range lm.levels {
		if lh == nil {
			continue
		}
		metrics = append(metrics, lh.metricsSnapshot())
	}
	return metrics
}

func (lm *levelManager) compactionDurations() (float64, float64, uint64) {
	if lm == nil {
		return 0, 0, 0
	}
	lastNs := atomic.LoadInt64(&lm.compactionLastNs)
	maxNs := atomic.LoadInt64(&lm.compactionMaxNs)
	runs := atomic.LoadUint64(&lm.compactionRuns)
	return float64(lastNs) / 1e6, float64(maxNs) / 1e6, runs
}

func (lm *levelManager) recordCompactionMetrics(duration time.Duration) {
	atomic.AddUint64(&lm.compactionRuns, 1)
	last := duration.Nanoseconds()
	atomic.StoreInt64(&lm.compactionLastNs, last)
	for {
		prev := atomic.LoadInt64(&lm.compactionMaxNs)
		if last <= prev {
			break
		}
		if atomic.CompareAndSwapInt64(&lm.compactionMaxNs, prev, last) {
			break
		}
	}
}

func (lm *levelManager) cacheMetrics() CacheMetrics {
	if lm == nil || lm.cache == nil {
		return CacheMetrics{}
	}
	return lm.cache.metricsSnapshot()
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

func (lm *levelManager) canRemoveWalSegment(id uint32) bool {
	if lm == nil || lm.manifestMgr == nil {
		return true
	}
	ptrs := lm.manifestMgr.RaftPointerSnapshot()
	for _, ptr := range ptrs {
		if ptr.SegmentIndex > 0 {
			if id >= uint32(ptr.SegmentIndex) {
				return false
			}
		}
		if ptr.Segment == 0 {
			continue
		}
		if id >= ptr.Segment {
			return false
		}
	}
	if lm.lsm != nil && lm.lsm.wal != nil {
		metrics := lm.lsm.wal.SegmentRecordMetrics(id)
		if metrics.RaftRecords() > 0 {
			log.Printf("[wal] segment %d retains raft records during GC eligibility (raft_entries=%d raft_states=%d raft_snapshots=%d)", id, metrics.RaftEntries, metrics.RaftStates, metrics.RaftSnapshots)
		}
	}
	return true
}

func (lm *levelManager) prefetch(key []byte, hot bool) {
	if lm == nil || len(key) == 0 {
		return
	}
	if len(lm.levels) == 0 {
		return
	}
	// Always probe L0 because ranges may overlap.
	_ = lm.levels[0].prefetch(key, hot)
	for level := 1; level < len(lm.levels); level++ {
		if lm.levels[level].prefetch(key, hot) {
			break
		}
	}
}

// --------- levelHandler ---------
type levelHandler struct {
	sync.RWMutex
	levelNum              int
	tables                []*table
	ingest                ingestBuffer
	totalSize             int64
	totalStaleSize        int64
	totalValueSize        int64
	lm                    *levelManager
	ingestRuns            uint64
	ingestMergeRuns       uint64
	ingestDurationNs      int64
	ingestMergeDurationNs int64
	ingestTablesCompacted uint64
	ingestMergeTables     uint64
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

	for _, t := range tables {
		if t == nil {
			continue
		}
		if err := t.closeHandle(); err != nil {
			return err
		}
	}
	for _, t := range ingestTables {
		if t == nil {
			continue
		}
		if err := t.closeHandle(); err != nil {
			return err
		}
	}
	return nil
}
func (lh *levelHandler) add(t *table) {
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
		IngestRuns:            int64(atomic.LoadUint64(&lh.ingestRuns)),
		IngestMs:              float64(atomic.LoadInt64(&lh.ingestDurationNs)) / 1e6,
		IngestTablesCompacted: int64(atomic.LoadUint64(&lh.ingestTablesCompacted)),
		IngestMergeRuns:       int64(atomic.LoadUint64(&lh.ingestMergeRuns)),
		IngestMergeMs:         float64(atomic.LoadInt64(&lh.ingestMergeDurationNs)) / 1e6,
		IngestMergeTables:     int64(atomic.LoadUint64(&lh.ingestMergeTables)),
	}
}

// densityLocked computes value density; caller must hold lh lock.
func (lh *levelHandler) densityLocked() float64 {
	if lh.totalSize <= 0 {
		return 0
	}
	return float64(lh.totalValueSize) / float64(lh.totalSize)
}

func keyInRange(min, max, key []byte) bool {
	if len(min) == 0 || len(max) == 0 || len(key) == 0 {
		return false
	}
	// Accept both internal keys (with timestamp) and raw user keys from HotRing.
	minUser := kv.ParseKey(min)
	maxUser := kv.ParseKey(max)
	keyUser := key
	if len(key) > 8 {
		keyUser = kv.ParseKey(key)
	}
	_, minUser, _ = kv.DecodeKeyCF(minUser)
	_, maxUser, _ = kv.DecodeKeyCF(maxUser)
	_, keyUser, _ = kv.DecodeKeyCF(keyUser)
	return bytes.Compare(keyUser, minUser) >= 0 && bytes.Compare(keyUser, maxUser) <= 0
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
			if keyInRange(t.MinKey(), t.MaxKey(), key) {
				return true
			}
		}
		return false
	}
	checkIngest := func(key []byte) bool {
		for _, sh := range lh.ingest.shards {
			for _, rng := range sh.ranges {
				if keyInRange(rng.min, rng.max, key) {
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

func (lh *levelHandler) prefetch(key []byte, hot bool) bool {
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
			if utils.CompareUserKeys(key, table.MinKey()) < 0 ||
				utils.CompareUserKeys(key, table.MaxKey()) > 0 {
				continue
			}
			if table.prefetchBlockForKey(key, hot) {
				hit = true
			}
		}
		return hit
	}
	if lh.ingest.prefetch(key, hot) {
		return true
	}
	table := lh.getTableForKey(key)
	if table == nil {
		return false
	}
	return table.prefetchBlockForKey(key, hot)
}

func (lh *levelHandler) Sort() {
	lh.Lock()
	defer lh.Unlock()
	if lh.levelNum == 0 {
		// Key range will overlap. Just sort by fileID in ascending order
		// because newer tables are at the end of level 0.
		sort.Slice(lh.tables, func(i, j int) bool {
			return lh.tables[i].fid < lh.tables[j].fid
		})
	} else {
		// Sort tables by keys.
		sort.Slice(lh.tables, func(i, j int) bool {
			return utils.CompareKeys(lh.tables[i].MinKey(), lh.tables[j].MinKey()) < 0
		})
	}
	lh.ingest.sortShards()
}

func (lh *levelHandler) searchL0SST(key []byte) (*kv.Entry, error) {
	var (
		version uint64
		best    *kv.Entry
	)
	for _, table := range lh.tables {
		if table == nil {
			continue
		}
		if utils.CompareUserKeys(key, table.MinKey()) < 0 ||
			utils.CompareUserKeys(key, table.MaxKey()) > 0 {
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
	table := lh.getTableForKey(key)
	if table == nil {
		return nil, utils.ErrKeyNotFound
	}
	if maxVersion != nil && table.MaxVersionVal() <= *maxVersion {
		return nil, utils.ErrKeyNotFound
	}
	if maxVersion == nil {
		var tmp uint64
		maxVersion = &tmp
	}
	if entry, err := table.Search(key, maxVersion); err == nil {
		return entry, nil
	} else if err != utils.ErrKeyNotFound {
		return nil, err
	}
	return nil, utils.ErrKeyNotFound
}
func (lh *levelHandler) getTableForKey(key []byte) *table {
	if len(lh.tables) > 0 && (utils.CompareUserKeys(key, lh.tables[0].MinKey()) < 0 ||
		utils.CompareUserKeys(key, lh.tables[len(lh.tables)-1].MaxKey()) > 0) {
		return nil
	}
	for i := len(lh.tables) - 1; i >= 0; i-- {
		if utils.CompareUserKeys(key, lh.tables[i].MinKey()) > -1 &&
			utils.CompareUserKeys(key, lh.tables[i].MaxKey()) < 1 {
			return lh.tables[i]
		}
	}
	return nil
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
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
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
	sort.Slice(lh.tables, func(i, j int) bool {
		return utils.CompareKeys(lh.tables[i].MinKey(), lh.tables[j].MinKey()) < 0
	})
	lh.Unlock() // s.Unlock before we DecrRef tables -- that can be slow.
	return decrRefs(toDel)
}

// deleteTables remove tables idx0, ..., idx1-1.
func (lh *levelHandler) deleteTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	// Make a copy as iterators might be keeping a slice of tables.
	var newTables []*table
	for _, t := range lh.tables {
		_, found := toDelMap[t.fid]
		if !found {
			newTables = append(newTables, t)
			continue
		}
		lh.subtractSize(t)
	}
	lh.tables = newTables

	lh.ingest.remove(toDelMap)

	lh.Unlock() // Unlock s _before_ we DecrRef our tables, which can be slow.

	return decrRefs(toDel)
}

func (lh *levelHandler) deleteIngestTables(toDel []*table) error {
	lh.Lock() // s.Unlock() below

	toDelMap := make(map[uint64]struct{})
	for _, t := range toDel {
		toDelMap[t.fid] = struct{}{}
	}

	lh.ingest.remove(toDelMap)

	lh.Unlock()

	return decrRefs(toDel)
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
	lh.ingest.remove(toDelMap)
	if len(toAdd) > 0 {
		lh.ingest.addBatch(toAdd)
	}

	lh.Unlock()

	return decrRefs(toDel)
}

func (lh *levelHandler) recordIngestMetrics(merge bool, duration time.Duration, tables int) {
	if tables < 0 {
		tables = 0
	}
	if merge {
		atomic.AddUint64(&lh.ingestMergeRuns, 1)
		atomic.AddInt64(&lh.ingestMergeDurationNs, duration.Nanoseconds())
		if tables > 0 {
			atomic.AddUint64(&lh.ingestMergeTables, uint64(tables))
		}
		return
	}
	atomic.AddUint64(&lh.ingestRuns, 1)
	atomic.AddInt64(&lh.ingestDurationNs, duration.Nanoseconds())
	if tables > 0 {
		atomic.AddUint64(&lh.ingestTablesCompacted, uint64(tables))
	}
}

func (lh *levelHandler) iterators(opt *utils.Options) []utils.Iterator {
	topt := &utils.Options{}
	if opt != nil {
		*topt = *opt
	}
	lh.RLock()
	defer lh.RUnlock()
	if lh.levelNum == 0 {
		return iteratorsReversed(lh.tables, topt)
	}

	var itrs []utils.Iterator
	itrs = append(itrs, lh.ingest.iterators(topt)...)
	if len(lh.tables) > 0 {
		itrs = append(itrs, NewConcatIterator(lh.tables, topt))
	}
	return itrs
}
