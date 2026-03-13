package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm/compact"
	"github.com/feichai0017/NoKV/lsm/tombstone"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
)

// initLevelManager initialize the levelManager
func (lsm *LSM) initLevelManager(opt *Options) (*levelManager, error) {
	lm := &levelManager{lsm: lsm} // dereference lsm
	lm.compactState = lsm.newCompactStatus()
	lm.opt = opt
	// read the manifest file to build the manager
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
	lm.compaction = compact.NewManager(
		lm,
		lm.opt.NumCompactors,
		compact.NewSchedulerPolicy(lm.opt.CompactionPolicy),
		lsm.getLogger(),
	)
	if opt != nil && opt.HotKeyProvider != nil {
		lm.hotProvider = opt.HotKeyProvider
	}
	return lm, nil
}

type levelManager struct {
	maxFID           atomic.Uint64
	opt              *Options
	cache            *cache
	manifestMgr      *manifest.Manager
	levels           []*levelHandler
	lsm              *LSM
	compactState     *compact.State
	compaction       *compact.Manager
	rtCollector      *tombstone.Collector
	logPtrMu         sync.RWMutex
	logPtrSeg        uint32
	logPtrOffset     uint64
	compactionLastNs atomic.Int64
	compactionMaxNs  atomic.Int64
	compactionRuns   atomic.Uint64
	hotProvider      func() [][]byte
}

// LevelMetrics aliases the shared metrics package model to keep the lsm API stable.
type LevelMetrics = metrics.LevelMetrics

func (lm *levelManager) close() error {
	var closeErr error
	if lm.cache != nil {
		closeErr = errors.Join(closeErr, lm.cache.close())
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

func (lm *levelManager) iterators(opt *utils.Options) []utils.Iterator {
	itrs := make([]utils.Iterator, 0, len(lm.levels))
	for _, level := range lm.levels {
		itrs = append(itrs, level.iterators(opt)...)
	}
	return itrs
}

// Get searches levels from L0 to Ln and returns the newest visible entry for key.
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
		lh.ingest.ensureInit()
		lm.levels = append(lm.levels, lh)
	}

	version := lm.manifestMgr.Current()
	lm.setLogPointer(version.LogSegment, version.LogOffset)
	lm.cache = newCache(lm.opt)
	var maxFID uint64
	var stale []manifest.FileMeta
	for level, files := range version.Levels {
		for _, meta := range files {
			fileName := utils.FileNameSSTable(lm.opt.WorkDir, meta.FileID)
			if _, err := fs.Stat(fileName); err != nil {
				slog.Default().Error("missing sstable", "path", fileName, "error", err)
				stale = append(stale, meta)
				continue
			}
			if meta.FileID > maxFID {
				maxFID = meta.FileID
			}
			t, err := openTable(lm, fileName, nil)
			if err != nil {
				slog.Default().Error("failed to open sstable", "path", fileName, "error", err)
				stale = append(stale, meta)
				continue
			}
			if t == nil {
				slog.Default().Error("failed to open sstable", "path", fileName, "error", "nil table")
				stale = append(stale, meta)
				continue
			}
			if meta.Ingest {
				lm.levels[level].addIngest(t)
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

	for _, meta := range stale {
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

	iter := immutable.NewIterator(&utils.Options{IsAsc: true})
	if iter == nil {
		return nil
	}
	defer func() { _ = iter.Close() }()

	iter.Rewind()
	if !iter.Valid() {
		if err := lm.lsm.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) {
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
	pointerEdit := manifest.Edit{
		Type:      manifest.EditLogPointer,
		LogSeg:    immutable.segmentID,
		LogOffset: uint64(immutable.walSize.Load()),
	}
	// Strict durability mode: persist SST directory entries before manifest references.
	if lm.opt.ManifestSync {
		if err := vfs.SyncDir(lm.opt.FS, lm.opt.WorkDir); err != nil {
			return err
		}
	}
	if err := lm.manifestMgr.LogEdits(fileEdit, pointerEdit); err != nil {
		return err
	}
	lm.setLogPointer(immutable.segmentID, uint64(immutable.walSize.Load()))
	lm.levels[0].add(table)
	// Register any range tombstones discovered during this flush.
	if lm.rtCollector != nil {
		for _, rt := range newTombstones {
			lm.rtCollector.Add(rt)
		}
	}
	if lm.canRemoveWalSegment(uint32(fid)) {
		if err := lm.lsm.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	if lm.compaction != nil {
		lm.compaction.Trigger()
	}
	return nil
}

// LogValueLogHead persists the latest value-log head pointer into manifest state.
func (lm *levelManager) LogValueLogHead(ptr *kv.ValuePtr) error {
	if ptr == nil {
		return nil
	}
	return lm.manifestMgr.LogValueLogHead(ptr.Bucket, ptr.Fid, uint64(ptr.Offset))
}

// LogValueLogDelete records a value-log file deletion in the manifest.
func (lm *levelManager) LogValueLogDelete(bucket uint32, fid uint32) error {
	return lm.manifestMgr.LogValueLogDelete(bucket, fid)
}

// LogValueLogUpdate updates value-log metadata for an existing file.
func (lm *levelManager) LogValueLogUpdate(meta *manifest.ValueLogMeta) error {
	if meta == nil {
		return nil
	}
	return lm.manifestMgr.LogValueLogUpdate(*meta)
}

// ValueLogHead returns manifest-tracked per-bucket active value-log heads.
func (lm *levelManager) ValueLogHead() map[uint32]manifest.ValueLogMeta {
	return lm.manifestMgr.ValueLogHead()
}

// ValueLogStatus returns manifest metadata for all known value-log files.
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
	if lm == nil || lm.lsm == nil {
		return true
	}
	return lm.lsm.canRemoveWalSegment(id)
}

func (lm *levelManager) prefetch(key []byte) {
	if lm == nil || len(key) == 0 {
		return
	}
	if len(lm.levels) == 0 {
		return
	}
	// Always probe L0 because ranges may overlap.
	_ = lm.levels[0].prefetch(key)
	for level := 1; level < len(lm.levels); level++ {
		if lm.levels[level].prefetch(key) {
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

func keyInRange(min, max, key []byte) bool {
	if len(min) == 0 || len(max) == 0 || len(key) == 0 {
		return false
	}
	_, minUser, _, minOK := kv.SplitInternalKey(min)
	_, maxUser, _, maxOK := kv.SplitInternalKey(max)
	_, keyUser, _, keyOK := kv.SplitInternalKey(key)
	if !minOK || !maxOK || !keyOK {
		return false
	}
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
			if utils.CompareUserKeys(key, table.MinKey()) < 0 ||
				utils.CompareUserKeys(key, table.MaxKey()) > 0 {
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
		return utils.CompareKeys(lh.tables[i].MinKey(), lh.tables[j].MinKey()) < 0
	})
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
	tables := lh.getTablesForKey(key)
	if len(tables) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	if maxVersion == nil {
		var tmp uint64
		maxVersion = &tmp
	}
	var best *kv.Entry
	for _, table := range tables {
		if table == nil {
			continue
		}
		if table.MaxVersionVal() <= *maxVersion {
			continue
		}
		if entry, err := table.Search(key, maxVersion); err == nil {
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

func (lh *levelHandler) getTableForKey(key []byte) *table {
	tables := lh.getTablesForKey(key)
	if len(tables) == 0 {
		return nil
	}
	return tables[0]
}

// getTablesForKey returns every table in this level whose user-key range covers key.
// Tables are returned in min-key order.
func (lh *levelHandler) getTablesForKey(key []byte) []*table {
	if len(lh.tables) == 0 {
		return nil
	}
	if utils.CompareUserKeys(key, lh.tables[0].MinKey()) < 0 {
		return nil
	}
	out := make([]*table, 0, 1)
	for _, t := range lh.tables {
		if t == nil {
			continue
		}
		// Since min keys are sorted ascending, we can stop once min > key.
		if utils.CompareUserKeys(t.MinKey(), key) > 0 {
			break
		}
		if utils.CompareUserKeys(key, t.MaxKey()) <= 0 {
			out = append(out, t)
		}
	}
	return out
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

func checkTablesOverlap(tables []*table) error {
	sorted := make([]*table, len(tables))
	copy(sorted, tables)

	sort.Slice(sorted, func(i, j int) bool {
		return utils.CompareUserKeys(sorted[i].MinKey(), sorted[j].MinKey()) < 0
	})

	for i := 1; i < len(sorted); i++ {
		prev := sorted[i-1]
		curr := sorted[i]
		if utils.CompareUserKeys(prev.MaxKey(), curr.MinKey()) >= 0 {
			return fmt.Errorf("imported SSTs have key range overlap: fid=%d <-> fid=%d",
				prev.fid, curr.fid)
		}
	}
	return nil
}

func (lm *levelManager) checkTablesOverlapWithL0(tables []*table) error {
	l0 := lm.levels[0]

	for _, tbl := range tables {
		for _, existing := range l0.tables {
			if existing == nil {
				continue
			}
			if utils.CompareUserKeys(tbl.MinKey(), existing.MaxKey()) <= 0 &&
				utils.CompareUserKeys(tbl.MaxKey(), existing.MinKey()) >= 0 {
				return fmt.Errorf("SST(fid=%d) overlaps with L0 existing table(fid=%d)",
					tbl.fid, existing.fid)
			}
		}
	}
	return nil
}

func (lm *levelManager) importExternalSST(paths []string) error {
	fs := vfs.Ensure(lm.opt.FS)
	workDir := lm.opt.WorkDir
	var (
		importedTables []*table
		importedMetas  []*manifest.FileMeta
		tempFIDs       []uint64
		pathMappings   = make(map[string]string)
	)

	rollback := func() {
		for sourcePath, targetPath := range pathMappings {
			if _, err := fs.Stat(targetPath); err == nil {
				_ = fs.Rename(targetPath, sourcePath)
			}
		}
		for _, tbl := range importedTables {
			if tbl != nil {
				lm.cache.delIndex(tbl.fid)
				_ = tbl.closeHandle()
			}
		}
		for _, fid := range tempFIDs {
			sstPath := utils.FileNameSSTable(workDir, fid)
			if _, err := fs.Stat(sstPath); err == nil {
				_ = fs.Remove(sstPath)
			}
		}

		rollbackEdits := make([]manifest.Edit, len(importedMetas))
		for i, meta := range importedMetas {
			rollbackEdits[i] = manifest.Edit{
				Type: manifest.EditDeleteFile,
				File: meta,
			}
		}
		_ = lm.manifestMgr.LogEdits(rollbackEdits...)
	}

	for _, path := range paths {
		stat, err := fs.Stat(path)
		if err != nil {
			rollback()
			return fmt.Errorf("invalid external SST: %s, err: %w", path, err)
		}
		if stat.IsDir() {
			rollback()
			return fmt.Errorf("external SST is a directory: %s", path)
		}
		if !strings.HasSuffix(path, ".sst") {
			rollback()
			return fmt.Errorf("external file is not an SST (missing .sst suffix): %s", path)
		}

		tempFID := lm.maxFID.Add(1)
		tempFIDs = append(tempFIDs, tempFID)
		targetPath := utils.FileNameSSTable(workDir, tempFID)

		if _, err := fs.Stat(targetPath); err == nil {
			rollback()
			return fmt.Errorf("target SST path already exists: %s", targetPath)
		}
		if err := fs.Rename(path, targetPath); err != nil {
			rollback()
			return fmt.Errorf("failed to move external SST: %s -> %s, err: %w", path, targetPath, err)
		}
		pathMappings[path] = targetPath

		tbl, err := openTable(lm, targetPath, nil)
		if err != nil {
			rollback()
			return fmt.Errorf("open imported sst failed: %s, err: %w", targetPath, err)
		}
		importedTables = append(importedTables, tbl)
	}

	if err := checkTablesOverlap(importedTables); err != nil {
		rollback()
		return fmt.Errorf("imported ssts overlap: %w", err)
	}

	l0 := lm.levels[0]
	l0.Lock()
	defer l0.Unlock()

	if err := lm.checkTablesOverlapWithL0(importedTables); err != nil {
		rollback()
		return fmt.Errorf("overlap with L0: %w", err)
	}

	for _, tbl := range importedTables {
		meta := &manifest.FileMeta{
			Level:     0,
			FileID:    tbl.fid,
			Size:      uint64(tbl.Size()),
			Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
			Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
			CreatedAt: uint64(time.Now().Unix()),
			ValueSize: tbl.ValueSize(),
			Ingest:    false,
		}
		importedMetas = append(importedMetas, meta)
	}

	edits := make([]manifest.Edit, len(importedMetas))
	for i, meta := range importedMetas {
		edits[i] = manifest.Edit{
			Type: manifest.EditAddFile,
			File: meta,
		}
	}

	if lm.opt.ManifestSync {
		if err := vfs.SyncDir(lm.opt.FS, workDir); err != nil {
			rollback()
			return fmt.Errorf("sync work dir failed: %w", err)
		}
	}

	if err := lm.manifestMgr.LogEdits(edits...); err != nil {
		rollback()
		return fmt.Errorf("log manifest edits failed: %w", err)
	}

	for _, tbl := range importedTables {
		if tbl == nil {
			continue
		}
		tbl.setLevel(l0.levelNum)
		l0.tables = append(l0.tables, tbl)
		l0.totalSize += tbl.Size()
		l0.totalStaleSize += int64(tbl.StaleDataSize())
		l0.totalValueSize += int64(tbl.ValueSize())
	}
	l0.sortTablesLocked()
	l0.ingest.sortShards()

	return nil
}
