package lsm

import (
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
	"github.com/feichai0017/NoKV/lsm/tombstone"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/vfs"
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
	lm.compaction = newCompaction(lm, lm.opt.NumCompactors, lm.opt.CompactionPolicy, lsm.getLogger())
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
	compactState     *State
	compaction       *compaction
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
	for level, files := range version.Levels {
		for _, meta := range files {
			t, err := lm.openManifestTable(fs, level, meta)
			if err != nil {
				return err
			}
			if meta.FileID > maxFID {
				maxFID = meta.FileID
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
	return nil
}

func (lm *levelManager) openManifestTable(fs vfs.FS, level int, meta manifest.FileMeta) (*table, error) {
	fileName := utils.FileNameSSTable(lm.opt.WorkDir, meta.FileID)
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

// checkTablesOverlapWithL0Locked checks imported tables against existing L0
// tables. Caller must hold l0.Lock().
func (lm *levelManager) checkTablesOverlapWithL0Locked(tables []*table) error {
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
		if len(rollbackEdits) == 0 {
			return
		}
		if err := lm.manifestMgr.LogEdits(rollbackEdits...); err != nil {
			lm.getLogger().Error("failed to log import rollback edits", "error", err, "files", len(rollbackEdits))
		}
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

	if err := lm.checkTablesOverlapWithL0Locked(importedTables); err != nil {
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
	l0.rebuildRangeFilterLocked()

	return nil
}
