package lsm

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/tombstone"
	"github.com/feichai0017/NoKV/engine/manifest"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/feichai0017/NoKV/engine/wal"
	"github.com/feichai0017/NoKV/metrics"
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
	lm.compactionPacer = newCompactionPacer(opt.CompactionWriteBytesPerSec)
	lm.compaction = newCompaction(lm, lm.opt.NumCompactors, lm.opt.CompactionPolicy, lsm.getLogger())
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
	compactionPacer  *compactionPacer
	rtCollector      *tombstone.Collector
	compactionLastNs atomic.Int64
	compactionMaxNs  atomic.Int64
	compactionRuns   atomic.Uint64
	rangeFilter      rangeFilterMetrics
}

type rangeFilterMetrics struct {
	pointCandidates   atomic.Uint64
	pointPruned       atomic.Uint64
	boundedCandidates atomic.Uint64
	boundedPruned     atomic.Uint64
	fallbacks         atomic.Uint64
}

func (lm *levelManager) recordRangeFilterPoint(total, candidates int, fallback bool) {
	if lm == nil {
		return
	}
	if candidates < 0 {
		candidates = 0
	}
	if total < candidates {
		total = candidates
	}
	lm.rangeFilter.pointCandidates.Add(uint64(candidates))
	lm.rangeFilter.pointPruned.Add(uint64(total - candidates))
	if fallback {
		lm.rangeFilter.fallbacks.Add(1)
	}
}

func (lm *levelManager) recordRangeFilterBounded(total, candidates int, fallback bool) {
	if lm == nil {
		return
	}
	if candidates < 0 {
		candidates = 0
	}
	if total < candidates {
		total = candidates
	}
	lm.rangeFilter.boundedCandidates.Add(uint64(candidates))
	lm.rangeFilter.boundedPruned.Add(uint64(total - candidates))
	if fallback {
		lm.rangeFilter.fallbacks.Add(1)
	}
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

func (lm *levelManager) iterators(opt *index.Options) []index.Iterator {
	itrs := make([]index.Iterator, 0, len(lm.levels))
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

// ValueLogHead returns manifest-tracked per-bucket active value-log heads.
func (lm *levelManager) ValueLogHead() map[uint32]manifest.ValueLogMeta {
	return lm.manifestMgr.ValueLogHead()
}

// ValueLogStatus returns manifest metadata for all known value-log files.
func (lm *levelManager) ValueLogStatus() map[manifest.ValueLogID]manifest.ValueLogMeta {
	return lm.manifestMgr.ValueLogStatus()
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
