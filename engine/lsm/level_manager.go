package lsm

import (
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync/atomic"
	"time"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	cachepkg "github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/lsm/table"
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
	lm.compactor = newCompactor(lm, opt)
	return lm, nil
}

type levelManager struct {
	maxFID      atomic.Uint64
	opt         *Options
	cache       *cachepkg.Cache
	manifestMgr *manifest.Manager
	levels      []*levelHandler
	lsm         *LSM
	rtCollector *tombstone.Collector
	compactor   *compactor
	rangeFilter rangeFilterMetrics
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
			tables:   make([]*table.Table, 0),
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
				lm.levels[level].addLandingTable(t)
				continue
			}
			lm.levels[level].addTable(t)
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

func (lm *levelManager) openManifestTable(fs vfs.FS, level int, meta manifest.FileMeta) (*table.Table, error) {
	fileName := vfs.FileNameSSTable(lm.opt.WorkDir, meta.FileID)
	if _, err := fs.Stat(fileName); err != nil {
		return nil, fmt.Errorf("lsm startup: manifest references missing sstable L%d F%d (%s): %w", level, meta.FileID, fileName, err)
	}
	t, err := table.Open(lm, fileName, nil)
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
	builder := table.NewBuilder(tableOptionsFor(lm.opt))
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
	tbl, err := table.Open(lm, sstName, builder)
	if err != nil {
		return fmt.Errorf("failed to build sstable %s: %w", sstName, err)
	}
	if tbl == nil {
		return fmt.Errorf("failed to build sstable %s: nil table", sstName)
	}
	meta := &manifest.FileMeta{
		Level:     0,
		FileID:    fid,
		Size:      uint64(tbl.Size()),
		Smallest:  kv.SafeCopy(nil, tbl.MinKey()),
		Largest:   kv.SafeCopy(nil, tbl.MaxKey()),
		CreatedAt: uint64(time.Now().Unix()),
		ValueSize: tbl.ValueSize(),
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
		// is enforced by flush.Runtime (per-shard queue + inFlight flag)
		// so this Store cannot race against a later same-shard flush.
		// The `> cur` guard is kept as belt-and-braces against future
		// runtime regressions.
		if cur := shard.highestFlushedSeg.Load(); immutable.segmentID > cur {
			shard.highestFlushedSeg.Store(immutable.segmentID)
		}
	}
	lm.levels[0].addTable(tbl)
	// Register any range tombstones discovered during this flush.
	if lm.rtCollector != nil {
		for _, rt := range newTombstones {
			lm.rtCollector.Add(rt)
		}
	}
	if err := immutable.shard.wal.RemoveSegment(uint32(fid)); err != nil && !errors.Is(err, os.ErrNotExist) && !errors.Is(err, wal.ErrSegmentRetained) {
		return err
	}
	if lm.compactor.sched != nil {
		lm.compactor.sched.Trigger()
	}
	return nil
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
// rebuildRangeTombstones scans SST levels to repopulate the range tombstone
// collector. Memtable tombstones are tracked separately in
// memTable.rangeTombstones and must not be included here to avoid duplication
// when those memtables flush. Called at startup and after max-level
// compaction (which may drop tombstones).
func (lm *levelManager) rebuildRangeTombstones() {
	if lm == nil || lm.rtCollector == nil || len(lm.levels) == 0 {
		return
	}
	var ranges []tombstone.Range
	opt := &index.Options{IsAsc: true}
	// Only scan SST levels — memtable tombstones are tracked separately
	// in memTable.rangeTombstones and must not be duplicated here.
	iters := lm.iterators(opt)
	defer func() {
		for _, it := range iters {
			if it != nil {
				_ = it.Close()
			}
		}
	}()
	for _, it := range iters {
		if it == nil {
			continue
		}
		it.Rewind()
		for it.Valid() {
			if item := it.Item(); item != nil {
				if e := item.Entry(); e != nil && e.IsRangeDelete() {
					cf, start, version, ok := kv.SplitInternalKey(e.Key)
					if !ok {
						it.Next()
						continue
					}
					if bytes.Compare(start, e.RangeEnd()) >= 0 {
						it.Next()
						continue
					}
					ranges = append(ranges, tombstone.Range{
						CF:      cf,
						Start:   kv.SafeCopy(nil, start),
						End:     kv.SafeCopy(nil, e.RangeEnd()),
						Version: version,
					})
				}
			}
			it.Next()
		}
	}
	lm.rtCollector.Rebuild(ranges)
}

func (lm *levelManager) Cache() *cachepkg.Cache {
	if lm == nil {
		return nil
	}
	return lm.cache
}

func (lm *levelManager) Options() table.Options {
	return tableOptionsFor(lm.opt)
}

func tableOptionsFor(o *Options) table.Options {
	if o == nil {
		return table.Options{}
	}
	out := table.Options{
		WorkDir:            o.WorkDir,
		FS:                 o.FS,
		SSTableMaxSize:     o.SSTableMaxSz,
		BlockSize:          int64(o.BlockSize),
		BloomFalsePositive: o.BloomFalsePositive,
		BlockCompression:   o.BlockCompression,
		ManifestSync:       o.ManifestSync,
	}
	if o.PrefixExtractor != nil {
		out.PrefixExtractor = func(b []byte) []byte { return o.PrefixExtractor(b) }
	}
	return out
}

func entryValueLen(e *kv.Entry) uint32 {
	if e == nil {
		return 0
	}
	return uint32(len(e.Value))
}
