package lsm

import (
	"bytes"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/tombstone"
)

// RangeTombstoneView captures a stable read-view for range tombstone checks.
//
// The view pins the current memtable set once, then reuses it for repeated
// coverage probes. This avoids per-key GetMemTables pin/unpin overhead on scan
// paths (for example, DB iterators / YCSB-E).
//
// Call Close when finished.
type RangeTombstoneView struct {
	lsm     *LSM
	tables  []*memTable
	release func()
}

// HasAnyRangeTombstone reports whether the current LSM state has any in-memory
// or flushed range tombstones.
func (lsm *LSM) HasAnyRangeTombstone() bool {
	if lsm == nil {
		return false
	}
	for _, s := range lsm.shards {
		s.lock.RLock()
		mem := s.memTable
		immutables := s.immutables
		s.lock.RUnlock()
		if mem != nil && mem.hasRangeTombstones() {
			return true
		}
		for _, mt := range immutables {
			if mt != nil && mt.hasRangeTombstones() {
				return true
			}
		}
	}
	return lsm.RangeTombstoneCount() > 0
}

// PinRangeTombstoneView captures and pins the current memtable set for repeated
// range tombstone checks.
func (lsm *LSM) PinRangeTombstoneView() *RangeTombstoneView {
	if lsm == nil {
		return nil
	}
	tables, release := lsm.getMemTables()
	return &RangeTombstoneView{
		lsm:     lsm,
		tables:  tables,
		release: release,
	}
}

// IsKeyCovered checks whether userKey@version is covered in this pinned view.
func (v *RangeTombstoneView) IsKeyCovered(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if v == nil || v.lsm == nil {
		return false
	}
	return v.lsm.checkRangeTombstone(cf, userKey, version, v.tables)
}

// Close releases pinned memtables held by this view.
func (v *RangeTombstoneView) Close() {
	if v == nil {
		return
	}
	if v.release != nil {
		v.release()
	}
	v.tables = nil
	v.release = nil
	v.lsm = nil
}

// rebuildRangeTombstones scans SST levels to repopulate the range tombstone
// collector. Memtable tombstones are tracked separately in memTable.rangeTombstones
// and must not be included here to avoid duplication when those memtables flush.
// This is called at startup and after max-level compaction (which may drop tombstones).
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
