package lsm

import (
	"bytes"
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

// RangeTombstone represents a single range deletion marker held in memory.
type RangeTombstone struct {
	CF      kv.ColumnFamily
	Start   []byte // inclusive
	End     []byte // exclusive
	Version uint64
}

// RangeTombstoneCollector maintains an in-memory collection of all active
// range tombstones for fast coverage checks during reads. It replaces the
// previous O(N) full-LSM scan per read with an O(M) in-memory check where
// M is the (typically small) number of range tombstones.
type RangeTombstoneCollector struct {
	mu         sync.RWMutex
	tombstones []RangeTombstone
}

// NewRangeTombstoneCollector creates a new empty collector.
func NewRangeTombstoneCollector() *RangeTombstoneCollector {
	return &RangeTombstoneCollector{}
}

// IsKeyCovered checks if userKey@version in the given CF is covered
// by any range tombstone. A tombstone covers the key when:
//   - CF matches
//   - tombstone version > key version
//   - userKey is in [Start, End)
func (c *RangeTombstoneCollector) IsKeyCovered(cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for i := range c.tombstones {
		rt := &c.tombstones[i]
		if rt.CF != cf {
			continue
		}
		if rt.Version > version && kv.KeyInRange(userKey, rt.Start, rt.End) {
			return true
		}
	}
	return false
}

// Add appends a single tombstone to the collector.
func (c *RangeTombstoneCollector) Add(rt RangeTombstone) {
	c.mu.Lock()
	c.tombstones = append(c.tombstones, rt)
	c.mu.Unlock()
}

// Rebuild completely replaces the tombstone set. Tombstones are sorted
// by (CF, Start) for cache-friendly access patterns.
func (c *RangeTombstoneCollector) Rebuild(tombstones []RangeTombstone) {
	sort.Slice(tombstones, func(i, j int) bool {
		if tombstones[i].CF != tombstones[j].CF {
			return tombstones[i].CF < tombstones[j].CF
		}
		return bytes.Compare(tombstones[i].Start, tombstones[j].Start) < 0
	})
	c.mu.Lock()
	c.tombstones = tombstones
	c.mu.Unlock()
}

// Count returns the current number of tracked tombstones.
func (c *RangeTombstoneCollector) Count() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.tombstones)
}

// rebuildRangeTombstones scans SST levels to repopulate the range tombstone
// collector. Memtable tombstones are tracked separately in memTable.rangeTombstones
// and must not be included here to avoid duplication when those memtables flush.
// This is called at startup and after max-level compaction (which may drop tombstones).
func (lm *levelManager) rebuildRangeTombstones() {
	if lm == nil || lm.rtCollector == nil || len(lm.levels) == 0 {
		return
	}
	var tombstones []RangeTombstone
	opt := &utils.Options{IsAsc: true}
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
					cf, start, version := kv.SplitInternalKey(e.Key)
					tombstones = append(tombstones, RangeTombstone{
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
	lm.rtCollector.Rebuild(tombstones)
}
