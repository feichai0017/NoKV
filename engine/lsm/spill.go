package lsm

import (
	"fmt"
	"sort"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

const (
	spillShardBits  = 2
	spillShardCount = 1 << spillShardBits
)

type spillShard struct {
	tables    []*table
	ranges    []tableRange
	prefixMax [][]byte
	size      int64
	valueSize int64
}

func (sh *spillShard) rebuildRanges() {
	if sh == nil {
		return
	}
	sh.ranges = sh.ranges[:0]
	sh.prefixMax = sh.prefixMax[:0]
	for _, t := range sh.tables {
		if t == nil {
			continue
		}
		sh.ranges = append(sh.ranges, tableRange{
			min: t.MinKey(),
			max: t.MaxKey(),
			tbl: t,
		})
	}
	if len(sh.ranges) > 1 {
		sort.Slice(sh.ranges, func(i, j int) bool {
			return kv.CompareInternalKeys(sh.ranges[i].min, sh.ranges[j].min) < 0
		})
	}
	var max []byte
	for _, rng := range sh.ranges {
		if max == nil || kv.CompareBaseKeys(rng.max, max) > 0 {
			max = rng.max
		}
		sh.prefixMax = append(sh.prefixMax, max)
	}
}

type spillBuffer struct {
	shards []spillShard
}

func (buf *spillBuffer) ensureInit() {
	if buf.shards == nil {
		buf.shards = make([]spillShard, spillShardCount)
	}
}

func shardIndexForRange(min []byte) int {
	_, userKey, _, ok := kv.SplitInternalKey(min)
	utils.CondPanicFunc(!ok, func() error {
		return fmt.Errorf("spill shardIndexForRange expects internal key: %x", min)
	})
	if len(userKey) == 0 {
		return 0
	}
	// Use the top bits of the first byte to partition into fixed shards.
	return int(userKey[0] >> (8 - spillShardBits))
}

func (buf *spillBuffer) add(t *table) {
	if t == nil {
		return
	}
	buf.ensureInit()
	idx := shardIndexForRange(t.MinKey())
	sh := &buf.shards[idx]
	sh.tables = append(sh.tables, t)
	sh.size += t.Size()
	sh.valueSize += int64(t.ValueSize())
	sh.rebuildRanges()
}

func (buf *spillBuffer) addBatch(ts []*table) {
	if len(ts) == 0 {
		return
	}
	buf.ensureInit()
	updated := make(map[int]struct{})
	for _, t := range ts {
		if t == nil {
			continue
		}
		idx := shardIndexForRange(t.MinKey())
		sh := &buf.shards[idx]
		sh.tables = append(sh.tables, t)
		sh.size += t.Size()
		sh.valueSize += int64(t.ValueSize())
		updated[idx] = struct{}{}
	}
	for idx := range updated {
		buf.shards[idx].rebuildRanges()
	}
}

func (buf *spillBuffer) remove(toDel map[uint64]struct{}) {
	if len(toDel) == 0 {
		return
	}
	buf.ensureInit()
	for i := range buf.shards {
		sh := &buf.shards[i]
		if len(sh.tables) == 0 {
			continue
		}
		var kept []*table
		for _, t := range sh.tables {
			if t == nil {
				continue
			}
			if _, drop := toDel[t.fid]; drop {
				sh.size -= t.Size()
				sh.valueSize -= int64(t.ValueSize())
				continue
			}
			kept = append(kept, t)
		}
		sh.tables = kept
		if sh.size < 0 {
			sh.size = 0
		}
		if sh.valueSize < 0 {
			sh.valueSize = 0
		}
		sh.rebuildRanges()
	}
}

func (buf spillBuffer) tableCount() int {
	var n int
	for _, sh := range buf.shards {
		n += len(sh.tables)
	}
	return n
}

func (buf spillBuffer) totalSize() int64 {
	var n int64
	for _, sh := range buf.shards {
		n += sh.size
	}
	return n
}

func (buf spillBuffer) totalValueSize() int64 {
	var n int64
	for _, sh := range buf.shards {
		n += sh.valueSize
	}
	return n
}

func (buf spillBuffer) allTables() []*table {
	var out []*table
	for _, sh := range buf.shards {
		out = append(out, sh.tables...)
	}
	return out
}

func (buf *spillBuffer) allMeta() []TableMeta {
	if buf == nil {
		return nil
	}
	buf.ensureInit()
	return tableMetaSnapshot(buf.allTables())
}

func (buf *spillBuffer) shardMetaByIndex(idx int) []TableMeta {
	if buf == nil {
		return nil
	}
	buf.ensureInit()
	if idx < 0 || idx >= len(buf.shards) {
		return nil
	}
	sh := buf.shards[idx]
	if len(sh.tables) == 0 {
		return nil
	}
	return tableMetaSnapshot(sh.tables)
}

func (buf *spillBuffer) sortShards() {
	buf.ensureInit()
	for i := range buf.shards {
		sh := &buf.shards[i]
		if len(sh.tables) > 1 {
			sort.Slice(sh.tables, func(a, b int) bool {
				return kv.CompareInternalKeys(sh.tables[a].MinKey(), sh.tables[b].MinKey()) < 0
			})
		}
		sh.rebuildRanges()
	}
}

func (buf spillBuffer) shardViews() []SpillShardView {
	buf.ensureInit()
	now := time.Now()
	var views []SpillShardView
	for i, sh := range buf.shards {
		if len(sh.tables) == 0 {
			continue
		}
		maxAge := float64(0)
		if len(sh.tables) > 0 {
			for _, t := range sh.tables {
				if t == nil {
					continue
				}
				age := now.Sub(t.createdAt).Seconds()
				if age > maxAge {
					maxAge = age
				}
			}
		}
		density := float64(0)
		if sh.size > 0 {
			density = float64(sh.valueSize) / float64(sh.size)
		}
		views = append(views, SpillShardView{
			Index:        i,
			TableCount:   len(sh.tables),
			SizeBytes:    sh.size,
			ValueBytes:   sh.valueSize,
			MaxAgeSec:    maxAge,
			ValueDensity: density,
		})
	}
	return views
}

func (buf spillBuffer) search(key []byte, maxVersion *uint64) (*kv.Entry, error) {
	if maxVersion == nil {
		var tmp uint64
		maxVersion = &tmp
	}
	var best *kv.Entry
	for _, sh := range buf.shards {
		if len(sh.ranges) == 0 {
			continue
		}
		ranges := sh.ranges
		if len(ranges) == 0 {
			continue
		}
		lo, hi := 0, len(ranges)
		for lo < hi {
			mid := (lo + hi) / 2
			if kv.CompareBaseKeys(key, ranges[mid].min) >= 0 {
				lo = mid + 1
			} else {
				hi = mid
			}
		}
		for i := lo - 1; i >= 0; i-- {
			if i < len(sh.prefixMax) && kv.CompareBaseKeys(key, sh.prefixMax[i]) > 0 {
				break
			}
			rng := ranges[i]
			if rng.tbl == nil {
				continue
			}
			if kv.CompareBaseKeys(key, rng.max) > 0 {
				continue
			}
			if rng.tbl.MaxVersionVal() <= *maxVersion {
				continue
			}
			if entry, err := rng.tbl.Search(key, maxVersion); err == nil {
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
	}
	if best != nil {
		return best, nil
	}
	return nil, utils.ErrKeyNotFound
}

func (buf spillBuffer) shardOrderBySize() []int {
	buf.ensureInit()
	views := buf.shardViews()
	return PickShardOrder(SpillPickInput{Shards: views})
}

func (lh *levelHandler) spillShardByBacklog() int {
	lh.spill.ensureInit()
	views := lh.spill.shardViews()
	return PickShardByBacklog(SpillPickInput{Shards: views})
}

func (buf spillBuffer) maxAgeSeconds() float64 {
	now := time.Now()
	var maxAge float64
	for _, sh := range buf.shards {
		for _, t := range sh.tables {
			if t == nil {
				continue
			}
			age := now.Sub(t.createdAt).Seconds()
			if age > maxAge {
				maxAge = age
			}
		}
	}
	return maxAge
}

func (buf spillBuffer) tablesWithinBounds(lower, upper []byte) []*table {
	var tables []*table
	for _, sh := range buf.shards {
		if len(sh.tables) == 0 {
			continue
		}
		matched := filterTablesByBounds(sh.tables, lower, upper)
		if len(matched) == 0 {
			continue
		}
		tables = append(tables, matched...)
	}
	return tables
}

// ---- levelHandler helpers that wrap the buffer ----

func (lh *levelHandler) addSpill(t *table) {
	if t == nil {
		return
	}
	lh.Lock()
	defer lh.Unlock()
	lh.spill.ensureInit()
	t.setLevel(lh.levelNum)
	lh.spill.add(t)
}

func (lh *levelHandler) spillValueBytes() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.spill.totalValueSize()
}

func (lh *levelHandler) spillValueDensity() float64 {
	lh.RLock()
	defer lh.RUnlock()
	total := lh.spill.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.spill.totalValueSize()) / float64(total)
}

// spillDensityLocked computes spill value density; caller must hold lh lock.
func (lh *levelHandler) spillDensityLocked() float64 {
	total := lh.spill.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.spill.totalValueSize()) / float64(total)
}

func (lh *levelHandler) maxSpillAgeSeconds() float64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.spill.maxAgeSeconds()
}

func (lh *levelHandler) numSpillTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return lh.spill.tableCount()
}

// numSpillTablesLocked returns the spill table count without acquiring the lock.
// Caller must already hold at least a read lock.
func (lh *levelHandler) numSpillTablesLocked() int {
	return lh.spill.tableCount()
}

func (lh *levelHandler) spillDataSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.spill.totalSize()
}
