package lsm

import (
	"sort"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

const (
	ingestShardBits  = 2
	ingestShardCount = 1 << ingestShardBits
)

type ingestShard struct {
	tables    []*table
	ranges    []tableRange
	size      int64
	valueSize int64
}

func (sh *ingestShard) rebuildRanges() {
	if sh == nil {
		return
	}
	sh.ranges = sh.ranges[:0]
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
			return utils.CompareKeys(sh.ranges[i].min, sh.ranges[j].min) < 0
		})
	}
}

type ingestBuffer struct {
	shards []ingestShard
}

func (buf *ingestBuffer) ensureInit() {
	if buf.shards == nil {
		buf.shards = make([]ingestShard, ingestShardCount)
	}
}

func shardIndexForRange(min []byte) int {
	if len(min) == 0 {
		return 0
	}
	// Use the top bits of the first byte to partition into fixed shards.
	return int(min[0] >> (8 - ingestShardBits))
}

func (buf *ingestBuffer) add(t *table) {
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

func (buf *ingestBuffer) addBatch(ts []*table) {
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

func (buf *ingestBuffer) remove(toDel map[uint64]struct{}) {
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

func (buf ingestBuffer) tableCount() int {
	var n int
	for _, sh := range buf.shards {
		n += len(sh.tables)
	}
	return n
}

func (buf ingestBuffer) totalSize() int64 {
	var n int64
	for _, sh := range buf.shards {
		n += sh.size
	}
	return n
}

func (buf ingestBuffer) totalValueSize() int64 {
	var n int64
	for _, sh := range buf.shards {
		n += sh.valueSize
	}
	return n
}

func (buf ingestBuffer) allTables() []*table {
	var out []*table
	for _, sh := range buf.shards {
		out = append(out, sh.tables...)
	}
	return out
}

func (buf *ingestBuffer) sortShards() {
	buf.ensureInit()
	for i := range buf.shards {
		sh := &buf.shards[i]
		if len(sh.tables) > 1 {
			sort.Slice(sh.tables, func(a, b int) bool {
				return utils.CompareKeys(sh.tables[a].MinKey(), sh.tables[b].MinKey()) < 0
			})
		}
		sh.rebuildRanges()
	}
}

func (buf ingestBuffer) prefetch(key []byte, hot bool) bool {
	for _, sh := range buf.shards {
		for _, table := range sh.tables {
			if table == nil {
				continue
			}
			if utils.CompareKeys(key, table.MinKey()) < 0 ||
				utils.CompareKeys(key, table.MaxKey()) > 0 {
				continue
			}
			if table.prefetchBlockForKey(key, hot) {
				return true
			}
		}
	}
	return false
}

func (buf ingestBuffer) search(key []byte) (*kv.Entry, error) {
	var version uint64
	for _, sh := range buf.shards {
		if len(sh.ranges) == 0 {
			continue
		}
		for _, rng := range sh.ranges {
			if rng.tbl == nil {
				continue
			}
			if utils.CompareUserKeys(key, rng.min) < 0 {
				break
			}
			if utils.CompareUserKeys(key, rng.max) > 0 {
				continue
			}
			if entry, err := rng.tbl.Search(key, &version); err == nil {
				return entry, nil
			}
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (buf ingestBuffer) shardOrderBySize() []int {
	buf.ensureInit()
	type shardView struct {
		idx  int
		size int64
	}
	var views []shardView
	for i, sh := range buf.shards {
		if len(sh.tables) == 0 {
			continue
		}
		views = append(views, shardView{idx: i, size: sh.size})
	}
	sort.Slice(views, func(i, j int) bool { return views[i].size > views[j].size })
	var out []int
	for _, v := range views {
		out = append(out, v.idx)
	}
	return out
}

func (buf ingestBuffer) shardByIndex(idx int) *ingestShard {
	buf.ensureInit()
	if idx < 0 || idx >= len(buf.shards) {
		return nil
	}
	return &buf.shards[idx]
}

func (lh *levelHandler) ingestShardByBacklog() int {
	lh.ingest.ensureInit()
	order := lh.ingest.shardOrderBySize()
	if len(order) == 0 {
		return -1
	}
	return order[0]
}

func (buf ingestBuffer) maxAgeSeconds() float64 {
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

func (buf ingestBuffer) iterators(topt *utils.Options) []utils.Iterator {
	var itrs []utils.Iterator
	for _, sh := range buf.shards {
		if len(sh.tables) == 0 {
			continue
		}
		itrs = append(itrs, iteratorsReversed(sh.tables, topt)...)
	}
	return itrs
}

// ---- levelHandler helpers that wrap the buffer ----

func (lh *levelHandler) addIngest(t *table) {
	if t == nil {
		return
	}
	lh.Lock()
	defer lh.Unlock()
	lh.ingest.ensureInit()
	t.setLevel(lh.levelNum)
	lh.ingest.add(t)
}

func (lh *levelHandler) addIngestBatch(ts []*table) {
	lh.Lock()
	defer lh.Unlock()
	lh.ingest.ensureInit()
	for _, t := range ts {
		if t == nil {
			continue
		}
		t.setLevel(lh.levelNum)
	}
	lh.ingest.addBatch(ts)
}

func (lh *levelHandler) ingestValueBytes() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.totalValueSize()
}

func (lh *levelHandler) ingestValueDensity() float64 {
	lh.RLock()
	defer lh.RUnlock()
	total := lh.ingest.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.ingest.totalValueSize()) / float64(total)
}

func (lh *levelHandler) ingestDensityLocked() float64 {
	total := lh.ingest.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.ingest.totalValueSize()) / float64(total)
}

func (lh *levelHandler) maxIngestAgeSeconds() float64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.maxAgeSeconds()
}

func (lh *levelHandler) numIngestTables() int {
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.tableCount()
}

func (lh *levelHandler) ingestDataSize() int64 {
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.totalSize()
}

func (lh *levelHandler) searchIngestSST(key []byte) (*kv.Entry, error) {
	return lh.ingest.search(key)
}
