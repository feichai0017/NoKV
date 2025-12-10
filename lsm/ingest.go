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
}

func (buf *ingestBuffer) addBatch(ts []*table) {
	for _, t := range ts {
		buf.add(t)
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

func (buf ingestBuffer) cloneWithRefs() ingestBuffer {
	var out ingestBuffer
	out.shards = make([]ingestShard, len(buf.shards))
	for i, sh := range buf.shards {
		dst := &out.shards[i]
		dst.size = sh.size
		dst.valueSize = sh.valueSize
		dst.tables = append([]*table(nil), sh.tables...)
		for _, t := range dst.tables {
			if t != nil {
				t.IncrRef()
				dst.ranges = append(dst.ranges, tableRange{
					min: t.MinKey(),
					max: t.MaxKey(),
					tbl: t,
				})
			}
		}
		if len(dst.ranges) > 1 {
			sort.Slice(dst.ranges, func(i, j int) bool {
				return utils.CompareKeys(dst.ranges[i].min, dst.ranges[j].min) < 0
			})
		}
	}
	return out
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
		idx := sort.Search(len(sh.ranges), func(i int) bool {
			return utils.CompareKeys(key, sh.ranges[i].max) <= 0
		})
		for i := idx; i < len(sh.ranges); i++ {
			rng := sh.ranges[i]
			if rng.tbl == nil {
				continue
			}
			if utils.CompareKeys(key, rng.min) < 0 {
				break
			}
			if utils.CompareKeys(key, rng.max) > 0 {
				continue
			}
			if entry, err := rng.tbl.Search(key, &version); err == nil {
				return entry, nil
			}
		}
	}
	return nil, utils.ErrKeyNotFound
}

// splitIntoRanges picks up to maxParts non-overlapping batches from the shard with the largest backlog.
func (buf *ingestBuffer) largestShard() *ingestShard {
	buf.ensureInit()
	var sel *ingestShard
	var maxSize int64
	for i := range buf.shards {
		sh := &buf.shards[i]
		if sh.size > maxSize && len(sh.tables) > 0 {
			maxSize = sh.size
			sel = sh
		}
	}
	return sel
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
	lh.refreshViewLocked()
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
		lh.ingest.add(t)
	}
	lh.refreshViewLocked()
}

func (lh *levelHandler) ingestValueBytes() int64 {
	if v := lh.loadView(); v != nil {
		return v.ingest.totalValueSize()
	}
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.totalValueSize()
}

func (lh *levelHandler) ingestValueDensity() float64 {
	if v := lh.loadView(); v != nil {
		total := v.ingest.totalSize()
		if total <= 0 {
			return 0
		}
		return float64(v.ingest.totalValueSize()) / float64(total)
	}
	lh.RLock()
	defer lh.RUnlock()
	total := lh.ingest.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.ingest.totalValueSize()) / float64(total)
}

func (lh *levelHandler) ingestValueBias(weight float64) float64 {
	if weight <= 0 {
		return 1.0
	}
	density := lh.ingestValueDensity()
	bias := 1.0 + weight*density
	if bias > 4.0 {
		return 4.0
	}
	if bias < 1.0 {
		return 1.0
	}
	return bias
}

func (lh *levelHandler) ingestDensityLocked() float64 {
	total := lh.ingest.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(lh.ingest.totalValueSize()) / float64(total)
}

func (lh *levelHandler) maxIngestAgeSeconds() float64 {
	v := lh.loadView()
	if v != nil {
		return v.ingest.maxAgeSeconds()
	}
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.maxAgeSeconds()
}

func (lh *levelHandler) ingestDensityLockedView(v *levelView) float64 {
	if v == nil {
		return 0
	}
	total := v.ingest.totalSize()
	if total <= 0 {
		return 0
	}
	return float64(v.ingest.totalValueSize()) / float64(total)
}

func (lh *levelHandler) numIngestTables() int {
	if v := lh.loadView(); v != nil {
		return v.ingest.tableCount()
	}
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.tableCount()
}

func (lh *levelHandler) ingestDataSize() int64 {
	if v := lh.loadView(); v != nil {
		return v.ingest.totalSize()
	}
	lh.RLock()
	defer lh.RUnlock()
	return lh.ingest.totalSize()
}

func (lh *levelHandler) searchIngestSSTView(key []byte, v *levelView) (*kv.Entry, error) {
	if v == nil {
		return nil, utils.ErrKeyNotFound
	}
	return v.ingest.search(key)
}
