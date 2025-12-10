package lsm

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	coreCache "github.com/feichai0017/NoKV/utils/cache"
)

const defaultCacheSize = 1024

type cache struct {
	indexs   *coreCache.Cache // key fid， value *pb.TableIndex
	indexHot *hotIndexCache
	blocks   *blockCache
	blooms   *bloomCache
	metrics  *cacheMetrics
}

// CacheMetrics captures cache hit/miss counters for read path observability.
type CacheMetrics struct {
	L0Hits      uint64
	L0Misses    uint64
	L1Hits      uint64
	L1Misses    uint64
	BloomHits   uint64
	BloomMisses uint64
	IndexHits   uint64
	IndexMisses uint64
}

type cacheMetrics struct {
	l0Hits      uint64
	l0Misses    uint64
	l1Hits      uint64
	l1Misses    uint64
	bloomHits   uint64
	bloomMisses uint64
	indexHits   uint64
	indexMisses uint64
}

type hotIndexCache struct {
	cp *clockProCache[*pb.TableIndex]
}

func newHotIndexCache(cap int) *hotIndexCache {
	if cap <= 0 {
		return nil
	}
	return &hotIndexCache{cp: newClockProCache[*pb.TableIndex](cap)}
}

func (hc *hotIndexCache) get(fid uint64) (*pb.TableIndex, bool) {
	if hc == nil || hc.cp == nil {
		return nil, false
	}
	return hc.cp.get(fid)
}

func (hc *hotIndexCache) promote(fid uint64, idx *pb.TableIndex) {
	if hc == nil || hc.cp == nil || idx == nil {
		return
	}
	hc.cp.promote(fid, idx)
}

type hotBloomCache struct {
	cp *clockProCache[utils.Filter]
}

func newHotBloomCache(cap int) *hotBloomCache {
	if cap <= 0 {
		return nil
	}
	return &hotBloomCache{
		cp: newClockProCache[utils.Filter](cap),
	}
}

func (hc *hotBloomCache) get(fid uint64) (utils.Filter, bool) {
	if hc == nil || hc.cp == nil {
		return nil, false
	}
	return hc.cp.get(fid)
}

func (hc *hotBloomCache) promote(fid uint64, filter utils.Filter) {
	if hc == nil || hc.cp == nil || len(filter) == 0 {
		return
	}
	dup := kv.SafeCopy(nil, filter)
	hc.cp.promote(fid, dup)
}

func (m *cacheMetrics) recordBlock(level int, hit bool) {
	switch level {
	case 0:
		if hit {
			atomic.AddUint64(&m.l0Hits, 1)
		} else {
			atomic.AddUint64(&m.l0Misses, 1)
		}
	case 1:
		if hit {
			atomic.AddUint64(&m.l1Hits, 1)
		} else {
			atomic.AddUint64(&m.l1Misses, 1)
		}
	}
}

func (m *cacheMetrics) recordBloom(hit bool) {
	if hit {
		atomic.AddUint64(&m.bloomHits, 1)
		return
	}
	atomic.AddUint64(&m.bloomMisses, 1)
}

func (m *cacheMetrics) snapshot() CacheMetrics {
	if m == nil {
		return CacheMetrics{}
	}
	return CacheMetrics{
		L0Hits:      atomic.LoadUint64(&m.l0Hits),
		L0Misses:    atomic.LoadUint64(&m.l0Misses),
		L1Hits:      atomic.LoadUint64(&m.l1Hits),
		L1Misses:    atomic.LoadUint64(&m.l1Misses),
		BloomHits:   atomic.LoadUint64(&m.bloomHits),
		BloomMisses: atomic.LoadUint64(&m.bloomMisses),
		IndexHits:   atomic.LoadUint64(&m.indexHits),
		IndexMisses: atomic.LoadUint64(&m.indexMisses),
	}
}

func (m *cacheMetrics) recordIndex(hit bool) {
	if hit {
		atomic.AddUint64(&m.indexHits, 1)
		return
	}
	atomic.AddUint64(&m.indexMisses, 1)
}

// close releases cache state.
func (c *cache) close() error {
	if c == nil {
		return nil
	}
	c.indexs = nil
	if c.blocks != nil {
		c.blocks.close()
		c.blocks = nil
	}
	if c.blooms != nil {
		c.blooms.close()
		c.blooms = nil
	}
	c.metrics = nil
	return nil
}

func newCache(opt *Options) *cache {
	metrics := &cacheMetrics{}
	hotCap := max(opt.BlockCacheSize, 0)
	blocks := newBlockCache(hotCap)
	blooms := newBloomCache(opt.BloomCacheSize)
	hotIdxCap := 64
	if opt != nil && opt.BlockCacheSize > 0 {
		hotIdxCap = min(max(opt.BlockCacheSize/64, 32), 256)
	}
	return &cache{
		indexs:   coreCache.NewCache(defaultCacheSize),
		indexHot: newHotIndexCache(hotIdxCap),
		blocks:   blocks,
		blooms:   blooms,
		metrics:  metrics,
	}
}

func (c *cache) addIndex(fid uint64, idx *pb.TableIndex) {
	if c == nil || c.indexs == nil {
		return
	}
	if idx == nil {
		return
	}
	c.indexs.Set(fid, idx)
	if c.indexHot != nil {
		c.indexHot.promote(fid, idx)
	}
}

func (c *cache) getIndex(fid uint64) (*pb.TableIndex, bool) {
	if c != nil && c.indexHot != nil {
		if idx, ok := c.indexHot.get(fid); ok && idx != nil {
			if c.metrics != nil {
				c.metrics.recordIndex(true)
			}
			return idx, true
		}
	}
	if c == nil || c.indexs == nil {
		return nil, false
	}
	val, ok := c.indexs.Get(fid)
	if c.metrics != nil {
		c.metrics.recordIndex(ok)
	}
	if !ok {
		return nil, false
	}
	index, ok := val.(*pb.TableIndex)
	if !ok || index == nil {
		return nil, false
	}
	return index, true
}

func (c *cache) delIndex(fid uint64) {
	if c == nil || c.indexs == nil {
		return
	}
	c.indexs.Del(fid)
}

func (c *cache) getBlock(level int, tbl *table, key uint64, hot bool) (*block, bool) {
	if c == nil || c.blocks == nil {
		return nil, false
	}
	blk, ok := c.blocks.get(level, tbl, key, hot)
	if ok {
		c.metrics.recordBlock(level, true)
		return blk, true
	}
	c.metrics.recordBlock(level, false)
	return nil, false
}

func (c *cache) addBlock(level int, tbl *table, key uint64, blk *block, hot bool) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.addWithTier(level, tbl, key, blk, hot)
}

func (c *cache) dropBlock(key uint64) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.del(nil, key)
}

func (c *cache) getBloom(fid uint64) (utils.Filter, bool) {
	if c == nil || c.blooms == nil {
		return nil, false
	}
	filter, ok := c.blooms.get(fid)
	c.metrics.recordBloom(ok)
	return filter, ok
}

func (c *cache) addBloom(fid uint64, filter utils.Filter) {
	if c == nil || c.blooms == nil {
		return
	}
	c.blooms.add(fid, filter)
}

func (c *cache) metricsSnapshot() CacheMetrics {
	if c == nil {
		return CacheMetrics{}
	}
	return c.metrics.snapshot()
}

type blockCache struct {
	rc       *ristretto.Cache[uint64, *blockEntry]
	hotMu    sync.Mutex
	hotCap   int
	hotItems map[uint64]*list.Element
	hotLRU   *list.List
}

type blockEntry struct {
	key uint64
	idx int
	tbl *table
	blk *block
}

func newBlockCache(capacity int) *blockCache {
	if capacity <= 0 {
		return nil
	}
	hotCap := min(min(max(capacity/8, 16), capacity), 256)
	bc := &blockCache{
		hotCap:   hotCap,
		hotItems: make(map[uint64]*list.Element, hotCap),
		hotLRU:   list.New(),
	}
	rc, err := ristretto.NewCache(&ristretto.Config[uint64, *blockEntry]{
		NumCounters: int64(capacity) * 8,
		MaxCost:     int64(capacity),
		BufferItems: 64,
		Cost: func(_ *blockEntry) int64 {
			return 1
		},
		OnEvict: func(item *ristretto.Item[*blockEntry]) {
			if item == nil || item.Value == nil {
				return
			}
			clearTableSlot(item.Value)
			bc.removeHotEntry(item.Value)
		},
	})
	if err != nil {
		return nil
	}
	bc.rc = rc
	return bc
}

func blockIndexFromKey(key uint64) int {
	return int(uint32(key))
}

func (c *blockCache) getHot(key uint64) (*blockEntry, bool) {
	if c == nil || c.hotCap == 0 {
		return nil, false
	}
	c.hotMu.Lock()
	defer c.hotMu.Unlock()
	elem, ok := c.hotItems[key]
	if !ok || elem == nil {
		return nil, false
	}
	c.hotLRU.MoveToFront(elem)
	be := elem.Value.(*blockEntry)
	return be, true
}

func (c *blockCache) promoteHot(be *blockEntry) {
	if c == nil || c.hotCap == 0 || be == nil {
		return
	}
	c.hotMu.Lock()
	defer c.hotMu.Unlock()
	if elem, ok := c.hotItems[be.key]; ok {
		elem.Value = be
		c.hotLRU.MoveToFront(elem)
		return
	}
	elem := c.hotLRU.PushFront(be)
	c.hotItems[be.key] = elem
	for c.hotLRU.Len() > c.hotCap {
		tail := c.hotLRU.Back()
		if tail == nil {
			break
		}
		tb := tail.Value.(*blockEntry)
		delete(c.hotItems, tb.key)
		c.hotLRU.Remove(tail)
	}
}

func (c *blockCache) removeHotEntry(be *blockEntry) {
	if c == nil || c.hotCap == 0 || be == nil {
		return
	}
	c.hotMu.Lock()
	defer c.hotMu.Unlock()
	elem, ok := c.hotItems[be.key]
	if !ok || elem == nil {
		return
	}
	if elem.Value == be {
		delete(c.hotItems, be.key)
		c.hotLRU.Remove(elem)
	}
}

func (c *blockCache) get(level int, tbl *table, key uint64, hotHint bool) (*block, bool) {
	if be, ok := c.getHot(key); ok && be != nil && be.blk != nil {
		if tbl != nil {
			c.storeTableSlot(tbl, be)
		}
		return be.blk, true
	}
	if tbl != nil {
		idx := blockIndexFromKey(key)
		if idx < len(tbl.cacheSlots) {
			if be := tbl.cacheSlots[idx]; be != nil && be.blk != nil {
				if hotHint {
					c.promoteHot(be)
				}
				return be.blk, true
			}
		}
	}
	if c == nil || c.rc == nil {
		return nil, false
	}
	if be, ok := c.rc.Get(key); ok && be != nil && be.blk != nil {
		if tbl != nil {
			c.storeTableSlot(tbl, be)
		}
		if hotHint {
			c.promoteHot(be)
		}
		return be.blk, true
	}
	return nil, false
}

func (c *blockCache) add(level int, tbl *table, key uint64, blk *block, hot bool) {
	if c == nil {
		return
	}
	c.addWithTier(level, tbl, key, blk, hot)
}

func (c *blockCache) addWithTier(level int, tbl *table, key uint64, blk *block, hot bool) {
	if c == nil || c.rc == nil || blk == nil {
		return
	}
	if level > 1 {
		return
	}
	entry := &blockEntry{
		key: key,
		idx: blockIndexFromKey(key),
		tbl: tbl,
		blk: blk,
	}
	if hot && c.hotCap > 0 {
		c.promoteHot(entry)
	}
	if tbl != nil {
		c.storeTableSlot(tbl, entry)
	}
	_ = c.rc.Set(key, entry, 1)
}

func (c *blockCache) del(tbl *table, key uint64) {
	if c == nil {
		return
	}
	if tbl != nil {
		idx := blockIndexFromKey(key)
		if idx < len(tbl.cacheSlots) && tbl.cacheSlots[idx] != nil {
			tbl.cacheSlots[idx] = nil
		}
	}
	if c.rc != nil {
		c.rc.Del(key)
	}
}

func (c *blockCache) close() {
	if c == nil {
		return
	}
	if c.rc != nil {
		c.rc.Close()
	}
}

func (c *blockCache) storeTableSlot(tbl *table, be *blockEntry) {
	if tbl == nil || be == nil {
		return
	}
	idx := be.idx
	if idx < 0 {
		return
	}
	if idx >= len(tbl.cacheSlots) {
		grown := make([]*blockEntry, idx+1)
		copy(grown, tbl.cacheSlots)
		tbl.cacheSlots = grown
	}
	tbl.cacheSlots[idx] = be
}

func clearTableSlot(be *blockEntry) {
	if be == nil || be.tbl == nil {
		return
	}
	idx := be.idx
	if idx < 0 || idx >= len(be.tbl.cacheSlots) {
		return
	}
	if be.tbl.cacheSlots[idx] == be {
		be.tbl.cacheSlots[idx] = nil
	}
}

type bloomCache struct {
	mu    sync.Mutex
	cap   int
	items map[uint64]*list.Element
	lru   *list.List
	hot   *hotBloomCache
}

type bloomEntry struct {
	fid    uint64
	filter utils.Filter
}

func newBloomCache(capacity int) *bloomCache {
	if capacity <= 0 {
		return nil
	}
	bc := &bloomCache{
		cap:   capacity,
		items: make(map[uint64]*list.Element, capacity),
		lru:   list.New(),
	}
	bc.hot = newHotBloomCache(min(max(capacity/8, 8), 256))
	return bc
}

func (bc *bloomCache) get(fid uint64) (utils.Filter, bool) {
	if bc == nil {
		return nil, false
	}
	if bc.hot != nil {
		if f, ok := bc.hot.get(fid); ok {
			return f, true
		}
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if elem, ok := bc.items[fid]; ok {
		bc.lru.MoveToFront(elem)
		entry := elem.Value.(*bloomEntry)
		return entry.filter, true
	}
	return nil, false
}

func (bc *bloomCache) add(fid uint64, filter utils.Filter) {
	if bc == nil || len(filter) == 0 {
		return
	}
	dup := kv.SafeCopy(nil, filter)
	if bc.hot != nil {
		bc.hot.promote(fid, dup)
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	if elem, ok := bc.items[fid]; ok {
		elem.Value.(*bloomEntry).filter = dup
		bc.lru.MoveToFront(elem)
		return
	}
	if bc.lru.Len() >= bc.cap {
		tail := bc.lru.Back()
		if tail != nil {
			entry := tail.Value.(*bloomEntry)
			delete(bc.items, entry.fid)
			bc.lru.Remove(tail)
		}
	}
	elem := bc.lru.PushFront(&bloomEntry{fid: fid, filter: dup})
	bc.items[fid] = elem
}

func (bc *bloomCache) close() {
	if bc == nil {
		return
	}
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.items = nil
	bc.lru = nil
	bc.cap = 0
}
