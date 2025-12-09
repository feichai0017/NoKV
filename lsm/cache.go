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
	indexs  *coreCache.Cache // key fid， value *pb.TableIndex
	blocks  *blockCache
	blooms  *bloomCache
	metrics *cacheMetrics
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
	return &cache{
		indexs:  coreCache.NewCache(defaultCacheSize),
		blocks:  blocks,
		blooms:  blooms,
		metrics: metrics,
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
}

func (c *cache) getIndex(fid uint64) (*pb.TableIndex, bool) {
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

func (c *cache) getBlock(level int, tbl *table, key uint64) (*block, bool) {
	if c == nil || c.blocks == nil {
		return nil, false
	}
	blk, ok := c.blocks.get(level, tbl, key)
	if ok {
		c.metrics.recordBlock(level, true)
		return blk, true
	}
	c.metrics.recordBlock(level, false)
	return nil, false
}

func (c *cache) addBlock(level int, tbl *table, key uint64, blk *block) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.addWithTier(level, tbl, key, blk)
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
	rc *ristretto.Cache[uint64, *blockEntry]
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
		},
	})
	if err != nil {
		return nil
	}
	return &blockCache{rc: rc}
}

func blockIndexFromKey(key uint64) int {
	return int(uint32(key))
}

func (c *blockCache) get(level int, tbl *table, key uint64) (*block, bool) {
	if tbl != nil {
		idx := blockIndexFromKey(key)
		if idx < len(tbl.cacheSlots) {
			if be := tbl.cacheSlots[idx]; be != nil && be.blk != nil {
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
		return be.blk, true
	}
	return nil, false
}

func (c *blockCache) add(level int, tbl *table, key uint64, blk *block) {
	if c == nil {
		return
	}
	c.addWithTier(level, tbl, key, blk)
}

func (c *blockCache) addWithTier(level int, tbl *table, key uint64, blk *block) {
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
}

type bloomEntry struct {
	fid    uint64
	filter utils.Filter
}

func newBloomCache(capacity int) *bloomCache {
	if capacity <= 0 {
		return nil
	}
	return &bloomCache{
		cap:   capacity,
		items: make(map[uint64]*list.Element, capacity),
		lru:   list.New(),
	}
}

func (bc *bloomCache) get(fid uint64) (utils.Filter, bool) {
	if bc == nil {
		return nil, false
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
