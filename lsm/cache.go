package lsm

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/utils"
	coreCache "github.com/feichai0017/NoKV/utils/cache"
)

const defaultCacheSize = 1024

type cache struct {
	indexs  *coreCache.Cache // key fid， value table
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
}

type cacheMetrics struct {
	l0Hits      uint64
	l0Misses    uint64
	l1Hits      uint64
	l1Misses    uint64
	bloomHits   uint64
	bloomMisses uint64
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
	}
}

// close releases cache state.
func (c *cache) close() error { return nil }

func newCache(opt *Options) *cache {
	metrics := &cacheMetrics{}
	hotCap := opt.BlockCacheSize
	if hotCap < 0 {
		hotCap = 0
	}
	hotFraction := opt.BlockCacheHotFraction
	if hotCap == 0 || hotFraction <= 0 || hotFraction >= 1 {
		hotFraction = 0
	}
	hotSize := int(float64(hotCap) * hotFraction)
	if hotFraction > 0 && hotSize == 0 && hotCap > 0 {
		hotSize = 1
	}
	if hotSize > hotCap {
		hotSize = hotCap
	}
	coldSize := hotCap - hotSize
	blocks := newBlockCache(hotSize, coldSize, metrics)
	blooms := newBloomCache(opt.BloomCacheSize)
	return &cache{
		indexs:  coreCache.NewCache(defaultCacheSize),
		blocks:  blocks,
		blooms:  blooms,
		metrics: metrics,
	}
}

func (c *cache) addIndex(fid uint64, t *table) {
	if c == nil || c.indexs == nil {
		return
	}
	c.indexs.Set(fid, t)
}

func (c *cache) getBlock(level int, key uint64) (*block, bool) {
	if c == nil || c.blocks == nil {
		return nil, false
	}
	blk, ok := c.blocks.get(level, key)
	if ok {
		c.metrics.recordBlock(level, true)
		return blk, true
	}
	c.metrics.recordBlock(level, false)
	return nil, false
}

func (c *cache) addBlock(level int, key uint64, blk *block) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.add(level, key, blk)
}

func (c *cache) dropBlock(key uint64) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.del(key)
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
	mu      sync.Mutex
	hotCap  int
	hotList *list.List
	hotData map[uint64]*list.Element
	cold    *clockCache
}

type blockEntry struct {
	key  uint64
	data *block
}

func newBlockCache(hotCap, coldCap int, metrics *cacheMetrics) *blockCache {
	bc := &blockCache{
		hotCap:  hotCap,
		hotList: list.New(),
		hotData: make(map[uint64]*list.Element, hotCap),
	}
	if coldCap > 0 {
		bc.cold = newClockCache(coldCap)
	}
	return bc
}

func (c *blockCache) get(level int, key uint64) (*block, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if elem, ok := c.hotData[key]; ok {
		c.hotList.MoveToFront(elem)
		return elem.Value.(*blockEntry).data, true
	}
	if c.cold != nil {
		if blk, ok := c.cold.get(key); ok {
			c.promoteLocked(key, blk)
			return blk, true
		}
	}
	return nil, false
}

func (c *blockCache) add(level int, key uint64, blk *block) {
	if c == nil || blk == nil {
		return
	}
	if level > 1 {
		// Only track blocks for L0/L1 as requested.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.promoteLocked(key, blk)
}

func (c *blockCache) del(key uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(key)
}

func (c *blockCache) promoteLocked(key uint64, blk *block) {
	if c.hotCap == 0 {
		if c.cold != nil {
			c.cold.add(key, blk)
		}
		return
	}
	if elem, ok := c.hotData[key]; ok {
		elem.Value.(*blockEntry).data = blk
		c.hotList.MoveToFront(elem)
		return
	}
	if c.hotList.Len() >= c.hotCap {
		tail := c.hotList.Back()
		if tail != nil {
			old := tail.Value.(*blockEntry)
			delete(c.hotData, old.key)
			c.hotList.Remove(tail)
			if c.cold != nil {
				c.cold.add(old.key, old.data)
			}
		}
	}
	elem := c.hotList.PushFront(&blockEntry{key: key, data: blk})
	c.hotData[key] = elem
}

func (c *blockCache) removeLocked(key uint64) {
	if elem, ok := c.hotData[key]; ok {
		delete(c.hotData, key)
		c.hotList.Remove(elem)
		return
	}
	if c.cold != nil {
		c.cold.del(key)
	}
}

type clockCache struct {
	capacity int
	entries  []clockEntry
	index    map[uint64]int
	hand     int
}

type clockEntry struct {
	key   uint64
	value *block
	ref   bool
	valid bool
}

func newClockCache(capacity int) *clockCache {
	if capacity <= 0 {
		return nil
	}
	return &clockCache{
		capacity: capacity,
		entries:  make([]clockEntry, capacity),
		index:    make(map[uint64]int, capacity),
	}
}

func (c *clockCache) get(key uint64) (*block, bool) {
	if c == nil {
		return nil, false
	}
	if idx, ok := c.index[key]; ok {
		entry := &c.entries[idx]
		if entry.valid {
			entry.ref = true
			return entry.value, true
		}
		delete(c.index, key)
	}
	return nil, false
}

func (c *clockCache) add(key uint64, value *block) {
	if c == nil || c.capacity == 0 || value == nil {
		return
	}
	if idx, ok := c.index[key]; ok {
		entry := &c.entries[idx]
		entry.value = value
		entry.ref = true
		entry.valid = true
		return
	}
	for {
		entry := &c.entries[c.hand]
		if !entry.valid {
			entry.key = key
			entry.value = value
			entry.ref = true
			entry.valid = true
			c.index[key] = c.hand
			c.advance()
			return
		}
		if entry.ref {
			entry.ref = false
			c.advance()
			continue
		}
		delete(c.index, entry.key)
		entry.key = key
		entry.value = value
		entry.ref = true
		entry.valid = true
		c.index[key] = c.hand
		c.advance()
		return
	}
}

func (c *clockCache) advance() {
	c.hand++
	if c.hand >= c.capacity {
		c.hand = 0
	}
}

func (c *clockCache) del(key uint64) {
	if c == nil {
		return
	}
	if idx, ok := c.index[key]; ok {
		entry := &c.entries[idx]
		entry.valid = false
		entry.ref = false
		entry.value = nil
		delete(c.index, key)
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
	dup := utils.SafeCopy(nil, filter)
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