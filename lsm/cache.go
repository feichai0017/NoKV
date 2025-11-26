package lsm

import (
	"container/list"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	coreCache "github.com/feichai0017/NoKV/utils/cache"
	"github.com/feichai0017/NoKV/vmcache"
)

const defaultCacheSize = 1024

type blockLookup struct {
	tbl    *table
	offset int
	length int
}

func (l *blockLookup) Release() {
	if l == nil || l.tbl == nil {
		return
	}
	_ = l.tbl.DecrRef()
	l.tbl = nil
}

type blockLocator func(id uint64) (*blockLookup, error)

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

func newCache(opt *Options, locator blockLocator) *cache {
	metrics := &cacheMetrics{}
	hotCap := max(opt.BlockCacheSize, 0)
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
	var cold coldBackend
	if coldSize > 0 && locator != nil {
		pageSize := opt.VMCachePageSize
		if pageSize <= 0 {
			pageSize = opt.BlockSize
		}
		evictBatch := opt.VMCacheEvictBatch
		var err error
		cold, err = newVMColdCache(opt, pageSize, coldSize, evictBatch, locator)
		if err != nil {
			utils.Err(err)
		}
	}
	blocks := newBlockCache(hotSize, cold, locator)
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

func (c *cache) getBlock(level int, key uint64) (*block, bool) {
	if c == nil || c.blocks == nil {
		return nil, false
	}
	blk, hit, err := c.blocks.get(key)
	if err != nil || blk == nil {
		c.metrics.recordBlock(level, false)
		return nil, false
	}
	c.metrics.recordBlock(level, hit)
	return blk, true
}

func (c *cache) addBlock(level int, key uint64, blk *block) {
	c.addBlockWithTier(level, key, blk, true)
}

func (c *cache) addBlockWithTier(level int, key uint64, blk *block, hot bool) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.addWithTier(level, key, blk, hot)
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
	cold    coldBackend
	locate  blockLocator
}

type blockEntry struct {
	key    uint64
	data   *block
	handle *vmcache.Handle
}

type coldBackend interface {
	Fix(id uint64) (*vmcache.Handle, bool, error)
	Release(id uint64, dirty bool)
	Evict(ids []uint64)
	Prefetch(ids []uint64)
	Close() error
}

func newBlockCache(hotCap int, cold coldBackend, locate blockLocator) *blockCache {
	bc := &blockCache{
		hotCap:  hotCap,
		hotList: list.New(),
		hotData: make(map[uint64]*list.Element, hotCap),
		cold:    cold,
		locate:  locate,
	}
	return bc
}

func newVMColdCache(opt *Options, pageSize, capacity, evictBatch int, locator blockLocator) (coldBackend, error) {
	loader := func(id uint64, dst []byte) (int, error) {
		loc, err := locator(id)
		if err != nil {
			return 0, err
		}
		if loc == nil || loc.tbl == nil {
			return 0, fmt.Errorf("vmcache loader: table missing for id %d", id)
		}
		defer loc.Release()
		if loc.length > len(dst) {
			return 0, fmt.Errorf("vmcache loader: block length %d exceeds page size %d", loc.length, len(dst))
		}
		return loc.tbl.copyBlockData(int(id&maxUint32), dst)
	}
	cache, err := vmcache.New(vmcache.Options{
		PageSize:       pageSize,
		Capacity:       capacity,
		EvictBatch:     evictBatch,
		UseMmap:        opt.VMCacheUseMmap,
		DisableMadvise: opt.VMCacheDisableMadvise,
	}, loader)
	if err != nil {
		return nil, err
	}
	return cache, nil
}

func (c *blockCache) get(key uint64) (*block, bool, error) {
	if c == nil {
		return nil, false, nil
	}
	c.mu.Lock()
	if elem, ok := c.hotData[key]; ok {
		c.hotList.MoveToFront(elem)
		blk := elem.Value.(*blockEntry).data
		c.mu.Unlock()
		return blk, true, nil
	}
	c.mu.Unlock()

	if c.cold == nil {
		return nil, false, nil
	}
	handle, hit, err := c.cold.Fix(key)
	if err != nil || handle == nil {
		return nil, false, err
	}
	blk, err := c.buildBlockFromHandle(key, handle)
	if err != nil {
		handle.Release(false)
		return nil, false, err
	}

	c.mu.Lock()
	c.promoteLocked(key, blk)
	c.mu.Unlock()
	return blk, hit, nil
}

func (c *blockCache) buildBlockFromHandle(key uint64, handle *vmcache.Handle) (*block, error) {
	offset := 0
	if c.locate != nil {
		if loc, err := c.locate(key); err == nil && loc != nil {
			offset = loc.offset
			loc.Release()
		}
	}
	blk, err := decodeBlock(handle.Bytes(), offset)
	if err != nil {
		return nil, err
	}
	blk.vmHandle = handle
	runtime.SetFinalizer(blk, (*block).releaseHandle)
	return blk, nil
}

func (c *blockCache) add(level int, key uint64, blk *block) {
	if c == nil {
		return
	}
	c.addWithTier(level, key, blk, true)
}

func (c *blockCache) addWithTier(level int, key uint64, blk *block, hot bool) {
	if c == nil || blk == nil {
		return
	}
	if level > 1 {
		// Only track blocks for L0/L1 as requested.
		return
	}
	if hot || c.hotCap == 0 {
		c.mu.Lock()
		c.promoteLocked(key, blk)
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	if elem, ok := c.hotData[key]; ok {
		entry := elem.Value.(*blockEntry)
		entry.data = blk
		entry.handle = blk.vmHandle
		c.hotList.MoveToFront(elem)
		c.mu.Unlock()
		return
	}
	c.promoteLocked(key, blk)
	c.mu.Unlock()
}

func (c *blockCache) del(key uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	c.removeLocked(key)
	c.mu.Unlock()
	if c.cold != nil {
		c.cold.Evict([]uint64{key})
	}
}

func (c *blockCache) promoteLocked(key uint64, blk *block) {
	if c.hotCap == 0 {
		return
	}
	if elem, ok := c.hotData[key]; ok {
		entry := elem.Value.(*blockEntry)
		if entry.data != blk {
			entry.data.releaseHandle()
			entry.data = blk
			entry.handle = blk.vmHandle
		}
		c.hotList.MoveToFront(elem)
		return
	}
	if c.hotList.Len() >= c.hotCap {
		tail := c.hotList.Back()
		if tail != nil {
			old := tail.Value.(*blockEntry)
			old.data.releaseHandle()
			delete(c.hotData, old.key)
			c.hotList.Remove(tail)
		}
	}
	elem := c.hotList.PushFront(&blockEntry{key: key, data: blk, handle: blk.vmHandle})
	c.hotData[key] = elem
}

func (c *blockCache) removeLocked(key uint64) {
	if elem, ok := c.hotData[key]; ok {
		entry := elem.Value.(*blockEntry)
		if entry != nil {
			entry.data.releaseHandle()
		}
		delete(c.hotData, key)
		c.hotList.Remove(elem)
		return
	}
}

func (c *blockCache) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	for _, elem := range c.hotData {
		if elem == nil {
			continue
		}
		if entry, ok := elem.Value.(*blockEntry); ok && entry != nil {
			entry.data.releaseHandle()
		}
	}
	c.hotData = nil
	c.hotList = nil
	c.mu.Unlock()
	if c.cold != nil {
		_ = c.cold.Close()
		c.cold = nil
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

func (c *clockCache) clear() {
	if c == nil {
		return
	}
	c.entries = nil
	c.index = nil
	c.capacity = 0
	c.hand = 0
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
