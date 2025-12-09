package lsm

import (
	"container/list"
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"

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
	c.addBlockWithTier(level, tbl, key, blk, true)
}

func (c *cache) addBlockWithTier(level int, tbl *table, key uint64, blk *block, hot bool) {
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
	shards []*blockCacheShard
	mask   uint64
}

type blockCacheShard struct {
	mu      sync.RWMutex
	cap     int
	hotList *list.List
	table   *cacheTable
}

type blockEntry struct {
	key  uint64
	idx  int
	tbl  *table
	data *block
}

func newBlockCache(capacity int) *blockCache {
	shardCount := pickBlockCacheShards(capacity)
	shards := make([]*blockCacheShard, shardCount)

	capDist := spreadCapacity(capacity, shardCount)
	for i := range shards {
		shards[i] = newBlockCacheShard(capDist[i])
	}

	mask := uint64(0)
	if isPowerOfTwo(shardCount) {
		mask = uint64(shardCount - 1)
	}
	return &blockCache{shards: shards, mask: mask}
}

func pickBlockCacheShards(capacity int) int {
	if capacity <= 0 {
		return 1
	}
	shards := min(min(max(runtime.GOMAXPROCS(0), 4), 16), capacity)
	if !isPowerOfTwo(shards) {
		shards = 1 << bits.Len(uint(shards))
	}
	if shards == 0 {
		shards = 1
	}
	return shards
}

func isPowerOfTwo(n int) bool {
	return n > 0 && (n&(n-1)) == 0
}

func spreadCapacity(total, parts int) []int {
	out := make([]int, parts)
	if parts <= 0 || total <= 0 {
		return out
	}
	base := total / parts
	rem := total % parts
	for i := range parts {
		out[i] = base
		if rem > 0 && i < rem {
			out[i]++
		}
	}
	return out
}

func blockIndexFromKey(key uint64) int {
	return int(uint32(key))
}

func (c *blockCache) shard(key uint64) *blockCacheShard {
	if c == nil || len(c.shards) == 0 {
		return nil
	}
	fid := key >> 32
	if c.mask > 0 {
		return c.shards[int(fid&c.mask)]
	}
	return c.shards[int(fid%uint64(len(c.shards)))]
}

func newBlockCacheShard(capacity int) *blockCacheShard {
	return &blockCacheShard{
		cap:     capacity,
		hotList: list.New(),
		table:   newCacheTable(capacity),
	}
}

func (c *blockCache) get(level int, tbl *table, key uint64) (*block, bool) {
	shard := c.shard(key)
	if shard == nil {
		return nil, false
	}
	return shard.get(level, tbl, key)
}

func (c *blockCache) add(level int, tbl *table, key uint64, blk *block) {
	if c == nil {
		return
	}
	c.addWithTier(level, tbl, key, blk, true)
}

func (c *blockCache) addWithTier(level int, tbl *table, key uint64, blk *block, hot bool) {
	shard := c.shard(key)
	if shard == nil {
		return
	}
	shard.addWithTier(level, tbl, key, blk, hot)
}

func (c *blockCache) del(tbl *table, key uint64) {
	shard := c.shard(key)
	if shard == nil {
		return
	}
	shard.del(tbl, key)
}

func (c *blockCache) close() {
	if c == nil {
		return
	}
	for _, shard := range c.shards {
		shard.close()
	}
	c.shards = nil
}

func (c *blockCacheShard) get(level int, tbl *table, key uint64) (*block, bool) {
	if c == nil {
		return nil, false
	}
	c.mu.RLock()
	idx := blockIndexFromKey(key)
	elem, ok := c.lookupEntry(tbl, key, idx)
	c.mu.RUnlock()
	if ok {
		return c.sliceFromEntry(elem)
	}
	return nil, false
}

func (c *blockCacheShard) add(level int, tbl *table, key uint64, blk *block) {
	if c == nil {
		return
	}
	c.addWithTier(level, tbl, key, blk, true)
}

func (c *blockCacheShard) addWithTier(level int, tbl *table, key uint64, blk *block, _ bool) {
	if c == nil || blk == nil {
		return
	}
	if level > 1 {
		// Only track blocks for L0/L1 as requested.
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.promoteLocked(tbl, key, blk)
}

func (c *blockCacheShard) del(tbl *table, key uint64) {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.removeLocked(tbl, key)
}

func (c *blockCacheShard) promoteLocked(tbl *table, key uint64, blk *block) {
	if c.cap == 0 {
		return
	}
	idx := blockIndexFromKey(key)
	// Replace existing entry in place.
	if elem, ok := c.lookupEntry(tbl, key, idx); ok {
		if v := elem.Value.(*blockEntry); v != nil {
			v.idx = idx
			v.tbl = blk.tbl
			if blk.tbl == nil {
				v.data = blk
			} else {
				v.data = nil
			}
		}
		c.hotList.MoveToFront(elem)
		return
	}

	if c.hotList.Len() >= c.cap {
		tail := c.hotList.Back()
		if tail != nil {
			old := tail.Value.(*blockEntry)
			c.deleteEntry(old.tbl, old.key, blockIndexFromKey(old.key))
			c.hotList.Remove(tail)
		}
	}
	entry := &blockEntry{key: key, idx: idx, tbl: blk.tbl}
	if blk.tbl == nil {
		entry.data = blk
	}
	elem := c.hotList.PushFront(entry)
	c.storeEntry(tbl, key, idx, elem)
}

func (c *blockCacheShard) removeLocked(tbl *table, key uint64) {
	idx := blockIndexFromKey(key)
	if elem, ok := c.lookupEntry(tbl, key, idx); ok {
		c.deleteEntry(tbl, key, idx)
		c.hotList.Remove(elem)
	}
}

func (c *blockCacheShard) close() {
	if c == nil {
		return
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.table = nil
	c.hotList = nil
}

func (c *blockCacheShard) sliceFromEntry(elem *list.Element) (*block, bool) {
	if elem == nil {
		return nil, false
	}
	be, ok := elem.Value.(*blockEntry)
	if !ok || be == nil {
		return nil, false
	}
	if be.data != nil {
		return be.data, true
	}
	if be.tbl == nil {
		return nil, false
	}
	// Reload block on demand using table/mmap to avoid storing full block data.
	blk, err := be.tbl.loadBlock(be.idx, true, false, true)
	if err != nil {
		return nil, false
	}
	return blk, true
}

func (c *blockCacheShard) lookupEntry(tbl *table, key uint64, idx int) (*list.Element, bool) {
	if tbl != nil {
		if idx < len(tbl.cacheSlots) {
			if elem := tbl.cacheSlots[idx]; elem != nil {
				return elem, true
			}
		}
		return nil, false
	}
	return c.table.get(key)
}

func (c *blockCacheShard) storeEntry(tbl *table, key uint64, idx int, elem *list.Element) {
	if tbl != nil {
		if idx >= len(tbl.cacheSlots) {
			needed := idx + 1
			grown := make([]*list.Element, needed)
			copy(grown, tbl.cacheSlots)
			tbl.cacheSlots = grown
		}
		tbl.cacheSlots[idx] = elem
		return
	}
	c.table.set(key, elem)
}

func (c *blockCacheShard) deleteEntry(tbl *table, key uint64, idx int) {
	if tbl != nil {
		if idx < len(tbl.cacheSlots) {
			tbl.cacheSlots[idx] = nil
		}
		return
	}
	c.table.del(key)
}

// cacheTable is a simple open-addressed hash table mapping key -> list element.
type cacheTable struct {
	slots []cacheSlot
	mask  uint64
}

type cacheSlot struct {
	key   uint64
	val   *list.Element
	state slotState
}

type slotState uint8

const (
	slotEmpty slotState = iota
	slotFull
	slotTombstone
)

func newCacheTable(capacity int) *cacheTable {
	size := nextPow2(capacity * 2)
	if size == 0 {
		size = 1
	}
	return &cacheTable{
		slots: make([]cacheSlot, size),
		mask:  uint64(size - 1),
	}
}

func nextPow2(n int) int {
	if n <= 0 {
		return 0
	}
	p := 1 << bits.Len(uint(n-1))
	if p == 0 {
		p = 1
	}
	return p
}

func (t *cacheTable) get(key uint64) (*list.Element, bool) {
	if t == nil || len(t.slots) == 0 {
		return nil, false
	}
	for idx, probes := t.index(key); probes < len(t.slots); idx, probes = t.next(idx, probes) {
		slot := &t.slots[idx]
		switch slot.state {
		case slotEmpty:
			return nil, false
		case slotFull:
			if slot.key == key {
				return slot.val, true
			}
		}
	}
	return nil, false
}

func (t *cacheTable) set(key uint64, val *list.Element) {
	if t == nil || len(t.slots) == 0 {
		return
	}
	firstTomb := -1
	for idx, probes := t.index(key); probes < len(t.slots); idx, probes = t.next(idx, probes) {
		slot := &t.slots[idx]
		switch slot.state {
		case slotEmpty:
			if firstTomb >= 0 {
				slot = &t.slots[firstTomb]
			}
			slot.key = key
			slot.val = val
			slot.state = slotFull
			return
		case slotFull:
			if slot.key == key {
				slot.val = val
				return
			}
		case slotTombstone:
			if firstTomb < 0 {
				firstTomb = idx
			}
		}
	}
}

func (t *cacheTable) del(key uint64) {
	if t == nil || len(t.slots) == 0 {
		return
	}
	for idx, probes := t.index(key); probes < len(t.slots); idx, probes = t.next(idx, probes) {
		slot := &t.slots[idx]
		switch slot.state {
		case slotEmpty:
			return
		case slotFull:
			if slot.key == key {
				slot.state = slotTombstone
				slot.key = 0
				slot.val = nil
				return
			}
		}
	}
}

func (t *cacheTable) index(key uint64) (idx int, probes int) {
	if t.mask > 0 {
		return int(key & t.mask), 0
	}
	return int(key % uint64(len(t.slots))), 0
}

func (t *cacheTable) next(idx, probes int) (int, int) {
	return (idx + 1) % len(t.slots), probes + 1
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
