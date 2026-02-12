package lsm

import (
	"container/list"
	"sync"
	"sync/atomic"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/metrics"
	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	coreCache "github.com/feichai0017/NoKV/utils/cache"
)

const defaultCacheSize = 1024

// CacheMetrics is an alias for the shared cache metrics snapshot type.
type CacheMetrics = metrics.CacheSnapshot

type cache struct {
	indexs  *coreCache.Cache // key: fid, value: *pb.TableIndex
	blocks  *blockCache
	blooms  *bloomCache
	metrics *metrics.CacheCounters
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
	if opt == nil {
		opt = &Options{}
	}
	counters := metrics.NewCacheCounters()
	blockCacheSize := opt.BlockCacheSize
	bloomCacheSize := opt.BloomCacheSize
	hotCap := max(blockCacheSize, 0)
	blocks := newBlockCache(hotCap)
	blooms := newBloomCache(bloomCacheSize)
	return &cache{
		indexs:  coreCache.NewCache(defaultCacheSize),
		blocks:  blocks,
		blooms:  blooms,
		metrics: counters,
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
		c.metrics.RecordIndex(ok)
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
	blk, ok := c.blocks.get(key)
	if ok {
		c.metrics.RecordBlock(level, true)
		return blk, true
	}
	c.metrics.RecordBlock(level, false)
	return nil, false
}

func (c *cache) addBlock(level int, tbl *table, key uint64, blk *block) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.add(level, tbl, key, blk)
}

func (c *cache) getBloom(fid uint64) (utils.Filter, bool) {
	if c == nil || c.blooms == nil {
		return nil, false
	}
	filter, ok := c.blooms.get(fid)
	c.metrics.RecordBloom(ok)
	return filter, ok
}

func (c *cache) addBloom(fid uint64, filter utils.Filter) {
	if c == nil || c.blooms == nil {
		return
	}
	c.blooms.add(fid, filter)
}

func (c *cache) metricsSnapshot() metrics.CacheSnapshot {
	if c == nil {
		return metrics.CacheSnapshot{}
	}
	return c.metrics.Snapshot()
}

type blockCache struct {
	rc *ristretto.Cache[uint64, *blockEntry]

	closing atomic.Bool
}

type blockEntry struct {
	key uint64
	tbl *table
	blk *block

	releaseOnce sync.Once
}

func (be *blockEntry) release() {
	if be == nil || be.tbl == nil {
		return
	}
	be.releaseOnce.Do(func() {
		_ = be.tbl.DecrRef()
	})
}

func newBlockCache(capacity int) *blockCache {
	if capacity <= 0 {
		return nil
	}
	bc := &blockCache{}
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
			if bc.closing.Load() {
				return
			}
			item.Value.release()
		},
	})
	if err != nil {
		return nil
	}
	bc.rc = rc
	return bc
}

func (c *blockCache) get(key uint64) (*block, bool) {
	if c == nil || c.rc == nil {
		return nil, false
	}
	if be, ok := c.rc.Get(key); ok && be != nil && be.blk != nil {
		return be.blk, true
	}
	return nil, false
}

func (c *blockCache) add(level int, tbl *table, key uint64, blk *block) {
	if c == nil || c.rc == nil || blk == nil {
		return
	}
	if level > 1 {
		return
	}
	entry := &blockEntry{
		key: key,
		tbl: tbl,
		blk: blk,
	}
	if entry.tbl != nil {
		entry.tbl.IncrRef()
	}
	_ = c.rc.Set(key, entry, 1)
}

func (c *blockCache) close() {
	if c == nil {
		return
	}
	c.closing.Store(true)
	if c.rc != nil {
		c.rc.Close()
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
	bc := &bloomCache{
		cap:   capacity,
		items: make(map[uint64]*list.Element, capacity),
		lru:   list.New(),
	}
	return bc
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
