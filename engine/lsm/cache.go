package lsm

import (
	"math/bits"
	"runtime"
	"sync"

	"github.com/dgraph-io/ristretto/v2"

	"github.com/feichai0017/NoKV/metrics"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	coreCache "github.com/feichai0017/NoKV/utils/cache"
	"google.golang.org/protobuf/proto"
)

const (
	defaultBlockCacheAdmissionSize int64 = 4 << 10
	defaultIndexCacheAdmissionSize int64 = 64 << 10
	minCacheCounters                     = 64
)

// CacheMetrics is an alias for the shared cache metrics snapshot type.
type CacheMetrics = metrics.CacheSnapshot

type cache struct {
	indexes *coreCache.Cache
	blocks  *blockCache
	metrics *metrics.CacheCounters
}

// close releases cache state.
func (c *cache) close() error {
	if c == nil {
		return nil
	}
	if c.indexes != nil {
		c.indexes = nil
	}
	if c.blocks != nil {
		c.blocks.close()
		c.blocks = nil
	}
	c.metrics = nil
	return nil
}

func newCache(opt *Options) *cache {
	if opt == nil {
		opt = &Options{}
	}
	counters := metrics.NewCacheCounters()
	return &cache{
		indexes: newIndexCache(opt.IndexCacheBytes),
		blocks:  newBlockCache(opt.BlockCacheBytes),
		metrics: counters,
	}
}

func (c *cache) addIndex(fid uint64, idx *storagepb.TableIndex) {
	if c == nil || c.indexes == nil || idx == nil {
		return
	}
	c.indexes.Set(fid, idx)
}

func (c *cache) getIndex(fid uint64) (*storagepb.TableIndex, bool) {
	if c == nil || c.indexes == nil {
		return nil, false
	}
	val, ok := c.indexes.Get(fid)
	if c.metrics != nil {
		c.metrics.RecordIndex(ok)
	}
	if !ok {
		return nil, false
	}
	index, ok := val.(*storagepb.TableIndex)
	if !ok || index == nil {
		return nil, false
	}
	return index, true
}

func (c *cache) delIndex(fid uint64) {
	if c == nil || c.indexes == nil {
		return
	}
	c.indexes.Del(fid)
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

func (c *cache) metricsSnapshot() metrics.CacheSnapshot {
	if c == nil || c.metrics == nil {
		return metrics.CacheSnapshot{}
	}
	return c.metrics.Snapshot()
}

func newIndexCache(budgetBytes int64) *coreCache.Cache {
	if budgetBytes <= 0 {
		return nil
	}
	estimatedItems := int(max(budgetBytes/defaultIndexCacheAdmissionSize, minCacheCounters))
	return coreCache.NewWeightedCache(budgetBytes, estimatedItems, func(value any) int64 {
		idx, _ := value.(*storagepb.TableIndex)
		return indexCacheCost(idx)
	})
}

type blockCache struct {
	budgetBytes int64
	shards      []blockCacheShard
	mask        uint64
}

type blockCacheShard struct {
	budgetBytes int64
	rc          *ristretto.Cache[uint64, *blockEntry]
}

type blockEntry struct {
	key uint64
	tbl *table
	blk *block

	cost        int64
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

func newBlockCache(budgetBytes int64) *blockCache {
	if budgetBytes <= 0 {
		return nil
	}
	shards := blockCacheShardCount()
	if budgetBytes < int64(shards) {
		shards = 1
	}
	bc := &blockCache{
		budgetBytes: budgetBytes,
		shards:      make([]blockCacheShard, shards),
		mask:        uint64(shards - 1),
	}
	perShard := budgetBytes / int64(shards)
	remainder := budgetBytes % int64(shards)
	for i := range bc.shards {
		budget := perShard
		if int64(i) < remainder {
			budget++
		}
		rc, err := ristretto.NewCache(&ristretto.Config[uint64, *blockEntry]{
			NumCounters: cacheCountersForBudget(budget, defaultBlockCacheAdmissionSize),
			MaxCost:     budget,
			BufferItems: 64,
			Cost: func(entry *blockEntry) int64 {
				if entry == nil || entry.cost <= 0 {
					return 1
				}
				return entry.cost
			},
			OnEvict: func(item *ristretto.Item[*blockEntry]) {
				if item == nil || item.Value == nil {
					return
				}
				item.Value.release()
			},
		})
		if err != nil {
			bc.close()
			return nil
		}
		bc.shards[i] = blockCacheShard{budgetBytes: budget, rc: rc}
	}
	return bc
}

func (c *blockCache) get(key uint64) (*block, bool) {
	shard := c.shard(key)
	if shard == nil || shard.rc == nil {
		return nil, false
	}
	if be, ok := shard.rc.Get(key); ok && be != nil && be.blk != nil {
		return be.blk, true
	}
	return nil, false
}

func (c *blockCache) add(level int, tbl *table, key uint64, blk *block) {
	shard := c.shard(key)
	if shard == nil || shard.rc == nil || blk == nil {
		return
	}
	if level > 1 {
		return
	}
	cost := blockCacheCost(blk)
	if cost <= 0 || cost > shard.budgetBytes {
		return
	}
	entry := &blockEntry{
		key:  key,
		tbl:  tbl,
		blk:  blk,
		cost: cost,
	}
	if entry.tbl != nil {
		entry.tbl.IncrRef()
	}
	if accepted := shard.rc.Set(key, entry, cost); !accepted {
		entry.release()
	}
}

func (c *blockCache) close() {
	if c == nil {
		return
	}
	for i := range c.shards {
		if c.shards[i].rc != nil {
			c.shards[i].rc.Close()
			c.shards[i].rc = nil
		}
	}
}

func (c *blockCache) wait() {
	if c == nil {
		return
	}
	for i := range c.shards {
		if c.shards[i].rc != nil {
			c.shards[i].rc.Wait()
		}
	}
}

func (c *blockCache) shard(key uint64) *blockCacheShard {
	if c == nil || len(c.shards) == 0 {
		return nil
	}
	const mix = 11400714819323198485
	idx := (key * mix) & c.mask
	return &c.shards[idx]
}

func blockCacheShardCount() int {
	n := min(max(runtime.GOMAXPROCS(0), 1), 32)
	if n&(n-1) != 0 {
		n = min(1<<bits.Len(uint(n)), 32)
	}
	return n
}

func indexCacheCost(idx *storagepb.TableIndex) int64 {
	if idx == nil {
		return 0
	}
	cost := int64(proto.Size(idx))
	if cost <= 0 {
		return 1
	}
	return cost
}

func blockCacheCost(blk *block) int64 {
	if blk == nil {
		return 0
	}
	if blk.estimateSz > 0 {
		return blk.estimateSz
	}
	if cap(blk.data) > 0 {
		return int64(cap(blk.data))
	}
	if len(blk.data) > 0 {
		return int64(len(blk.data))
	}
	return 1
}

func cacheCountersForBudget(budgetBytes, avgItemBytes int64) int64 {
	if budgetBytes <= 0 {
		return minCacheCounters
	}
	if avgItemBytes <= 0 {
		avgItemBytes = 1
	}
	items := max(budgetBytes/avgItemBytes, minCacheCounters)
	return items * 8
}
