// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package cache owns the LSM block + index cache layer. It is engine-neutral:
// callers pass opaque block payloads (Block) and a TableRef abstraction that
// only exposes the ref-counting hooks the cache needs to release a block on
// eviction. The lsm package wires its concrete *table into TableRef and its
// *block into Block at the boundary.
//
// Lifecycle:
//
//	AddBlock retains the table via IncrRef. The cache holds the reference for
//	as long as the entry lives in the bucket; on eviction (ristretto OnEvict)
//	or Close the cache calls DecrRef exactly once. Callers reading via
//	GetBlock get a borrowed handle whose payload is valid for the duration of
//	the entry's residency; persistence beyond that requires copying.
package cache

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

// TableRef is the minimal contract a table must satisfy to be cached: ref-
// counting only. The cache never reads the table's data or interprets its
// shape — it just holds one reference per cached block and releases on
// eviction.
type TableRef interface {
	IncrRef()
	DecrRef() error
}

// Block is the opaque payload the cache stores. Compression is left as a
// uint8 tag; the caller (table reader) interprets the value when decoding.
type Block struct {
	DiskData    []byte
	Compression uint32
	RawLen      int
}

// Entry is the cached block plus the table reference it pins.
type Entry struct {
	Key         uint64
	Tbl         TableRef
	DiskData    []byte
	Compression uint32
	RawLen      int

	cost        int64
	releaseOnce sync.Once
}

func (e *Entry) release() {
	if e == nil || e.Tbl == nil {
		return
	}
	e.releaseOnce.Do(func() {
		_ = e.Tbl.DecrRef()
	})
}

// Options configures the cache budgets. Either may be zero to disable that
// half of the cache.
type Options struct {
	IndexBytes int64
	BlockBytes int64
}

// Cache combines an index cache (typed *storagepb.TableIndex) and a block
// cache (opaque Block payload).
type Cache struct {
	indexes *coreCache.Cache
	blocks  *blockCache
	metrics *metrics.CacheCounters
}

// New constructs a Cache from the configured budgets.
func New(opt Options) *Cache {
	counters := metrics.NewCacheCounters()
	return &Cache{
		indexes: newIndexCache(opt.IndexBytes),
		blocks:  newBlockCache(opt.BlockBytes),
		metrics: counters,
	}
}

// Close releases cache state.
func (c *Cache) Close() error {
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

// AddIndex records the table index so subsequent reads can avoid re-decoding.
func (c *Cache) AddIndex(fid uint64, idx *storagepb.TableIndex) {
	if c == nil || c.indexes == nil || idx == nil {
		return
	}
	c.indexes.Set(fid, idx)
}

// GetIndex returns the cached table index when present.
func (c *Cache) GetIndex(fid uint64) (*storagepb.TableIndex, bool) {
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

// DelIndex evicts the index entry for the given table id.
func (c *Cache) DelIndex(fid uint64) {
	if c == nil || c.indexes == nil {
		return
	}
	c.indexes.Del(fid)
}

// GetBlock returns the cached block entry when present. The level is recorded
// for hit/miss metrics only.
func (c *Cache) GetBlock(level int, key uint64) (*Entry, bool) {
	if c == nil || c.blocks == nil {
		return nil, false
	}
	entry, ok := c.blocks.get(key)
	if ok {
		c.metrics.RecordBlock(level, true)
		return entry, true
	}
	c.metrics.RecordBlock(level, false)
	return nil, false
}

// AddBlock inserts a block into the cache. The cache pins tbl via IncrRef and
// releases it on eviction or Close.
func (c *Cache) AddBlock(level int, tbl TableRef, key uint64, blk Block) {
	if c == nil || c.blocks == nil {
		return
	}
	c.blocks.add(level, tbl, key, blk)
}

// Wait blocks until pending block-cache writes are visible. This is exposed
// primarily for tests that insert blocks and immediately read them; ristretto
// admits asynchronously, so without this barrier reads may legitimately miss.
func (c *Cache) Wait() {
	if c == nil || c.blocks == nil {
		return
	}
	for i := range c.blocks.shards {
		if c.blocks.shards[i].rc != nil {
			c.blocks.shards[i].rc.Wait()
		}
	}
}

// MetricsSnapshot returns a point-in-time copy of cache hit/miss counters.
func (c *Cache) MetricsSnapshot() metrics.CacheSnapshot {
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
	rc          *ristretto.Cache[uint64, *Entry]
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
		rc, err := ristretto.NewCache(&ristretto.Config[uint64, *Entry]{
			NumCounters: cacheCountersForBudget(budget, defaultBlockCacheAdmissionSize),
			MaxCost:     budget,
			BufferItems: 64,
			Cost: func(entry *Entry) int64 {
				if entry == nil || entry.cost <= 0 {
					return 1
				}
				return entry.cost
			},
			OnEvict: func(item *ristretto.Item[*Entry]) {
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

func (c *blockCache) get(key uint64) (*Entry, bool) {
	shard := c.shard(key)
	if shard == nil || shard.rc == nil {
		return nil, false
	}
	if be, ok := shard.rc.Get(key); ok && be != nil && len(be.DiskData) > 0 {
		return be, true
	}
	return nil, false
}

func (c *blockCache) add(level int, tbl TableRef, key uint64, blk Block) {
	shard := c.shard(key)
	if shard == nil || shard.rc == nil {
		return
	}
	if level > 1 {
		return
	}
	payload := blk.DiskData
	if len(payload) == 0 {
		return
	}
	cost := int64(len(payload))
	if cost <= 0 || cost > shard.budgetBytes {
		return
	}
	entry := &Entry{
		Key:         key,
		Tbl:         tbl,
		DiskData:    append([]byte(nil), payload...),
		Compression: blk.Compression,
		RawLen:      blk.RawLen,
		cost:        cost,
	}
	if entry.Tbl != nil {
		entry.Tbl.IncrRef()
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
