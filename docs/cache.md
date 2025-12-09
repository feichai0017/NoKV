# Cache & Bloom Filters

NoKV's LSM tier layers a multi-level block cache with bloom filter caching to accelerate lookups. The implementation is in [`lsm/cache.go`](../lsm/cache.go).

---

## 1. Components

| Component | Purpose | Source |
| --- | --- | --- |
| `cache.indexs` | Table index cache (`fid` → `*pb.TableIndex`) reused across reopen. | [`coreCache.Cache`](../utils/cache) wrapper |
| `blockCache` | Two-tier block cache (hot LRU + cold CLOCK). | [`lsm/cache.go#L168-L266`](../lsm/cache.go#L168-L266) |
| `bloomCache` | LRU cache of bloom filter bitsets per SST. | [`lsm/cache.go#L296-L356`](../lsm/cache.go#L296-L356) |
| `cacheMetrics` | Atomic hit/miss counters for L0/L1 blocks and blooms. | [`lsm/cache.go#L30-L110`](../lsm/cache.go#L30-L110) |

Badger uses a similar block cache split (`Pinner`/`Cache`) while RocksDB exposes block cache(s) via the `BlockBasedTableOptions`. NoKV keeps it Go-native and GC-friendly.

---

### 1.1 Index Cache & Handles

* SSTable metadata stays with the `table` struct, while decoded protobuf indexes are stored in `cache.indexs`. Lookups first hit the cache before falling back to disk.
* SST handles are reopened on demand for lower levels. L0/L1 tables keep their file descriptors pinned, while deeper levels close them once no iterator is using the table.

---

## 2. Block Cache Strategy

```text
Hot tier (LRU)  -> user-space cache for latency-critical blocks (L0/L1)
Cold tier       -> handled by OS page cache (no user-space cold CLOCK)
```

* `Options.BlockCacheSize` sets the user-space cache capacity (in blocks); all容量都是热层，冷数据直接依赖 OS page cache。
* **Hot tier** – Doubly linked list managed with `container/list`. Promotion happens on every hit; eviction simply drops into the OS cache instead of a user-space冷层。
* 锁策略：读路径 `RLock` 探测，命中短暂升级写锁更新 LRU；写入/淘汰仍用写锁。L0/L1 读都会记录命中/未命中指标。

```mermaid
flowchart LR
    Read --> CheckHot
    CheckHot -->|hit| Return
    CheckHot -->|miss| LoadFromTable (OS cache handles cold)
    LoadFromTable --> InsertHot
    InsertHot --> Return
```

By default only L0 and L1 blocks are cached (`level > 1` short-circuits), reflecting the higher re-use for top levels.

---

## 3. Bloom Cache

* `bloomCache` stores the raw filter bitset (`utils.Filter`) per table ID. Entries are deep-copied (`SafeCopy`) to avoid sharing memory with mmaps.
* LRU eviction ensures the newest filters stay resident; older ones are dropped to keep memory bounded (`Options.BloomCacheSize`).
* Bloom hits/misses are recorded via `cacheMetrics.recordBloom`, feeding into `StatsSnapshot.BloomHitRate`.

---

## 4. Metrics & Observability

`cache.metricsSnapshot()` produces:

```go
type CacheMetrics struct {
    L0Hits, L0Misses uint64
    L1Hits, L1Misses uint64
    BloomHits, BloomMisses uint64
    IndexHits, IndexMisses uint64
}
```

`Stats.Snapshot` converts these into hit rates. Monitor them alongside the block cache sizes to decide when to scale memory.

---

## 5. Interaction with Value Log

* Keys stored as value pointers (large values) still populate block cache entries for the key/index block. The value payload is read directly from the vlog (`valueLog.read`), so block cache hit rates remain meaningful.
* Discard stats from flushes can demote cached blocks via `cache.dropBlock`, ensuring obsolete SST data leaves the cache quickly.

---

## 6. Comparison

| Feature | RocksDB | BadgerDB | NoKV |
| --- | --- | --- | --- |
| Hot/cold tiers | Configurable multiple caches | Single cache | Built-in hot LRU + cold CLOCK |
| Bloom cache | Enabled per table, no explicit cache | Optional | Dedicated LRU storing filters |
| Metrics | Block cache stats via `GetAggregatedIntProperty` | Limited | `NoKV.Stats.Cache.*` hit rates |

---

## 7. Operational Tips

* 如果 bloom 命中率低于 60%，考虑提高每 key 的 bloom 位数或增大 Bloom cache。
* 用 `nokv stats --json` 跟踪缓存命中率趋势；回归通常指向迭代器使用不当或工作集变化。

More on SST layout lives in [`docs/manifest.md`](manifest.md) and [`docs/architecture.md`](architecture.md#4-read-path--iterators).
