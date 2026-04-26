# 2026-04-26 LSM 引擎吞吐与性能优化路线图

> 本文合并了原 `2026-04-26-lsm-engine-throughput-roadmap.md`（综合路线图）和
> `2026-04-26-lsm-data-plane-sharding-design.md`（数据面分片设计稿）两份笔记。
> 二次审计后又把若干已经落地但仍写在"待办"的项目搬到"已完成"：
> Compressed Block Cache、Subcompactions、Adaptive L0 Slowdown、
> Auto-tuning Compaction Concurrency、Prefix Bloom、Adaptive Iterator
> Prefetch、IteratorPool。代码出处全部已在小节内注明。

---

## 1. 范围与目标

回答两个问题：

1. NoKV 当前 LSM 引擎在哪些路径上**还有显著吞吐 / 延迟空间**？
2. 哪些工业界已经验证的优化**值得直接借鉴**？

不讨论：

- WAL 子系统（已分阶段落地：Durability / Retention / Backpressure /
  Group Commit / Catalog / Sharded Manager / 数据面 4-shard 分片）
- fsmeta 应用层特化（Prefix Bloom / DeleteRange / Per-Kind tuning）
- 控制面（Eunomia 协议）

---

## 2. 现状盘点

### 2.1 已具备的能力（截至 2026-04-26）

| 能力 | 位置 | 备注 |
|---|---|---|
| ART memtable | `engine/lsm/memtable.go` | 前缀友好，对元数据 workload 天然合适 |
| Range tombstones | `engine/lsm/compaction_executor.go` | 跨层保留，max level 不丢 |
| Bloom filter | `engine/lsm/cache.go` | 可配 false-positive rate |
| Block cache + Index cache | `engine/lsm/cache.go` | 双 cache 分离 |
| Compaction picker / executor | `engine/lsm/picker.go` / `compaction_executor.go` | 标准 LSM |
| L0 sublevels (Phase A+B) | `engine/lsm/l0_sublevels.go` | 读路径 + trivial move |
| L0 slowdown / stop trigger | `engine/lsm/options.go` | 写背压 |
| **Adaptive L0 throttle** | `lsm.throttlePressure` permille + `WriteThrottleState` 三档 | 滑动 backlog → slowdown / stop / resume |
| External SST ingestion | `engine/lsm/external_sst.go` | bulk load 快路径 |
| KV separation (vlog) | `engine/vlog/` | WiscKey-style，bucket-sharded |
| Pipelined Write | `db.go` | MPSC commitQueue + dispatcher + N processor |
| **Sharded LSM Data Plane** | `engine/lsm/shard.go`、`db.go`、`wal-XX/` 目录 | **N=4 个独立 WAL Manager + memtable + commit processor，per-key affinity 路由 + commit burst coalesce** |
| **跨 shard MVCC 读** | `lsm.Get` walks all shards, picks max-version | sub-µs ART lookup × 4 shards |
| **Subcompactions** | `engine/lsm/compaction_executor.go:484/572` `levelManager.subcompact` | 按 splits 并行写出，`utils.Throttle(8+len(splits))` 控并发 |
| **Auto-tuning Compactor 并发** | `compaction_executor.go:67` "adaptive bump: more backlog → more shards" | backlog 大就拉 shard 数 |
| **Compressed Block Cache** | `engine/lsm/cache.go` `blockEntry.diskData/compression` + `table.decodeCachedBlock` | cache 存压缩字节，hit 时按需解压；BlockCache 已 shard |
| **Prefix Bloom (SST)** | `tableIndex.PrefixBloomFilter` + `table.prefixBloomMiss` | metadata workload 直接命中 |
| **Adaptive iterator prefetch** | `engine/index/iterator.go` `PrefetchAdaptive` | 动态调 prefetch 深度 |
| **IteratorPool** | `internal/runtime/iterator_pool.go` | iterator context 池化复用 |

### 2.2 主要剩余缺口

| 维度 | 缺口 |
|---|---|
| Filter | 无 Ribbon；filter 当前内联在 decoded TableIndex / IndexCache 内，不存在独立 filter block eviction 问题 |
| SST 格式 | 单层 index；当前 block 内是完整 entry offset 表，没有 Pebble-style restart interval |
| 读路径 | 跨 shard memtable Get hint 已落地；高 miss 场景由 negative cache 兜底 |
| Compaction 结构 | hybrid scheduler 已有；缺口是更激进的 L0 tiered layout 与 SST 大索引结构 |
| IO | syscall 模型，无 io_uring / vectored / O_DIRECT |
| 架构 | shared levels/manifest（未 shard-per-core） |

---

## 3. 写吞吐优化

### 3.1 ✅ Sharded LSM Data Plane —— 已落地

> Commits: `eeeee1f0` (Phase 1+2)、`5a6ec5ef` (Phase 3+4)

**问题**：所有 write 进单 memtable 单 WAL Manager。`wal.Manager.mu`
profile 上占 16% lock2 + 19% pthread_cond_wait + 19% syscall。
吞吐 ceiling 即此架构。

**已实现的设计**：

```
                    DB.commitDispatcher (1 个 goroutine, owns MPSC consumer)
                                  │
                                  │ batch
                                  ▼
                       routing: hash(firstKey) & (N-1)
                                  │
              ┌───────────────────┼───────────────────┐
              ▼                   ▼                   ▼
       commitProcessor[0]   commitProcessor[1]   commitProcessor[N-1]
              │                   │                   │
              │ shardID=0         │ shardID=1         │ shardID=N-1
              ▼                   ▼                   ▼
        ┌──────────┐         ┌──────────┐         ┌──────────┐
        │ lsmShard │         │ lsmShard │         │ lsmShard │
        │  ──────  │         │  ──────  │         │  ──────  │
        │  wal     │         │  wal     │         │  wal     │
        │  active  │         │  active  │         │  active  │
        │  immut[] │         │  immut[] │         │  immut[] │
        └──────────┘         └──────────┘         └──────────┘
              │                   │                   │
              └───────────────────┴───────────────────┘
                                  │ flushed SSTs
                                  ▼
                      Shared L0..LN (L0 sublevels)
```

**Manager 数量**：4 raft + 4 LSM data = 8（无独立 control-plane Manager；
原来的 `db.wal` 已解散进 4 个 LSM shard）。每 Manager ≈ 4 MiB bufio +
1 fd + 2 goroutines。

**路由策略**：commit-worker affinity，按 batch 第一条 entry 的
user-key 做 fnv1a32 hash 然后 `& (N-1)`。同 key 永远同 shard，
确保 percolator 的 lock-on/lock-off（同 startTS 同 key）落在同一
memtable，保留 last-write-wins 语义。整个 batch 不会跨 shard，
SetBatch 原子性保留。

**SetBatch 原子性不变量**：
- 一个 commit request 只活在一个 batch 里。
- 一个 batch 端到端只交给一个 commit processor。
- 一个 commit processor 与一个 shard 一一绑定。
- → 一个 commit request 只写一个 WAL shard 和一个 memtable shard。

**读路径**：`Get(key)` 走遍所有 shard 的 memtable + immutables，
按 MVCC version 取最大；未命中再下沉共享 levels。`Iterator` 是
N memtable 源 + N immutable 源 + L0..LN merge。N=4 下 ART memtable
sub-µs，O(N) 增量微小。

**Recovery**：每个 shard 独立 replay 自己的 WAL（`<workdir>/lsm-wal-XX/`）。
全局 manifest logPointer 不再用作 retention 高水位（多 shard 下不准）；
每个 shard 在内存里维护自己的 `highestFlushedSeg.atomic.Uint32`，
flush 完同步推进，retention callback 用本 shard 的水位。WAL 段在
flush 完成时 inline 删除；recovery 重放磁盘上残留段，MVCC 让重复
apply 幂等。

**Flush**：每个 shard 自己 rotate；共享 flushQueue，N 个 flush worker。
SST 进共享 L0 + sublevels。

**Range tombstones**：DELETE_RANGE 走单 batch → 落单 shard memtable；
读路径已经 O(N) walk all shards，无须改动。

**Commit pipeline 实现细节**：
- MPSC `commitQueue` 用户线程 lock-free 入队（`utils.MPSCQueue`）。
- 单 dispatcher goroutine consumer，hash 路由。
- per-shard buffered channel cap=32（生产者写满即 block，给 processor
  追赶时间；尝试过 SPSC 替换实测 30-40% 慢，channel 在 cap=32 下已
  amortize 调度开销）。
- 每 commit processor 用 burst coalesce：drain 通道里所有 ready batch，
  merge 成一次 vlog.write + lsm.SetBatchGroup。failedAt 还原回每 batch
  的语义。
- burst==1 走 fast path（`runSingleCommit`），跳过 merge bookkeeping。

**实测性能（YCSB-A 50/50 R-W, value=1KB, 500K records / 500K ops）**：

| 阶段 | YCSB-A QPS |
|---|---|
| Pre-shard baseline | ~175K |
| Phase 1 优化（pipelined write 等）| ~400K |
| Sharded data plane (N=4, conc=128) | **725K** (benchmark-tuned) / ~605K (production-default) |

p99 延迟 N=4 c=128 = 491µs，比 N=2 (732µs) 稳。

---

### 3.2 Pipelined Write —— 已落地

> 已合入主干，实现见 `db.go` `commitProcessor` / `runCommitBurst`。

caller 拿 ack（memtable 已插入），不等 fsync；fsync 走 syncQueue
后台 worker。

---

### 3.3 Parallel Memtable Flush —— 已落地

> 配合 §3.1 一并完成：N 个 flush worker (= shardCount)。

burst 写场景下避免 write stall。

---

### 3.4 Trivial Move —— 已落地

> 见 commits `e07f97d9` (Phase A: 非重叠表 move without rewrite)
> 和 `d84a59d7` (Phase B: L0 sublevel-aware overlap check)。

Pebble 风格：L0 SST 与 L1 不重叠时直接 rename。fsmeta workload 命中率高。

---

### 3.5 Subcompactions —— ✅ 已落地

> 实现：`engine/lsm/compaction_executor.go:484` `subcompact` +
> `utils.Throttle(8+len(splits))` 控并发。compaction 按 KeyRange splits
> 切片，N 个 worker 并行写 N 个 SST。

---

### 3.6 Adaptive L0 Slowdown —— ✅ 已落地

> 实现：`lsm.throttleWrites` 把 `WriteThrottleState`（None / Slowdown
> / Stop）和 `throttlePressure` (permille [0,1000]) + `throttleRate`
> (bytes/sec) 一起暴露给 DB 写入侧。`compaction_executor.go:67` 的
> "adaptive bump: more backlog => allow more shards" 把 compaction
> shard 数也按 backlog 自适应。L0 slowdown / stop / resume 三档触发器
> 在 `options.go:99-104` + `applyLSMSharedOptions` 里默认值化。

---

## 4. 读路径优化

### 4.1 Compressed Block Cache —— ✅ 已落地

> 实现：`engine/lsm/cache.go` `blockEntry { diskData, compression,
> rawLen }` 直接存磁盘字节；`engine/lsm/table.go:425`
> `decodeCachedBlock` 在 cache hit 时按需调用 `decodeBlockPayload`
> 解压。BlockCache 已 shard（`blockCacheShardCount`），命中走 ristretto
> 子缓存。覆盖测试 `engine/lsm/cache_test.go:92`
> `TestBlockCacheStoresCompressedPayload`。

---

### 4.2 Filter Block 独立 Cache + 永驻 —— 暂不单独做

当前 SST 格式把 bloom / prefix bloom 内联在 decoded `TableIndex` 里，
`TableIndex` 走独立 `IndexCache`，不和 data block 竞争 BlockCache。
因此没有 Pebble-style filter block eviction 问题。只有在后续引入
Two-Level Index / 独立 filter block layout 时，才需要单独的 pinned
filter cache。

---

### 4.3 Ribbon Filter 替代 Bloom —— 待办

Bloom 1% FPR 需 ~10 bits/key；Ribbon (Facebook 2021) 1% FPR 需 ~7 bits/key，
filter 占用 -30%。

---

### 4.4 Negative Cache —— ✅ 已落地

> 实现：`engine/lsm/negative_cache.go` + `lsm.Get` 入口 fast path。

维护 bounded exact negative cache。缓存 key 是完整 internal key，写路径按
base key 推进 generation，使所有 read-version miss 同时失效。存在 range
tombstone 时 conservative disable，避免复杂范围失效；external SST ingest
成功后清空 cache，避免导入表被旧 miss 短路。

---

### 4.5 Multi-Level Iterator Pinning —— ⚠️ 部分落地

> 已有：`engine/index/iterator.go` `PrefetchAdaptive` 字段动态调
> prefetch 深度；`internal/runtime/iterator_pool.go` `IteratorPool` 复用
> per-iterator context；block 在 cache 里有 refcount + `release()`，长
> scan 内单 block 不会被淘汰。
>
> 还缺：跨 SST/level 切换时显式 pin 当前 block，到下一个 block 才
> release。Pebble 在长 prefix scan 上更激进。

---

### 4.6 跨 shard memtable Get hint cache —— ✅ 已落地（Sharding 副产品）

**问题**：cross-shard 读路径每个 Get 走遍 N 个 memtable。当前
profile 显示 LSM Get cum 17% CPU，N=8 时会扩大。

**设计**：维护 (key → 最近写入 shardID) 的小型 hint cache。Get 先查
hint shard，命中即返回；miss 再 fall back 全 shard walk。

**收益**：消除多 shard 读放大。

---

## 5. Compaction 结构优化

### 5.1 Hybrid Tiered + Leveled Compaction —— 待办

L0 tiered（写放大 1×）+ L1↓ leveled（read amp 低）。配合 4.1+4.2 缓解
read amp 上升。

---

### 5.2 TTL Compaction —— ✅ 已落地

> 实现：`Options.TTLCompactionMinAge` + `PlanForMaxLevel(..., ttlMinAge)`。

max level 表只要存在 stale bytes 且年龄超过 `TTLCompactionMinAge`，
即使 stale bytes 未达到 10MiB size threshold，也会触发 rewrite。
Zero disables age-triggered cleanup，保留原 size-triggered 行为。

---

### 5.3 Auto-tuning Compaction Concurrency —— ✅ 已落地

> `compaction_executor.go:67` 的 "adaptive bump: more backlog => allow
> more shards, capped by shard count" 实时按 backlog 调 inflight
> compaction worker 数。

---

## 6. SST 格式优化

### 6.1 Block Restart Interval 自适应 —— 暂不适用

NoKV 当前 block format 存完整 `entryOffsets` 表，seek 直接在 offset
数组上定位，再 decode entry；没有 Pebble-style restart interval 这个
调参面。短 key 优化已经由 prefix compression + Prefix Bloom + compressed
cache 承担。除非后续重写 block format，否则不单独实现 restart interval。

### 6.2 Two-Level Index + Index Compression —— 待办

Top-level index 永驻 + data-level 按需读 + prefix compression。大 SST
的 index 内存占用 -80%。

### 6.3 Direct mmap of Immutable SSTs —— 待办

immutable SST 直接 mmap，OS page cache 自动管理。两种模式都支持。

---

## 7. IO 层优化

### 7.1 io_uring 替代 syscall —— 待办

NoKV 已有 VFS 抽象。落地 io_uring 后单 enter syscall 提交 N 个 IO。

> **当前 profile 上 syscall.write 占 N=4 c=128 总 CPU 32%**，是写侧
> 第一大瓶颈，io_uring 是直接对症的优化。

### 7.2 Vectored IO (readv/writev) —— 待办

scan 时一次 `readv` 读 N 个相邻 block，syscall 数下降。

### 7.3 Direct IO 模式（可选）—— 待办

提供 `O_DIRECT` 选项 bypass OS page cache，memory 占用减半，性能更可预测。

---

## 8. 并发架构（远期）

### 8.1 Shard-per-Core 架构（ScyllaDB 路线）

**当前**：worker pool + shared levels/manifest。
**ScyllaDB 设计**：每个 CPU core 独占一个 shard，无共享，message passing。

**收益**：性能线性扩展到核数，1M → 5M+ ops/s。

**代价**：整个引擎大重构，**远期愿景**。

---

## 9. 优先级排序与落地周期（更新）

### Tier 0 — 已落地

| 项 | 状态 |
|---|---|
| Sharded LSM Data Plane (§3.1) | ✅ 4-shard，YCSB-A 725K |
| Pipelined Write (§3.2) | ✅ MPSC + dispatcher + N processor |
| Parallel Memtable Flush (§3.3) | ✅ N flush workers |
| Trivial Move (§3.4) | ✅ Phase A+B |
| **Subcompactions (§3.5)** | ✅ `levelManager.subcompact` + Throttle |
| **Adaptive L0 Slowdown (§3.6)** | ✅ throttlePressure permille + 三档 state |
| **Compressed Block Cache (§4.1)** | ✅ `blockEntry.diskData/compression` + `decodeCachedBlock` |
| **Auto-tuning Compactor (§5.3)** | ✅ adaptive shard bump by backlog |
| **Prefix Bloom (SST)** | ✅ `tableIndex.PrefixBloomFilter` |
| **Adaptive iterator prefetch** | ✅ `index.PrefetchAdaptive` |
| **IteratorPool** | ✅ `dbruntime.IteratorPool` |
| L0 sublevels read path | ✅ |
| Commit burst coalesce | ✅ |
| per-key affinity routing | ✅ |
| Cross-shard memtable hint (§4.6) | ✅ |
| Negative Cache (§4.4) | ✅ |
| TTL Compaction (§5.2) | ✅ |

### Tier 1 — 短期最高 ROI（更新）

| # | 优化 | 周 | 主要收益 |
|---|---|---|---|
| 7.1 | io_uring | 3 | 写侧 syscall.write -50% |
| 4.3 | Ribbon Filter | 1 | filter 占用 -30% |

**3-4 周做完后预期**：
- 写吞吐 N=4 c=128：725K → 1M+
- 读 cache 命中率 +30-50%
- 读 p99 -30%

### Tier 2 — 结构性收益

| # | 优化 | 周 |
|---|---|---|
| 5.1 | Hybrid Tiered+Leveled | 2 |
| 6.2 | Two-Level Index + Index Compression | 1.5 |
| 4.5 | 跨 SST iterator pin（完成 partial）| 1 |
| 3.5补 | Subcompaction 错误回滚 / 切片质量提升 | 1 |

### Tier 3 — 特化场景

| # | 优化 | 收益场景 |
|---|---|---|
| 6.3 | mmap SST | read-heavy 元数据 |
| 7.3 | Direct IO | 内存紧张部署 |
| 6.2 | Two-Level Index | 大 SST |

### Tier 4 — 远期

| # | 优化 | 备注 |
|---|---|---|
| 8.1 | Shard-per-Core | 数年规模重构 |

---

## 10. 工业界对比

| 优化 | RocksDB | Pebble | TiKV | ScyllaDB | NoKV 现状 | NoKV Tier1+2 后 |
|---|---|---|---|---|---|---|
| Pipelined Write | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Sharded Memtable | ⚠️ | ❌ | ✅ | ✅ | **✅ (N=4)** | ✅ |
| Sharded WAL | ❌ | ❌ | per-region | per-core | **✅ (N=4 LSM + 4 raft)** | ✅ |
| Trivial Move | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ |
| Subcompactions | ✅ | ⚠️ | ✅ | ✅ | **✅** | ✅ |
| Adaptive L0 Slowdown | ✅ | ✅ | ✅ | ✅ | **✅** | ✅ |
| Auto-tuning Compactor 并发 | ✅ | ⚠️ | ✅ | ✅ | **✅** | ✅ |
| Compressed Cache | ✅ | ✅ | ✅ | ✅ | **✅** | ✅ |
| Prefix Bloom | ✅ | ✅ | ✅ | ⚠️ | **✅** | ✅ |
| Ribbon Filter | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| Filter 独立永驻 cache | ✅ | ⚠️ | ✅ | ✅ | N/A（filter 内联 TableIndex） | N/A |
| Hybrid scheduler | ✅ | ❌ | ✅ | ✅ | ✅ | ✅ |
| Two-Level Index | ✅ | ✅ | ✅ | ⚠️ | ❌ | ✅ (Tier2) |
| io_uring | ⚠️ | ⚠️ | ✅ | ✅ | (planned) | ✅ |
| mmap SST | ⚠️ | ⚠️ | ❌ | ✅ | ❌ | ✅ (可选) |
| Shard-per-Core | ❌ | ❌ | ❌ | ✅ | ❌ | (Tier4) |

**结论**：当前能力对照 RocksDB / Pebble / TiKV / ScyllaDB 的核心列表，
NoKV 已落地 ✅ 13 项，剩余主要差距在 **Ribbon Filter /
Two-Level Index / io_uring / 更激进 L0 tiered layout**。Tier 1+2 推完，
单机引擎成熟度和 RocksDB / Pebble 同代，部分维度（unified sharded
WAL+memtable+commit pipeline、subcompactions+auto-tuning concurrency、
compressed block cache + prefix bloom）已经接近或更现代。

---

## 11. 量化预期

### 单机写吞吐（高并发，本机 macOS, ARM, NVMe）

| 阶段 | YCSB-A QPS |
|---|---|
| Pre-shard baseline | 175K |
| Phase 1（pipelined write 等已落地） | 400K |
| **Sharded data plane (当前)** | **725K** (benchmark-tuned) / 605K (production-default) |
| Tier 1 完成（io_uring 等）| 1M-1.5M |
| Tier 1+2 完成 | **1.5M-2.5M** |
| Tier 1+2+4 (Shard-per-Core) | 5M+（远期） |

### 启动时间（10GB WAL，已和 WAL Catalog 协同）

| 阶段 | RTO |
|---|---|
| WAL Catalog 完成（已落地） | ~10 sec |
| Sharded WAL（已落地，并行 replay） | ~5 sec |

### 读延迟 p99

| 阶段 | 元数据点查 p99 |
|---|---|
| Pre-shard baseline | ~300 µs |
| 当前（N=4, c=128） | 491 µs (写侧 contention 影响) / 单纯读 ~150 µs |
| Tier 1 已完成（cross-shard hint + negative cache）| ~100 µs |
| Tier 1+2 完成（compressed cache + Ribbon）| ~50 µs |

---

## 12. 风险与权衡

| 优化 | 风险 / 权衡 |
|---|---|
| Sharded LSM (已落地) | 同 (key, version) 在多 shard 下需 tiebreaker —— 已用 per-key affinity 路由解决 |
| Pipelined Write (已落地) | 错误回传 + crash 一致性需仔细设计 —— 已通过 syncQueue 分桶 |
| Subcompactions | 实现复杂；fan-out/fan-in 错误处理 |
| 更激进 L0 tiered layout | read amp 略升，需要依赖 Prefix Bloom / TableIndex 常驻来抵消 |
| mmap SST | 无法精确控制驱逐，OS 行为不一定可预测 |
| Direct IO | 失去 OS read-ahead 优势，scan 可能变慢 |
| Shard-per-Core | 数年工程，期间引擎双轨 |

---

## 13. Sharding 实现的决策日志（保留备查）

- **2026-04-26**: shard at LSM level, not at DB or coordinator level.
  Reason: the LSM is where the lock contention is, not above it.
- **2026-04-26**: shard wal AND memtable, not just wal. Reason:
  recovery and flush coordination is simpler with full sharding.
- **2026-04-26**: route by per-key affinity (hash by first user key)
  not round-robin. Reason: same-(key, version) writes (percolator
  lock-on/lock-off, fsmeta same-startTS) need to land in the same
  memtable so last-write-wins ordering is preserved. Whole-batch
  routing keeps SetBatch atomicity.
- **2026-04-26**: default N=4 for LSM, 4 for raft = 8 total Managers.
  The legacy single `db.wal` is dissolved into the 4 LSM shards;
  there is no separate control-plane Manager. Tunable via `LSMShardCount`.
- **2026-04-26**: per-shard `highestFlushedSeg` 替代全局 logPointer
  作为 retention 高水位。recovery 重放磁盘上残留段，MVCC 幂等。
- **2026-04-26**: per-shard buffered channel cap=32 + burst coalesce.
  曾尝试 utils.SPSCQueue 替换 channel：bench 上慢 30-40%，channel
  在 cap=32 下已经 amortize 调度开销，回退到 channel + 保留 SPSC
  作为 utils 通用 primitive。

---

## 14. 一句话总结

> **写路径**：sharded WAL+memtable+commit pipeline ✅、Subcompactions ✅、
> Adaptive L0 ✅；下一波主要是 io_uring。
> **读路径**：Compressed Block Cache ✅、Prefix Bloom ✅、Adaptive
> Iterator Prefetch ✅、IteratorPool ✅、cross-shard hint ✅、Negative
> Cache ✅；下一波是 Ribbon Filter + 跨 SST iterator pin。
> **结构**：TTL Compaction ✅；Two-Level Index / 更激进 L0 tiered layout 仍待办。
> **远期**：Shard-per-Core 让性能从亚线性扩展变线性扩展。
>
> 目前 N=4 c=128 = 725K ops/s（YCSB-A，benchmark-tuned），相对 175K
> baseline 翻 4×。Tier 1 完成预计推到 1M+。
