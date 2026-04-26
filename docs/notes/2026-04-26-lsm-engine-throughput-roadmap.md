# 2026-04-26 LSM 引擎吞吐与性能优化路线图

> 本文合并了原 `2026-04-26-lsm-engine-throughput-roadmap.md`（综合路线图）和
> `2026-04-26-lsm-data-plane-sharding-design.md`（数据面分片设计稿）两份笔记。
> Sharding 部分（原属于路线图 §3.1）已经按设计稿落地为 commits
> `eeeee1f0` (Phase 1+2) 和 `5a6ec5ef` (Phase 3+4)，本文相应地把它从"待办"
> 移到"已完成 + 实现细节"，剩余路线图保持原状。

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
| External SST ingestion | `engine/lsm/external_sst.go` | bulk load 快路径 |
| KV separation (vlog) | `engine/vlog/` | WiscKey-style，bucket-sharded |
| Pipelined Write | `db.go` | MPSC commitQueue + dispatcher + N processor |
| **Sharded LSM Data Plane** | `engine/lsm/shard.go`、`db.go`、`wal-XX/` 目录 | **N=4 个独立 WAL Manager + memtable + commit processor，per-key affinity 路由 + commit burst coalesce** |
| **跨 shard MVCC 读** | `lsm.Get` walks all shards, picks max-version | sub-µs ART lookup × 4 shards |

### 2.2 主要剩余缺口

| 维度 | 缺口 |
|---|---|
| Compaction | 单 worker 跑整个 keyrange，无 subcompactions |
| Cache | 未 shard，未压缩缓存；filter 受 LRU 影响 |
| Filter | 无 Ribbon、无 negative cache |
| SST 格式 | 单层 index，restart interval 固定 |
| IO | syscall 模型，无 io_uring / vectored |
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

### 3.5 Subcompactions —— 待办

**问题**：一次 compaction 单 worker 跑整个 keyrange。大 compaction
可能 30s+，期间 backlog 累积。

**Pebble 设计**：把 compaction input 按 keyrange 切成 N 段，N 个
worker 并行写 N 个输出 SST。

**收益**：compaction 完成时间 -70%，前台 latency 抖动减小。

**复杂度**：中。compaction executor 改为 fan-out / fan-in 结构。

---

### 3.6 Adaptive L0 Slowdown —— 待办

**问题**：`L0SlowdownWritesTrigger` 固定数字。

**设计**：滑动窗口监测 `L0 增长速度` vs `compaction 完成速度`，
自适应退避。

**收益**：burst 写不被无意义降速；p99 写延迟更稳。

---

## 4. 读路径优化

### 4.1 Compressed Block Cache —— 待办

**问题**：BlockCache 缓存解压后的 block。压缩比 3-5× 时，cache
容量等价被压缩。

**Pebble 设计**：cache 中存压缩后的 block，hit 时按需解压。

**收益**：磁盘读 -50~70%，p99 读延迟显著下降。

---

### 4.2 Filter Block 独立 Cache + 永驻 —— 待办

filter blocks 用独立 cache，标 `Pinned` 优先级，不参与普通 LRU。

---

### 4.3 Ribbon Filter 替代 Bloom —— 待办

Bloom 1% FPR 需 ~10 bits/key；Ribbon (Facebook 2021) 1% FPR 需 ~7 bits/key，
filter 占用 -30%。

---

### 4.4 Negative Cache —— 待办

维护 known-absent keys cache（容量 1k-10k）。命中即返回 not found。
高 read miss 场景 ×N 加速。必须在写路径正确失效。

---

### 4.5 Multi-Level Iterator Pinning —— 待办

iterator 持有 block 引用直到 seek 到下一 block，长 scan 的 cache 抖动消除。

---

### 4.6 跨 shard memtable Get hint cache —— 新增（Sharding 副产品）

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

### 5.2 TTL Compaction —— 待办

按 SST oldest-write-time，超过 TTL 强制 compact。fsmeta 收益尤其大
（retired snapshot / unlinked dentry 自动 GC）。

---

### 5.3 Auto-tuning Compaction Concurrency —— 待办

按 write QPS / L0 backlog 动态调整 worker 数。

---

## 6. SST 格式优化

### 6.1 Block Restart Interval 自适应 —— 待办

短 key (<32B) → interval 4 或 8；长 key → 16-32。fsmeta 元数据 key 短，
seek path binary search depth 下降。

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
| L0 sublevels read path | ✅ |
| Commit burst coalesce | ✅ |
| per-key affinity routing | ✅ |

### Tier 1 — 短期最高 ROI

| # | 优化 | 周 | 主要收益 |
|---|---|---|---|
| 4.1 | Compressed Block Cache | 1 | 容量等价 ×3-5 |
| 7.1 | io_uring | 3 | 写侧 syscall.write -50% |
| 4.6 | Cross-shard memtable hint | 0.5 | 读路径 -10~17% |
| 3.6 | Adaptive L0 Slowdown | 1 | burst 写 p99 改善 |

**3-4 周做完后预期**：
- 写吞吐 N=4 c=128：725K → 1M+
- 读 cache 命中率 +30-50%
- 读 p99 -30%

### Tier 2 — 结构性收益

| # | 优化 | 周 |
|---|---|---|
| 3.5 | Subcompactions | 2 |
| 4.3 | Ribbon Filter | 1 |
| 4.2 | Filter 永驻 | 0.5 |
| 5.1 | Hybrid Tiered+Leveled | 2 |

### Tier 3 — 特化场景

| # | 优化 | 收益场景 |
|---|---|---|
| 4.4 | Negative Cache | 高 miss 率 |
| 5.2 | TTL Compaction | snapshot / 长寿命数据 |
| 6.3 | mmap SST | read-heavy 元数据 |
| 7.3 | Direct IO | 内存紧张部署 |
| 6.1 | 自适应 restart interval | 短 key workload |
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
| Subcompactions | ✅ | ⚠️ | ✅ | ✅ | ❌ | ✅ |
| Compressed Cache | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| Ribbon Filter | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| Hybrid Tiered+Leveled | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ |
| Two-Level Index | ✅ | ✅ | ✅ | ⚠️ | ❌ | ✅ (Tier3) |
| io_uring | ⚠️ | ⚠️ | ✅ | ✅ | (planned) | ✅ |
| mmap SST | ⚠️ | ⚠️ | ❌ | ✅ | ❌ | ✅ (可选) |
| Shard-per-Core | ❌ | ❌ | ❌ | ✅ | ❌ | (Tier4) |

**结论**：sharded WAL+memtable + commit pipeline 已经把 NoKV 推到了 Go
生态里少有的位置（Pebble/Badger 都还是单 WAL）；做完 Tier 1+2，单机
引擎成熟度和 RocksDB / Pebble 同代，部分维度（unified WAL、sharded
data plane、Ribbon、io_uring）可能更现代。

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
| Tier 1 完成（cross-shard hint）| ~100 µs |
| Tier 1+2 完成（compressed cache + Ribbon）| ~50 µs |

---

## 12. 风险与权衡

| 优化 | 风险 / 权衡 |
|---|---|
| Sharded LSM (已落地) | 同 (key, version) 在多 shard 下需 tiebreaker —— 已用 per-key affinity 路由解决 |
| Pipelined Write (已落地) | 错误回传 + crash 一致性需仔细设计 —— 已通过 syncQueue 分桶 |
| Subcompactions | 实现复杂；fan-out/fan-in 错误处理 |
| Hybrid Tiered+Leveled | read amp 略升，需 Filter 永驻配合 |
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

> **写路径**：sharded WAL+memtable+commit pipeline ✅；下一波是 io_uring +
> Subcompactions。
> **读路径**：cross-shard hint cache + compressed block cache + Ribbon。
> **结构**：Hybrid Tiered+Leveled。
> **远期**：Shard-per-Core 让性能从亚线性扩展变线性扩展。
>
> 目前 N=4 c=128 = 725K ops/s（YCSB-A，benchmark-tuned），相对 175K
> baseline 翻 4×。Tier 1 完成预计推到 1M+。
