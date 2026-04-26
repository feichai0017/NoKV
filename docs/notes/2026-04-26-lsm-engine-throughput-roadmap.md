# 2026-04-26 LSM 内核引擎吞吐与性能优化路线图

> 状态：设计 / 路线图。本文系统列出 NoKV LSM 引擎在写吞吐、读延迟、compaction、SST 格式、IO、并发架构六个维度上可继续推进的优化项，并给出按 ROI 排序的落地优先级。
>
> 不包含 fsmeta 特化（见同目录 `2026-04-26-lsm-fsmeta-specialization.md` 如有），不包含 WAL 子系统（已在 `engine/wal` commit 系列推进）。
>
> 关联：
> - 现状：`engine/lsm/`、`engine/index/`、`engine/vlog/`
> - 工业对照：RocksDB / Pebble / TiKV raft-engine / ScyllaDB

---

## 1. 范围与目标

本文回答两个问题：

1. NoKV 当前 LSM 引擎在哪些路径上**还有显著吞吐 / 延迟空间**？
2. 哪些工业界已经验证的优化**值得直接借鉴**？

不讨论：
- WAL 子系统（已分阶段落地：Durability / Retention / Backpressure / Group Commit / Catalog / Sharded Manager）
- fsmeta 应用层特化（Prefix Bloom / DeleteRange / Per-Kind tuning）
- 控制面（Eunomia 协议）

---

## 2. 现状盘点

### 2.1 已具备的能力

| 能力 | 位置 | 备注 |
|---|---|---|
| ART memtable | `engine/lsm/memtable.go` | 前缀友好，对元数据 workload 天然合适 |
| Range tombstones | `engine/lsm/compaction_executor.go:498` | 跨层保留，max level 不丢 |
| Bloom filter | `engine/lsm/cache.go` | 可配 false-positive rate |
| Block cache + Index cache | `engine/lsm/cache.go` | 双 cache 分离 |
| Compaction picker / executor | `engine/lsm/picker.go` / `compaction_executor.go` | 标准 LSM |
| L0 slowdown trigger | `engine/lsm/options.go` | 基础写背压 |
| External SST ingestion | `engine/lsm/external_sst.go` | bulk load 快路径 |
| KV separation (vlog) | `engine/vlog/` | WiscKey-style |

### 2.2 主要缺口

| 维度 | 缺口 |
|---|---|
| 写并发 | memtable 单实例，写吞吐 ≈ 单核 |
| 写延迟 | 同步路径含 fsync，无 pipelined commit |
| Compaction | 单 worker 跑整个 compaction，无 subcompactions / trivial move |
| Cache | 未 shard，未压缩缓存；filter 受 LRU 影响 |
| Filter | 无 Ribbon、无 negative cache |
| SST 格式 | 单层 index，restart interval 固定 |
| IO | syscall 模型，无 io_uring / vectored |
| 架构 | shared-state + worker pool（未 shard-per-core） |

---

## 3. 写吞吐优化（6 项）

### 3.1 Sharded Memtable —— 写并发线性扩展

**问题**：所有 write 进单 memtable，吞吐 ≈ 单核。

**设计**：
```go
type ShardedMemtable struct {
    shards [N]*memTable          // N = NumCPU 或固定 16
    flushQueue                    // 共享 flush 调度
}

func (sm *ShardedMemtable) Put(key, value []byte) {
    sm.shards[hash(key) & (N-1)].Put(key, value)
}

// Get 路径：按 hash 路由直查目标 shard
// Iterator：N 路 merge iterator
```

**收益**：写吞吐 ×N（N = CPU 核数）。

**复杂度**：高。memtable flush 调度需要重写，iterator 需要新增 N-way merge 路径。

**前置依赖**：无。

---

### 3.2 Pipelined Write —— commit / fsync / apply 解耦

**问题**：`Set` 当前走 `[encode → wal append → fsync → memtable insert]` 串行；caller 等 fsync 完成才返回。

**设计**：参考 Pebble commit pipeline：
- Caller 拿到 "逻辑可见 ack"（memtable 已插入），不等 fsync
- WAL 的 `DurabilityFsyncBatched` 在后台 worker 推进
- 强一致 caller 显式选 `DurabilityFsync` 或在 ack 后 wait fsync watermark

**结合现有 Durability Policy**：write 接口加 `DurabilityPolicy` 参数，4 档行为已经在 WAL 层定义。

**收益**：写延迟 ~150µs → ~10-20µs；高并发下吞吐 ×5-10。

**复杂度**：中。要新增 commit pipeline goroutine + 错误回传机制。

---

### 3.3 Parallel Memtable Flush —— burst 写不再 stall

**问题**：单 flush worker。memtable full → switch → 后台 flush；下一个 memtable 在前一个 flush 完成前不能 switch，写堵塞。

**设计**：N 个 flush worker，能并行处理多个 immutable memtable。

**收益**：burst 写场景下避免 write stall。

**前置依赖**：3.1（Sharded Memtable）—— 否则只有一个 immutable memtable，并行 flush 无意义。

---

### 3.4 Trivial Move —— L0→L1 不读不写

**Pebble 设计**：当 L0 SST 的 keyrange 与 L1 完全不重叠，直接 rename 文件到 L1，零 IO。

**fsmeta workload 命中率高**：不同 mount / 不同 parent 的 dentry keyrange 自然不重叠。

**收益**：write amplification -30~50%。

**复杂度**：低。compaction picker 加一个 keyrange 重叠检查。

---

### 3.5 Subcompactions —— 单次 compaction 内部并行

**问题**：一次 compaction 单 worker 跑整个 keyrange。大 compaction 可能 30s+，期间 backlog 累积。

**Pebble 设计**：把 compaction input 按 keyrange 切成 N 段，N 个 worker 并行写 N 个输出 SST。

**收益**：compaction 完成时间 -70%，前台 latency 抖动减小。

**复杂度**：中。compaction executor 改为 fan-out / fan-in 结构。

---

### 3.6 Adaptive L0 Slowdown —— 替代固定阈值

**问题**：`L0SlowdownWritesTrigger` 固定数字。L0 SST 数到了就一刀切 throttle。

**设计**：滑动窗口监测 `L0 增长速度` vs `compaction 完成速度`：
- 跟得上 → 不 throttle
- 跟不上 → 线性退避（小幅）
- 严重落后 → 强力退避

**收益**：burst 写不被无意义降速；p99 写延迟更稳。

**复杂度**：低。picker 加观测 + 反馈循环。

---

## 4. 读路径优化（5 项）

### 4.1 Compressed Block Cache —— 容量等价放大 3-5×

**问题**：BlockCache 缓存解压后的 block。压缩比 3-5× 时，cache 容量等价被压缩。

**Pebble 设计**：cache 中存压缩后的 block，hit 时按需解压。CPU 多花 ~200ns/4KB，但 cache 命中率显著上升。

**fsmeta 收益尤其大**：元数据高度可压缩。

**收益**：磁盘读 -50~70%，p99 读延迟显著下降。

**前置依赖**：4.4（Block Compression）—— 没有压缩时 cache 直接存解压块即可。

---

### 4.2 Filter Block 独立 Cache + 永驻

**问题**：bloom filter 和 data block 共享 cache。filter 可能被 LRU 淘汰，导致下次 lookup 多一次 IO。

**设计**：filter blocks 用独立 cache，标 `Pinned` 优先级，不参与普通 LRU。

**收益**：bloom 命中率 100%，无效磁盘 IO 进一步消除。

**复杂度**：低。Pebble 有 `cache.Priority` 概念，可直接借鉴。

---

### 4.3 Ribbon Filter 替代 Bloom

**对比**：
- Bloom: 1% FPR 需要 ~10 bits/key
- Ribbon (Facebook 2021): 1% FPR 需要 ~7 bits/key

**收益**：filter 占用 -30% → cache 装更多 filter → 命中率上升。

**复杂度**：低-中。Filter 编解码器替换。RocksDB / TiKV 都已支持 Ribbon。

---

### 4.4 Negative Cache —— 缓存"不存在"

**问题**：lookup 不存在的 key，过 N 个 SST 的 bloom（false positive）后才返回 not found。下一次同 key 又重复整个流程。

**设计**：维护小型 known-absent keys cache（容量 1k-10k）。命中即返回 not found。

**收益**：高 read miss 场景 ×N 加速。

**注意**：必须在写路径正确失效。

---

### 4.5 Multi-Level Iterator Pinning

**问题**：iterator 跨层切换时可能 re-fetch 同一 block。

**设计**：iterator 持有 block 引用直到 seek 到下一 block。

**收益**：长 scan 的 cache 抖动消除。

---

## 5. Compaction 结构优化（3 项）

### 5.1 Hybrid Tiered + Leveled Compaction

**当前**：纯 leveled。写放大 ~10-15×。

**设计**：
- L0 用 tiered：写入只 append，不 merge → 写放大 1×
- L1 及以下用 leveled：保证 read amp 低

**收益**：write amplification 12 → 6-7。

**代价**：read amp 略升（L0 SST 数变多）。配合 4.1（Filter 永驻）可缓解。

---

### 5.2 TTL Compaction —— 自动 GC 过期数据

**问题**：旧版本 / tombstone 只在 size-triggered compaction 中清理。

**设计**：周期扫每个 SST 的 oldest-write-time，超过 TTL 强制 compact。

**fsmeta 收益**：retired snapshot / unlinked dentry 打 TTL，到期自动 GC。

---

### 5.3 Auto-tuning Compaction Concurrency

**当前**：`NumCompactors` 配置后固定。

**设计**：根据 write QPS / L0 backlog 动态调整 worker 数。低负载省 CPU，burst 立即拉满。

**收益**：稳态 CPU 利用率改善 + burst 应对能力提升。

---

## 6. SST 格式优化（3 项）

### 6.1 Block Restart Interval 自适应

**问题**：固定 restart interval（默认 16）。短 key（fsmeta ~20-40B）情况下 restart 之间字节数大，binary search 范围大。

**设计**：SST writer 观察平均 key 长度，调整 restart interval：
- 短 key (<32B) → interval 4 或 8
- 长 key → interval 16-32

**收益**：seek 路径 binary search depth 下降，点查 latency p99 改善。

---

### 6.2 Two-Level Index + Index Compression

**问题**：大 SST 的 index block 一次性 page 进 cache，浪费内存。

**Pebble 设计**：
- Top-level index 永驻（小，覆盖整个 SST 的"哪个 index block 管哪段 key"）
- Data-level index 按需读
- 都用 prefix compression

**收益**：大 SST 的 index 内存占用 -80%，cache 容量等价放大 5-10×。

---

### 6.3 Direct mmap of Immutable SSTs

**问题**：SST 块通过 read syscall 进 cache，syscall 开销 + double caching（OS page cache + BlockCache）。

**设计**：immutable SST 直接 mmap，OS page cache 自动管理。

**适合**：read-heavy + 元数据 workload（小文件多）。

**代价**：无法精确控制驱逐策略。建议**两种模式都支持**，按部署选用。

---

## 7. IO 层优化（3 项）

### 7.1 io_uring 替代 syscall

**NoKV 已有 VFS 抽象 + 路线图**。落地 io_uring 后：
- 单 enter syscall 提交 N 个 IO
- completion queue 异步回收
- fsync 可批量

**收益**：高并发 IO 场景 syscall 开销 -50~80%，fsync rate 上限提升。

---

### 7.2 Vectored IO (readv/writev)

**当前**：每次只读一个 block。

**设计**：scan 时一次 `readv` 读 N 个相邻 block。

**收益**：syscall 数下降，scan 吞吐提升。

---

### 7.3 Direct IO 模式（可选）

**当前**：默认走 OS page cache，read 后双重缓存。

**设计**：提供 `O_DIRECT` 选项，bypass OS page cache。

**收益**：memory 占用减半，性能更可预测。

**适合**：内存紧张 / 性能确定性高的部署。

---

## 8. 并发架构（远期 1 项）

### 8.1 Shard-per-Core 架构（ScyllaDB 路线）

**当前**：worker pool + shared state + locking。

**ScyllaDB 设计**：每个 CPU core 独占一个 shard，shard 之间无共享状态，跨 shard 通信走 message passing。无锁。

**收益**：性能线性扩展到核数（不是亚线性）。100k → 1M+ ops/s。

**代价**：整个引擎大重构。**作为远期愿景，不在短期 ROI 项内**。

---

## 9. 优先级排序与落地周期

### Tier 1 — 最高 ROI（4 周）

| # | 优化 | 周 | 主要收益 |
|---|---|---|---|
| 4.1 | Compressed Block Cache | 1 | 容量等价 ×3-5 |
| 3.1 | Sharded Memtable | 2 | 写吞吐 ×NumCPU |
| 3.2 | Pipelined Write | 1.5 | 写延迟 -90% |
| 3.4 | Trivial Move | 1 | 写放大 -30% |

**4 周做完后预期**：
- 写吞吐 5-10×
- 读 cache 命中率 +30-50%
- 写放大 -30%

### Tier 2 — 结构性收益（再 8 周）

| # | 优化 | 周 |
|---|---|---|
| 3.5 | Subcompactions | 2 |
| 4.3 | Ribbon Filter | 1 |
| 5.1 | Hybrid Tiered + Leveled | 2 |
| 7.1 | io_uring | 3 |

**累计 12 周后预期**：
- compaction 时间 -70%
- 长 lookup p99 -40%
- IO syscall 数 -50%

### Tier 3 — 特化场景

| # | 优化 | 收益场景 |
|---|---|---|
| 4.4 | Negative Cache | 高 miss 率 |
| 5.2 | TTL Compaction | snapshot / 长寿命数据 |
| 6.3 | mmap SST | read-heavy 元数据 |
| 7.3 | Direct IO | 内存紧张部署 |

### Tier 4 — 远期

| # | 优化 | 备注 |
|---|---|---|
| 8.1 | Shard-per-Core | 数年规模重构 |

---

## 10. 工业界对比

| 优化 | RocksDB | Pebble | TiKV | ScyllaDB | NoKV 现状 | NoKV Tier1+2 后 |
|---|---|---|---|---|---|---|
| Pipelined Write | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| Sharded Memtable | ⚠️ | ❌ | ✅ | ✅ | ❌ | ✅ |
| Trivial Move | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| Subcompactions | ✅ | ⚠️ | ✅ | ✅ | ❌ | ✅ |
| Compressed Cache | ✅ | ✅ | ✅ | ✅ | ❌ | ✅ |
| Ribbon Filter | ✅ | ❌ | ✅ | ❌ | ❌ | ✅ |
| Hybrid Tiered+Leveled | ✅ | ❌ | ✅ | ✅ | ❌ | ✅ |
| Two-Level Index | ✅ | ✅ | ✅ | ⚠️ | ❌ | ✅ (Tier3) |
| io_uring | ⚠️ | ⚠️ | ✅ | ✅ | (planned) | ✅ |
| mmap SST | ⚠️ | ⚠️ | ❌ | ✅ | ❌ | ✅ (可选) |
| Shard-per-Core | ❌ | ❌ | ❌ | ✅ | ❌ | (Tier4) |

**结论**：做完 Tier 1+2，单机引擎成熟度和 RocksDB / Pebble 同代，部分维度（unified WAL、Ribbon、io_uring）可能更现代。

---

## 11. 量化预期

### 单机写吞吐（高并发，8 vCPU + NVMe）

| 阶段 | YCSB-A QPS（estimate） |
|---|---|
| 当前（README）| 175K |
| Tier 1 完成 | 500K-800K |
| Tier 1+2 完成 | **800K-1.5M** |
| Tier 1+2+4 (Shard-per-Core) | 2M-5M（远期） |

### 启动时间（10GB WAL，已和 WAL Catalog 协同）

| 阶段 | RTO |
|---|---|
| WAL Catalog 完成（已落地） | ~10 sec |

### 读延迟 p99

| 阶段 | 元数据点查 p99 |
|---|---|
| 当前 | ~300 µs |
| Tier 1 完成 | ~150 µs |
| Tier 1+2 完成 | ~80 µs |

---

## 12. 与现有路线图的衔接

本路线图与已落地 / 进行中的工作互补：

| 已完成 / 进行中 | 本路线补强方向 |
|---|---|
| WAL Durability Policy / Group Commit / Sharded Manager | 配合 Pipelined Write，commit pipeline 直接受益 |
| ART memtable | 配合 Sharded Memtable，每 shard 仍是 ART |
| Range Tombstone | 配合 fsmeta DeleteRange API |
| External SST | 不变 |
| KV Separation (vlog) | 配合 Block Compression，vlog 也可以压缩 |

---

## 13. 风险与权衡

| 优化 | 风险 |
|---|---|
| Sharded Memtable | flush 调度复杂度上升；iterator merge cost |
| Pipelined Write | 错误回传 + crash 一致性需仔细设计 |
| Hybrid Tiered+Leveled | read amp 略升，需 Filter 永驻配合 |
| mmap SST | 无法精确控制驱逐，OS 行为不一定可预测 |
| Direct IO | 失去 OS read-ahead 优势，scan 可能变慢 |
| Shard-per-Core | 数年工程，期间引擎双轨 |

---

## 14. 一句话总结

> **LSM 内核还能拉的最大杠杆**：
> - **写**：Sharded Memtable + Pipelined Write + Subcompactions / Trivial Move
> - **读**：Compressed Block Cache + Ribbon Filter + Filter 永驻
> - **结构**：Hybrid Tiered+Leveled
> - **IO**：io_uring
>
> 4 周做完 Tier 1，写吞吐 5-10×；12 周做完 Tier 1+2，整体性能进入工业级头部。
>
> Shard-per-Core 是远期愿景，能让性能从亚线性扩展变成线性扩展，但代价是整个引擎重写，不在短期 ROI 项内。
