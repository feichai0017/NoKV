# 2026-04-27 Slab Substrate：NoKV metadata primitives 的 typed sidecar 物理执行层

> 状态：**Phase 0–5 已落地，6 forward-ref**。Phase 0 在 PR #161；Phase 1–5
> 在 `feature/slab-substrate` 分支：metadata default no-offload fast path
> (Phase 1)、`engine/slab/Segment` 物理层抽出 (Phase 2)、persistent
> Negative Slab (Phase 3)、SnapshotSlab spike → 不做 (Phase 4，独立 note)、
> DirPageSlab RFC (Phase 5，独立 note) 都已 commit。Phase 5b（拆出
> `engine/slab/Manager` + vlog 降级成 wrapper）在同一分支上完成。Phase 6
> （UpdateSlab）独立 RFC，**不在本分支**。
>
> 本文是 vlog 重构的第三版 note。前两版（"MetaSlab redesign" / "Slab
> substrate v1"）依次被 review 修正，最终切法不再是"重构 vlog"，而是
> **把 slab 升级成 NoKV native metadata primitives 的 typed sidecar
> 物理执行层**。slab 不是新版 vlog，是 fsmeta primitive（`ReadDirPlus`、
> `SnapshotSubtree`、`WatchSubtree`、`RenameSubtree`）的 *primitive-aware
> physical layout*。

---

## 1. 重新定位：从"vlog 重构"到"primitive-aware physical layout"

之前两版 note 把 slab 当成 "更通用的 vlog"，思路是 "value 不分 size，按
metadata 语义分 layout"。这次 review 进一步指出：**真正有创新价值的是
"按 NoKV native primitive 决定物理位置"**——

| 通用 KV 视角（旧） | NoKV native 视角（新） |
|---|---|
| value > threshold 进 vlog | `ReadDirPlus` 大目录进 DirPage Slab |
| 通用 negative cache | fsmeta key 的 negative slab，跟 mount/subtree 绑定 |
| snapshot = LSM 范围扫 | `SnapshotSubtree` 直接产出 sealed slab artifact |
| GC by reference scanning | GC by lifecycle event（mount/subtree retire） |

slab 跟 NoKV 自己的 primitive 绑起来，才是真正区别于 RocksDB / Pebble /
Badger / FoundationDB 的设计点。否则就是又写一个通用 KV 优化。

## 2. 产品定位与现状错配

NoKV 的客户场景是 **metadata-first**：

| 场景 | value 大小 | 主要 primitive |
|---|---|---|
| DFS metadata（HDFS / CephFS / JuiceFS / SeaweedFS filer） | 150B-1KB | `ReadDirPlus`、`Lookup` |
| 对象存储 metadata（S3 manifest / bucket index） | 200B-1KB | `ListObjects`、`HeadObject` |
| AI training metadata（dataset manifest / feature schema） | 100-500B | `SnapshotSubtree`、checkpoint |
| 嵌入式偶发大对象 | 4KB-1MB | 显式 blob API |

vlog 当前形态的问题：

1. **mandatory 在主写路径上**：`db.vlog.write()` 是 commit pipeline 必经
2. **bug 暴露面大**：1M+3KB bench 发现 `LogFile.Write` 在 reserve 解耦
   写顺序时把 lf.size 缩回去（fix 见 §4）
3. **物理层和业务语义混在 `engine/vlog/`**：mmap segment 管理、bucket
   路由、ValuePtr 编解码、GC sample 全在一个包里——其它子系统想复用
   mmap 物理层就得拖 vlog 业务语义
4. **没有 primitive-awareness**：所有大 value 都进 vlog，不区分 dataset
   snapshot / dir page / negative cache / value separation 各自不同的
   生命周期和一致性需求

## 3. 三层架构

```
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 3 — Rooted Lifecycle（控制平面集成）                              │
│    correctness-critical slab → meta/root 生命周期                      │
│    derived slab → 不进 root，丢失重建                                    │
│    snapshot slab → epoch 绑定，retire 整文件删除                         │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 2 — Typed Slab Consumers（业务语义）                             │
│  ┌──────────────┬──────────────┬──────────────┬────────────────────┐│
│  │ ValueLog     │ Negative     │ DirPage      │ Snapshot          ││
│  │ (existing)   │ Slab         │ Slab         │ Slab              ││
│  │ Authoritative│ Derived      │ Derived      │ Lifecycle-bound   ││
│  └──────────────┴──────────────┴──────────────┴────────────────────┘│
├──────────────────────────────────────────────────────────────────────┤
│  Layer 1 — Slab Substrate（engine/slab/）                            │
│    BlobLog / SlabFile：append / read / seal / verify / remove         │
│    没有：ValuePtr、bucket 路由、business GC、main manifest 集成          │
└──────────────────────────────────────────────────────────────────────┘
```

**关键 invariant**：每一层只关心自己那一层的事。物理层不知道 DirPage 是
什么；DirPage Consumer 不知道 Snapshot 怎么 seal；Rooted Lifecycle
不知道 mmap 怎么 grow。

## 4. Phase 0：vlog `LogFile.Write` high-water CAS（已完成）

### 4.1 Bug 现象

1M YCSB + 3KB value（触发 value separation）跑 ~4 秒后 `NoKV read: EOF`。
最小复现：200k records + 3KB + conc=16 在 0.79s 内必崩。

debug log 命中 `EOF #3`：`offset+valsz > lfsz` 且 `offset == lfsz`，
说明写入这条 entry 的 batch 已经 publish ptr，但 lf.size 没包含尾部。

### 4.2 Root cause

`engine/vlog/manager.reserve()` 持 `m.filesLock` 给每个 batch 拿 disjoint
offset 区间，**释放 filesLock 之后**才去抢 `store.Lock` 写入。N 个 batch
的 reserve 顺序与 Write 完成顺序解耦：较大 offset 的 Write 先完成
（lfsz=300），较小 offset 的 Write 后完成（lfsz.Store(200) 把高水位覆盖
回小值）→ 已 publish 的 ptr 在 Read 时命中 EOF。

### 4.3 Fix

`LogFile.Write` 改用 monotonic CAS（`engine/file/vlog.go:113-129`）。

### 4.4 关键 invariant

> **Invariant V1**：value pointer 只有在 `db.vlog.write(reqs)` 完整返回
> 之后才会被 publish 到 LSM。reader 可见 ptr 时，对应 batch 的所有 Write
> 都已经完成、lf.size 必然 ≥ ptr.offset + ptr.len。

high-water CAS **只保证 lf.size 不回退**，不保证"高水位以内没有洞"。
洞是真实存在的（reserve 之后未 Write 的区间），但 V1 屏蔽 reader 不会
撞到洞。`db.go runBurstCommit / runSingleCommit` 的 `vlog.write →
applyRequests` 顺序保证 V1。

### 4.5 测试覆盖

- `engine/file/vlog_test.go::TestLogFileWriteSizeMonotonicOutOfOrder` —
  覆盖 lf.size 单调（无 fix 时失败）
- `engine/vlog/manager_test.go::TestManagerConcurrentAppendReadAfterWrite`
  — 16 worker × 64 batch × 8 entry 并发 AppendEntries + 立即 Read，覆盖
  invariant V1（ptr publish discipline）

## 5. 一致性等级分类（这是设计核心，不是实现细节）

每个 slab consumer 必须明确标一个 consistency class。**slab 不是同一种
东西**，混在一起设计就会出 UpdateSlab 那种"破坏 MVCC 语义"的事故。

| Class | 含义 | 例子 | 失败语义 |
|---|---|---|---|
| **Authoritative** | 数据本体由 slab 持有，丢了就是数据损坏 | ValueLog（ptr → vlog payload） | 必须 WAL/manifest/GC 严格保证 |
| **Lifecycle-bound** | 由外部生命周期事件管理（snapshot epoch / mount retire） | Snapshot Slab | seal 后不可变，retire 后整文件删除 |
| **Derived** | LSM 是 authoritative，slab 是 cache/物化 | Negative Slab、DirPage Slab、（future）Hot Cache | 丢失可重建，不参与 commit |
| **Transactional** | 跟 MVCC version / commit 集成，必须 atomic | （future）UpdateSlab 等待独立 design | 跟 WAL/Percolator 一起设计才能做 |

**这个分类本身就是 design point**：之前 metaslab note 把 5 个 slab 当成
同种东西，正是因为没区分 class。

## 6. V1 三个 Consumer

### 6.1 NegativeSlab（Derived）

**对应 NoKV primitive**：fsmeta `Lookup` / `GetAttr` 对不存在 path 的
查询；S3 GetObject 404；HDFS 路径探测。

**机制**：`engine/lsm/negative_cache.go` 当前是 in-memory cuckoo filter，
进程重启全部丢失。加 `slab.Manager` 后端：
- async append miss key（不在主 read 路径上）
- restart 时 iterate segment 重建 in-memory cuckoo filter
- crash 丢 segment 数据 = 重新 warm，**不影响 read correctness**
- **不需要 manifest**（重建即可）

**为什么是第一个 consumer**：失败不影响 correctness，最小风险，最适合
验证 slab substrate 抽象可行性。

**收益**：进程 restart 后立刻有完整 negative cache，零 warmup。

### 6.2 SnapshotSlab（Lifecycle-bound）

**对应 NoKV primitive**：`fsmeta.SnapshotSubtree`、AI dataset
checkpoint、`PlanSnapshotSubtree`。

**机制**：`SnapshotSubtree` 当前返回一个 `SnapshotSubtreeToken`（MVCC
read epoch），后续 read 走 LSM MVCC。SnapshotSlab 改成"snapshot 可以
materialize 成 sealed slab artifact"：

```go
// 取一个 subtree snapshot，materialize 成 slab
artifact, err := db.MaterializeSnapshot(SnapshotSubtreeRequest{...})
// artifact 是一个 sealed slab file，包含所有 dentry+attr
// 可以通过 export / scp / S3 上传
// retire 时整文件删除，不需要 GC scan
```

**生命周期**：
- write：snapshot epoch 触发，一次性写入
- seal：epoch 关闭后 slab seal，不可变
- retire：epoch retire 时 unlink slab 文件，O(1)

**收益**：
- AI dataset checkpoint 可以 export 成单一 slab artifact（zero-copy
  sendfile 跨节点）
- snapshot 跟 epoch lifecycle 严格绑定，不需要独立 GC
- 跟 `2026-03-31-sst-snapshot-install.md` 对齐：SST snapshot 是
  raft-level 物理迁移，SnapshotSlab 是 fsmeta-level 逻辑 export

**Phase 4 spike**：先调研 SST snapshot install 现状，确认 SnapshotSlab
不重复 raft snapshot 的功能后再做。

### 6.3 DirPageSlab（Derived）—— 最贴 NoKV 的创新点

**对应 NoKV primitive**：`fsmeta` 的 `ReadDirPlus`-style 操作（返回
`DentryAttrPair`）。

**痛点**：大目录 listing 是 metadata 系统的核心瓶颈，通用 KV 很难原生
优化。当前 NoKV 走 LSM prefix scan：
- 每次 ReadDirPlus 都走 N 个 SST 的 prefix range
- block cache 装的是 SST data block，不是 packed dirent
- 大目录（10K+ entry）每次 list 都是 N 次 IO + N 次 decode

**机制**：DirPage Slab 把大目录的 dentry+attr 物化成 packed pages：

```
DirPage record:
  mount        uint32
  parent_inode uint64
  page_no      uint32
  frontier     uint64  // WatchSubtree event cursor，判断 page 是否过期
  checksum     uint32
  payload      []packed DentryAttrPair
```

**读路径**：
1. `ReadDirPlus` 先查 DirPageSlab：找 (mount, parent_inode) 的 pages
2. 如果存在且 frontier ≥ 当前 WatchSubtree epoch → 直接 sequential 读
   page，O(1) decode 出 DentryAttrPair[]
3. 否则 fallback LSM prefix scan，async 写入新 page

**写路径**：
- 主写路径**不变**——dentry 仍写 LSM（authoritative truth）
- DirPage 是 derived，async materialize（compaction 后台 / 第一次
  ReadDirPlus 时 lazy build）
- `RenameSubtree` / `Unlink` 把相关 (mount, parent_inode) 的 pages
  invalid（标 stale），不需要同步重写

**与 fsmeta primitive 的耦合**：
- WatchSubtree 的 event cursor → DirPage frontier
- RenameSubtree → invalidate source + dest parent 的 pages
- SnapshotSubtree → 可以基于 DirPage materialize（如果 frontier 够新）

**为什么是 NoKV 的 design point**：
- 通用 KV（RocksDB / Pebble / Badger）：不知道 "directory" 是什么，
  没法专门优化
- TiKV：靠应用层（CDC、TiFlash）做物化，跟 KV 引擎解耦
- NoKV：fsmeta primitive 是 first-class，DirPage 直接对应
  `ReadDirPlus`，是引擎自己的 native optimization

**收益**：
- 大目录 ReadDirPlus 从 N 次 SST IO + N 次 decode → 1 次 page read
- block cache 不再被大目录 dirent 占据
- WatchSubtree 集成天然，不需要外部 invalidation 机制

### 6.4 ValueLog Consumer（Authoritative，保留）

`engine/vlog/` 包**继续存在**，对 `db.go` 接口零改动：
- bucket 路由、ValuePtr 编解码、discardStats、main manifest 集成都留在
  vlog 这层
- 物理 IO 委托给 `slab.Manager`

**降级**：从"主写路径默认必经层"降成"value separation consumer"。
metadata profile（value < threshold）下 commit pipeline 不进 vlog 代码
路径（Phase 1 fast path）。

**main manifest 不动**：ValueLog 是 Authoritative class，segment 元数据
（valueLogHead / discardStats）必须在 main manifest，是 correctness
边界。

## 7. Update Slab：明确移出 v1

之前两版 note 都把 UpdateSlab 当成"in-place 优化"。但它**改变了 "一个
version = 一个 LSM entry" 的 invariant**：
- snapshot read：拿 LSM 历史版本 + slot 当前值 → 时间错乱
- Percolator commit：lock/write/data 三列，slot 走哪一列？slot CAS 跟
  lock acquire 怎么排序？
- crash recovery：WAL replay 主路径 entry vs slot record 的相对顺序怎么定？
- Bloom + range filter：slot 改了不通过 LSM，filter 不变 → false negative

UpdateSlab 是 **Transactional class**，必须独立 design RFC。如果以后做，
设计应该是 **versioned append/delta**（每次 update 写新 version 到 slab，
版本链跟 MVCC 对齐），**不是 in-place fixed slot**。

本次 vlog 重构 **不做 UpdateSlab**。

## 8. 创新点（论文级 design points）

| Design point | 含义 | 跟谁不一样 |
|---|---|---|
| **Primitive-aware physical layout** | 物理位置按 metadata primitive 决定，不按 value size threshold | RocksDB / Badger 都是 size-based |
| **Authority-scoped lifecycle** | slab 创建/seal/retire 跟 mount / subtree / snapshot epoch 绑定 | 通用 KV 靠后台 GC scan reference |
| **Correctness-class separation** | Authoritative / Lifecycle-bound / Derived / Transactional 明确分开 | 大多数 KV 把 cache 和 data 当成同种东西 |
| **Directory-page materialization** | ReadDirPlus 从 KV prefix scan → metadata-native page read | 通用 KV 不知道 dir 是什么 |
| **Snapshot as storage artifact** | SnapshotSubtree 产出 sealed slab，可 export/transfer | snapshot 通常是 MVCC token，不是物理对象 |
| **GC by lifecycle, not scanning** | 整文件 unlink，不需要 reference counting | vlog GC 必须 sample + scan |

这六个里面，**Directory-page materialization** 和 **Authority-scoped
lifecycle** 是真正"NoKV 自己的东西"——前者直接对应 fsmeta primitive，
后者直接对应三平面定位（control plane 事件驱动 slab 生命周期）。

## 9. v1 路线

| Phase | 动作 | Class | 状态 | 验证 |
|---|---|---|---|---|
| **0** | LogFile.Write high-water CAS | — | ✓ done (PR #161, b6b0dd25) | 1M+3KB 全 6 workload 第一 |
| **0a** | manager 并发 AppendEntries+Read 测试 | — | ✓ done (PR #161) | invariant V1 显式覆盖 |
| **0b** | 修正 design note（本文） | — | ✓ done | 本文 |
| **1** | metadata default no-offload fast path | — | ✓ done (c0458f03) | BenchmarkDBCommitVlogFastPath inline +22%~+64% |
| **2** | 抽 `engine/slab/Segment` 物理层；vlog 文件层改 wrapper | — | ✓ done (083a71a0) | 现有 vlog 单测 + 1M+3KB bench 全绿 |
| **2a** | `Segment` size 语义重做（Open=0, LoadSizeFromFile, Capacity） | — | ✓ done | TestSegmentFreshOpenSizeIsZero / TestSegmentLoadSizeFromFile |
| **3** | NegativeSlab + 顶层 `NoKV.Options` 透传 | Derived | ✓ done (c0dbaa35 + 后续) | TestNegativeCachePersistsAcrossOpen |
| **4** | SnapshotSlab spike → 不做 | Lifecycle-bound | ✓ done | `2026-04-27-snapshot-slab-spike.md` |
| **5** | DirPageSlab RFC（API + 格式 + frontier） | Derived | ✓ RFC done | `2026-04-27-dirpage-slab-rfc.md` |
| **5b** | 拆出 `engine/slab/Manager` + vlog 降级成 wrapper | — | ✓ done | 现有 vlog/lsm 全测 + 数据 race-free |
| **5c-5f** | DirPageSlab 实现（page write/read、ReadDirPlus 集成、失效、bench） | Derived | TODO | 大目录 ReadDirPlus latency |
| **6** | UpdateSlab 独立 RFC | Transactional | independent | 先 design 后实现，**不在本分支** |

## 10. 1M + 3KB bench baseline（vlog 路径走满）

Phase 0 fix 后跑通，全 6 workload NoKV 第一：

| Workload | NoKV | Badger | Pebble | NoKV vs Badger |
|---|---|---|---|---|
| A 50/50 r/u | 607K | 283K | 25K | 2.1x |
| B 95/5 r/u | 1.15M | 651K | 153K | 1.8x |
| C 100% read | 989K | 634K | 78K | 1.6x |
| D latest | 1.10M | 732K | 182K | 1.5x |
| E scan | 69K | 29K | 46K | 2.4x |
| F RMW | 510K | 225K | 26K | 2.3x |

这是 vlog 路径走满（Authoritative consumer）的对比起点。Phase 1 完成后
跑 1M+1KB（不触发 value separation）作为 metadata profile baseline。
Phase 5 DirPageSlab 完成后跑专门的大目录 ReadDirPlus bench（fsmeta 自己
的 workload，不是 YCSB）。

## 11. 决策日志

- **不做"通用 vlog"**：slab 是 NoKV native primitive 的 typed sidecar
  物理执行层，不是 BadgerDB 风格的 generic value separation
- **物理层叫 Slab，单文件叫 Segment**：跟 kernel allocator / RocksDB /
  Pebble 术语对齐
- **vlog 包名保留**：跟 Badger / RocksDB BlobDB 命名沿袭一致，外部用户
  不困惑
- **ValueLog 元数据留在 main manifest**：Authoritative class，是
  correctness 边界
- **Negative / DirPage 用独立 SlabManifest 或不用 manifest**：Derived
  class，丢失可重建
- **DirPage Slab 是核心创新**：直接对应 fsmeta `ReadDirPlus` primitive，
  是其他通用 KV 没有的 design point
- **UpdateSlab 移出本次重构**：Transactional class，破坏 "一个 version
  = 一个 LSM entry" invariant，必须独立 RFC
- **DeleteSlab 移出本次重构**：现有 LSM RangeTombstone 已经覆盖大部分
  批量删除场景
- **Snapshot consumer 加 spike 调研**：可能跟 SST snapshot install 重复

## 12. 关联文档

- `2026-04-27-sharded-wal-memtable.md` — 主写路径 sharding，跟 Phase 1
  no-offload fast path 都是主路径优化
- `2026-04-27-parallel-l0-compaction.md` — compaction 并行化
- `2026-03-31-sst-snapshot-install.md` — raft-level snapshot install
  现状，Phase 4 spike 时需要参考
- `2026-04-25-namespace-authority-events-umbrella.md` — control plane
  事件下发模型，DirPage / Snapshot Slab 的 lifecycle 触发要对齐
- `2026-04-24-fsmeta-positioning.md` — fsmeta 产品定位，slab consumer
  设计的源头
- `2026-04-25-snapshot-subtree-mvcc-epoch.md` — SnapshotSubtree 的
  epoch 模型，SnapshotSlab 的 lifecycle 基础
