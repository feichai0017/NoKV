# 2026-04-27 DirPageSlab RFC：把 ReadDirPlus 物化成 packed dirent pages

> 状态：**RFC**（设计提案，未实现）。这是 slab substrate redesign
> （`2026-04-27-slab-substrate.md`）Phase 5 的展开 RFC。本文锁定 API、
> 物理 wire format、与 fsmeta primitive 的耦合点、phase 路线图。
>
> DirPageSlab 是 slab substrate 的 **创新核心**——它跟 NegativeSlab
> 这种"持久化 cache"不同，是 NoKV native metadata primitive
> （`fsmeta.ReadDirPlus`）专属的物理 layout。通用 KV（RocksDB / Pebble /
> Badger / FoundationDB）做不出来，因为它们不知道"目录"是什么。

---

## 1. 痛点

### 1.1 当前 ReadDirPlus 的开销

`fsmeta.ReadDirPlus`（语义；`PlanReadDir` + 后续读取）当前走的路径：

1. LSM prefix scan dentry keys（key encoding `mount|parent|name`）
2. 对每个 dentry record，再 LSM Get 对应的 inode record
3. 拼装成 `DentryAttrPair[]` 返回

对单条 lookup，这没问题。对**大目录**（HDFS 经验：10K-1M 个 entry 的目录
非常常见——日志归档、训练数据集 shard、对象存储 bucket index）：

- prefix scan：N 个 dentry 跨多个 L0/L1 SST → N 次 block IO + binary search
- per-dentry inode lookup：N 次 Get → N 次 SST probe（即使 hot 也要解码）
- block cache 装的是 SST data block，**dirent 和 inode 物理上分散**——一个
  10K-entry 的目录可能跨几十个 block，cache fragmentation 严重
- compaction 后 N 次 SST 重排，热目录的 block locality 经常被破坏

profile 上一个 10K-entry ReadDirPlus 在 cold cache 下耗时几百 ms 是常态。
HDFS / CephFS 之所以在大目录场景慢，根源就在这里。

### 1.2 通用 KV 优化为什么不够

- **Bloom filter**：只对存在性查询有效，对 prefix scan 无帮助
- **block cache**：fragment 严重，hit ratio 不能解决"还要 N 次 decode"
- **prefix bloom**：对随机 lookup 有用，对一次性扫整目录不省事
- **shard hint cache**：之前做的，对单 key 有效，对 N 个 dentry 仍是 N 次
- **negative cache**：完全无关

**根本问题**：通用 KV 把 dentry 和 inode 当成无意义 KV，无法做"directory
作为一个对象"的 layout。

### 1.3 业界对比

- **HDFS NameNode**：纯内存，大目录开销天花板是内存
- **CephFS MDS**：fragmenting + `dirfrag` page-based directory，类似 DirPage
- **JuiceFS / SeaweedFS filer**：靠后端 KV 的 prefix scan，跟 NoKV 现在
  痛点一样
- **TiKV-backed FS**：靠应用层（CDC / TiFlash）做物化，跟 KV 引擎解耦

NoKV 的优势：**fsmeta primitive 是 first-class，DirPage 可以直接进引擎**，
不需要应用层另起 cache 层。

## 2. 核心设计

### 2.1 概念

把 `(mount, parent_inode)` 的 dentry+attr 列表 **物化成 packed pages**，
存在 slab consumer 里。LSM 仍然是 authoritative truth；DirPage 是
**Derived class**——丢失 / 过期 / corrupt 都可以从 LSM 重建。

### 2.2 一致性等级

**Derived**（per slab-substrate note §5）：
- LSM 是 authoritative
- DirPage 是 cache / 物化
- crash 丢 DirPage = re-warm（下次 ReadDirPlus fallback LSM scan 顺便重建）
- 不参与 commit pipeline，不需要 WAL 集成
- 不写 main manifest

**关键 invariant**：
- write path（Create/Unlink/Rename）**永远**正确写 LSM
- DirPage 只是查询加速；任何丢失都是 best-effort
- frontier 比对（§4）保证不返回 stale 数据

## 3. Wire format

每个 DirPage record（slab segment 内的 entry）：

```
DirPage record:
  magic        uint32  "DPSL" little-endian
  version      uint16
  mount        varint  (mount ID 编码后)
  parent       uvarint (parent InodeID)
  page_no      uvarint (0-indexed within (mount, parent))
  frontier     uvarint (WatchSubtree event cursor / mutation epoch)
  entry_count  uvarint
  entries: repeated of
    name_len   uvarint
    name       [name_len]byte
    inode      uvarint  (InodeID)
    attr_blob  uvarint length-prefixed (encoded InodeRecord)
  checksum     uint32 (CRC32 of preceding bytes within this record)
```

设计选择：
- **包含 inode attr blob**：跟 ReadDirPlus 语义一致，省掉后续 N 次 Get
- **每条 record 自带 frontier**：reader 不需要查另一张表
- **跟 InodeRecord encoding 解耦但 reuse**：DirPage 内部存 `attr_blob`，
  调用方用 `fsmeta.DecodeInodeValue` 反序列化
- **per-record checksum**：partial corruption 只丢这一页

### 3.1 Page 大小

每页目标 **4-16KB**（一个 mmap page 内）。entry 数 cap 在 64-256 个
（取决于平均 name 长度）。大目录跨多页，按 `page_no` 串接。

### 3.2 Page 生命周期

| 状态 | 含义 |
|---|---|
| Valid | frontier 等于当前 directory 的 mutation epoch |
| Stale | frontier 落后；下次 read 跳过，触发 lazy rebuild |
| Garbage | 整 segment 满了或 stale 比例过高，等 compaction 整体 retire |

## 4. 与 fsmeta primitive 的耦合

### 4.1 Frontier（mutation epoch）

每个 `(mount, parent_inode)` 维护一个单调递增的 `dir_epoch`。任何修改这个
目录的操作（Create / Unlink / Link / Rename 涉及到的 from/to parent）
**bump epoch by 1**。

存储位置：
- 选项 A：放 InodeRecord 里加 `DirEpoch uint64` 字段
- 选项 B：单独一个 `dir_epoch` key per directory inode
- 选项 C：纯内存 + 重启时从 DirPage 最大 frontier 推断（best-effort
  Derived 可以这么做）

**推荐选项 C**：dir_epoch 不需要持久化（DirPage 是 Derived），重启时
统一 fallback LSM 重建一次足够。Phase 5b 决定。

### 4.2 WatchSubtree 集成

`WatchSubtree` 已经按 subtree 维护 event cursor。DirPage frontier 复用
WatchSubtree 的 `event_cursor`：
- subtree 内任何 dentry 修改 → cursor++
- DirPage 写入时记下当前 cursor 作为 frontier
- DirPage read 时比对 cursor，落后则 stale

这样 DirPage 的失效跟 `WatchSubtree` 的事件流统一。Watch 客户端看到的
dentry change event 跟 DirPage stale 是同一个 trigger。

### 4.3 RenameSubtree 失效

`PlanRenameSubtree(req)` 当前移动 subtree 根 dentry。失效路径：
- bump source parent 的 dir_epoch
- bump dest parent 的 dir_epoch
- subtree 内部 dentry 不动 → 内部 DirPage 不需要失效

`PlanRename`（同 parent rename）只需要 bump 该 parent 的 dir_epoch。

### 4.4 Unlink 失效

`PlanUnlink(req)` 删除一条 dentry：
- bump parent 的 dir_epoch

不需要扫 DirPage 找具体 entry 删除——下次 read 跳过 stale page，lazy
rebuild。

### 4.5 Create / Link 失效

同上，bump parent 的 dir_epoch。

## 5. API 设计

### 5.1 fsmeta 层

```go
// fsmeta/dirpage.go (新文件)

// DirPageReader is the Derived consumer interface DirPageSlab implements.
type DirPageReader interface {
    // Lookup returns the materialized pages for (mount, parent) if every
    // page is fresh (frontier matches the current dir_epoch). Returns
    // (nil, false) on miss / stale / corrupt — caller falls back to LSM
    // prefix scan and may opportunistically MaterializeAsync.
    Lookup(mount MountID, parent InodeID) ([]DentryAttrPair, bool)

    // MaterializeAsync schedules a background rebuild of the directory
    // page set. Caller hands over the LSM-scanned dentry+attr pairs.
    // Idempotent.
    MaterializeAsync(mount MountID, parent InodeID, frontier uint64, pairs []DentryAttrPair)

    // Invalidate bumps dir_epoch for (mount, parent). Called from
    // Create/Unlink/Link/Rename plan apply path.
    Invalidate(mount MountID, parent InodeID)
}
```

### 5.2 ReadDirPlus 整合

```go
func (s *Server) ReadDirPlus(req ReadDirRequest) ([]DentryAttrPair, error) {
    // 1. fast path: DirPageSlab
    if pairs, ok := s.dirPages.Lookup(req.Mount, req.Parent); ok {
        return paginate(pairs, req.StartAfter, req.Limit), nil
    }
    // 2. fallback: LSM prefix scan + per-dentry inode Get
    pairs, frontier, err := s.scanLSM(req.Mount, req.Parent)
    if err != nil { return nil, err }
    // 3. async materialize
    s.dirPages.MaterializeAsync(req.Mount, req.Parent, frontier, pairs)
    return paginate(pairs, req.StartAfter, req.Limit), nil
}
```

### 5.3 LSM 层

```go
// engine/lsm/dirpage.go (新文件)
// DirPageSlab implements fsmeta.DirPageReader on top of slab.Manager.

type DirPageSlab struct {
    mgr      *slab.Manager     // 物理 substrate (Phase 2 建立的)
    epochs   sync.Map          // (mount, parent) -> *atomic.Uint64
    cache    *dirPageCache     // in-memory page index (LRU)
}
```

## 6. 实现 phase 路线

| Phase | 内容 | 估时 |
|---|---|---|
| **5a** | RFC（本文） | done |
| **5b** | `engine/slab/manager.go`：从 `engine/vlog/manager.go` 抽出通用 segment manager（rotation / GC sample / sub-manifest），DirPage / 未来 consumer 都用它 | 1 周 |
| **5c** | `engine/lsm/dirpage.go`：DirPageSlab 实现，独立单测（page write/read/stale detect），不接 fsmeta | 1 周 |
| **5d** | fsmeta `ReadDirPlus` fast-path 集成 + LSM fallback + async materialize；端到端单测 | 1 周 |
| **5e** | 失效路径：Create/Unlink/Link/Rename/RenameSubtree 调用 `Invalidate` | 0.5 周 |
| **5f** | 大目录 bench：10K / 100K entry ReadDirPlus latency 对比 cold/warm | 0.5 周 |

总计 ~4 周。每个 phase 可独立 PR。

## 7. Bench 验证目标

新增 `BenchmarkFsmetaReadDirPlus`：

| 维度 | 数值 |
|---|---|
| Entries per dir | 1K / 10K / 100K |
| Cache state | cold / warm |
| 操作 | full ReadDirPlus / paginated |
| 对比 | DirPageSlab on vs off |

预期收益：
- 10K-entry warm cache：从 ~10-50ms 降到 < 1ms（一次 mmap page read）
- 10K-entry cold（first read）：跟 baseline 持平（必须 LSM scan 一次重建）
- 100K-entry warm：一次 page read 显著优于 N 次 SST IO

## 8. 一致性 / 风险 / trade-off

### 8.1 stale page 风险

invalidate 不及时 → reader 拿到过期数据。缓解：
- frontier 比对是 per-read 的；stale page 直接 fallback LSM
- write path 必须**同步** Invalidate（不能 async），保证下次 read 立即
  fallback；async 是 *re-materialize*，不是 invalidate
- 测试覆盖：mutation 后立即 ReadDirPlus，必须看到新状态

### 8.2 page size tuning

太小 → 大目录跨太多 page，sequential read 变 random read。
太大 → 单页占 cache，evict 其它热目录。
Phase 5c 用 4-16KB 起步，bench 后调。

### 8.3 frontier 持久化

option C（不持久化）的代价：重启后第一次 ReadDirPlus 必须 fallback LSM
重建。重启后短期 latency 升高（warm-up window）。可接受——同 NegativeSlab
的"crash forces re-warm"语义一致。

### 8.4 multi-page 一致性

跨 page 的 ReadDirPlus 需要原子性吗？答：**不需要**。LSM scan 本身也不是
原子（scan 期间可能有写）。DirPage 的 frontier 是 per-page 的，page A
和 page B 可能 frontier 不同——caller 看到的就是"两次部分更新"，跟现在
LSM scan 行为一致。client 想要原子的应该用 SnapshotSubtree 拿 token。

### 8.5 跟 SnapshotSubtree 的关系

SnapshotSubtree 拿 MVCC token；DirPage 跟 token 无关，永远 read latest。
SnapshotSubtree 要的"frozen subtree" 用户应该走 SST snapshot install
路径或者未来的 SnapshotSlab，不是 DirPage。**两条独立的 axis**。

### 8.6 内存开销

每页 4-16KB；100K entry / 10 entry per page = 10K pages × 16KB = 160MB
per directory。需要 LRU 淘汰。Phase 5c 加 cap option（默认 256MB-1GB
per LSM）。

## 9. 创新点（这是论文级 design point）

总结之前 metaslab 大伞被砍剩下的真正"NoKV-only" design points：

1. **Primitive-aware physical layout**：DirPage 跟 `ReadDirPlus` 一一对应，
   不是按 value size 分流。
2. **WatchSubtree-driven frontier**：cache 失效跟 fsmeta 事件流统一，
   不需要外部 invalidation 协议。
3. **Derived consistency class**：丢了重建，不参与 WAL/commit；释放出
   DirPage 设计空间，避免"另一个 LSM"陷阱。
4. **Lazy rebuild on miss**：fast-path 失败自动触发 materialize，不需要
   显式 warmup API。

这四点 generic KV 都做不到——需要引擎知道 metadata primitive 是什么。
**这是 NoKV 区别于 RocksDB / Pebble / Badger / FoundationDB 的真正
design point**，不是"我们也有 vlog"。

## 10. 决策日志

- **Page 内含 inode attr blob**：避免 ReadDirPlus 的二次 Get；用空间换
  IO 数。每页 +inode size × entry count。
- **frontier 用 WatchSubtree event cursor**：复用现有事件流，不另起
  invalidation 协议。
- **Derived class，不参与 main manifest**：失败丢失=re-warm；最大化 design
  自由度。
- **Page 大小 4-16KB 起步**：与 mmap page size 对齐，bench 后调。
- **不持久化 dir_epoch**：option C；重启 warm-up window 可接受。
- **不做"原子 ReadDirPlus across pages"**：跟 LSM scan 行为一致；要原子
  用 SnapshotSubtree 走另一条路。

## 11. 关联文档

- `2026-04-27-slab-substrate.md` — slab substrate 重定义，本文是其
  Phase 5 的展开 RFC
- `2026-04-27-snapshot-slab-spike.md` — Phase 4 spike 结论
- `2026-04-24-fsmeta-positioning.md` — fsmeta 产品定位
- `2026-04-25-snapshot-subtree-mvcc-epoch.md` — SnapshotSubtree epoch 模型
- `2026-04-25-namespace-authority-events-umbrella.md` — control-plane 事件
  下发模型，DirPage frontier 跟 WatchSubtree 事件流的协议基础
