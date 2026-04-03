# 基于 SST 的 Snapshot Install 设计与实现

> 状态：已在 migration 路径和内部 raft snapshot payload 路径中落地。

这篇记录 GitHub issue `#28` 背后的设计与实现：

> 为 `RaftStore` 实现基于 SSTable 的 Snapshot Install。

---

## 1. 为什么这件事重要

NoKV 之前已经有一条正确的 standalone 到 cluster 的提升路径：

- `plan`
- `init`
- `serve`
- `expand`
- `transfer-leader`
- `remove-peer`

这条路径的工作流、checkpoint、恢复和测试都已经成立。接下来真正的瓶颈不再是“流程怎么走”，而是：

> 数据怎么搬过去。

最初的 region bootstrap / install 路径是刻意 correctness-first 的：

- `init` 把 region snapshot 导出到本地 seed 目录
- `expand` 生成内存 snapshot payload
- 目标节点通过常规 write/apply 路径导入这些逻辑 entry

这条路第一阶段是对的，因为：

- 生命周期语义清楚
- 恢复容易推理
- 不会过早把存储层细节泄露到 migration 协议里

但它也有明显代价：

- 全量逻辑重编码
- 大 snapshot 会占用更多内存
- 目标侧还要走一遍常规 write path
- 存在可避免的写放大

因此，数据搬运层需要升级，但 migration 主线本身不应该被推翻。

---

## 2. 当前系统边界

当前实现相关代码主要分布在：

- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/raftstore/snapshot/meta.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/raftstore/snapshot/dir.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/raftstore/snapshot/payload.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/raftstore/migrate/init.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/raftstore/migrate/expand.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/raftstore/store/peer_lifecycle.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/lsm/external_sst.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/db_snapshot.go`

当前 snapshot bridge 暴露的是一层窄接口：

- `ExportSnapshot(...)`
- `ImportSnapshot(...)`
- `ExportSnapshotTo(...)`
- `ImportSnapshotFrom(...)`

这层接口故意维持在“region snapshot”语义，不直接把 LSM 的底层 SST builder 暴露给 raftstore。

### 当前已经具备的能力

存储层已经有 SST ingest 支持：

- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/lsm/external_sst.go`

`ExportExternalSST(...)` / `ImportExternalSST(...)` / `RollbackExternalSST(...)` 已经处理了：

- 从 materialized entries 生成 snapshot SST
- 输入校验
- key-range overlap 检查
- 与现有 L0 的冲突检查
- manifest logging
- 失败回滚

也就是说，SST ingest primitive 已经存在，真正要解决的是：

> 如何把它放进 migration / snapshot 协议，而不是让存储层细节反向污染迁移层。

### 让问题变复杂的约束

NoKV 采用了 value separation：

- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/kv/value.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/db.go`
- `/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/vlog.go`

一部分 value 可能以 `ValuePtr` 的形式留在 vlog，而不是直接 inline 在 LSM 里。

这意味着：

> “直接拷贝现有 SST 文件再 ingest” 并不自动成立。

因为导入后的 SST 可能仍然指向源节点的 vlog 文件。

---

## 3. 我们最终采用的设计

### 3.1 设计目标

目标不是重写 migration 主线，而是只替换“数据搬运层”：

- 仍然先把一个 standalone workdir 提升成 full-range seed region
- 仍然通过 `expand` 把 seed 扩成 replicated region
- 仍然保留 install-before-publish 的生命周期边界

### 3.2 关键决策

当前第一阶段的 SST snapshot 采用：

> **region-scoped、self-contained、与源端 vlog 独立**

也就是：

- snapshot 仍然是 region 范围内的逻辑快照
- 导出的 SST 文件里，value 会被 materialize 成 inline user bytes
- 不依赖源端 vlog segment 继续存在

这样做的结果是：

- snapshot install 仍然保持 region-scoped
- 目标端不需要先理解源端 vlog 布局
- rollback 和恢复语义保持简单

### 3.3 为什么 `ImportSnapshot(...)` 要返回富结果

`ImportSnapshot(...)` 当前不是只返回 region meta，而是返回完整 staged-import 结果。

原因是 install 路径需要完整处理这条生命周期：

1. 导入 SST
2. 尝试 publish / host peer
3. 如果 publish 失败，回滚已导入的 SST 文件

所以 import primitive 必须能携带：

- `result.Meta.Region`
- `result.ImportedFileIDs`
- `result.Rollback()`

这样高层 install 逻辑就可以：

- 保持 snapshot-level 语义
- 同时仍然拿得到 storage rollback 所需的信息

---

## 4. 哪些“看起来简单”的方案是错的

### 4.1 直接复用现有 SST 文件

这个看起来最省事，但对 migration phase one 是错的。

原因：

1. 现有 SST 是 LSM/compaction 产物，不是 region snapshot 产物
2. SST 边界未必和 region 边界对齐
3. 现有 SST 里的 value 可能仍然依赖源端 vlog
4. 这样会把 migration 协议和 compaction 历史绑定在一起

### 4.2 把源端 vlog 一起打包过去

这也不适合作为第一阶段方案。

问题在于：

1. install snapshot 会变成跨层协议：
   - SST 文件
   - vlog segments
   - vlog head / manifest 语义
2. 目标端导入和回滚会复杂很多
3. snapshot 不再干净地保持 region-scoped

以后对于超大 value，也许可以继续优化，但不该作为第一步。

### 4.3 把 split / reshard 和 snapshot install 一起做

当前 migration 的主线仍然是：

- 先提升出一个 full-range region
- 再在这个基础上做扩副本和后续 reshape

如果把 split / re-shard 和 SST install 一起改，会把以下几件事缠在一起：

- snapshot redesign
- install semantics
- region 布局演化

当前阶段不值得这么做。

---

## 5. Snapshot 产物长什么样

当前 SST snapshot 目录形态大致是：

```text
snapshot/
  sst-snapshot.json
  tables/
    000001.sst
    000002.sst
    ...
```

元信息里会明确记录：

- `version`
- `region`
- `entry_count`
- `table_count`
- `inline_values = true`
- aggregate `size_bytes`
- aggregate `value_bytes`
- per-table：
  - relative path
  - smallest key
  - largest key
  - entry count
  - size bytes
  - value bytes
- `created_at`

这个 manifest 的目的不是替代 LSM `MANIFEST`，而是把：

> region-scoped snapshot contract

写清楚。

---

## 6. 导出与安装流程

### 6.1 导出流程

导出仍然以 region-scoped logical iteration 为驱动，而不是扫描当前磁盘上“有哪些现成 SST 能搬”。

流程是：

1. 以 region key range 为边界迭代内部 entry
2. 通过当前 snapshot source 路径把 entry materialize 出来
3. 确保导出结果是 inline value entry
4. 写成 snapshot 专用 SST 文件

这样做的好处是：

- 语义边界清楚
- 不依赖 compaction 历史
- 不把底层 LSM layout 直接暴露给 migration 协议

### 6.2 安装流程

目标端安装流程保持原有生命周期原则：

1. 先导入 snapshot SST
2. 再执行 publish / host peer
3. 如果 publish 失败，则回滚已导入的 external SST

这条边界仍然非常关键，因为它决定了：

- install 完成前，不允许 region 对外可见
- publish 失败时，不留下半安装状态

---

## 7. 这次改动真正改变了什么

### 没变的

- migration 主线没变
- seed -> expand 的故事没变
- install-before-publish 的生命周期边界没变
- raft metadata snapshot 和 region state snapshot 仍然分层

### 变了的

- 数据搬运从“逻辑 entry payload”为主，升级成“SST snapshot payload”为主
- 大 region 的安装不再需要把所有内容都走常规 write/apply path
- snapshot install 的写放大和内存压力明显下降

也就是说：

> 这次升级改的是 transport / install 形态，不是 migration 的语义本体。

---

## 8. 还没解决什么

当前这条 SST snapshot install 路线仍然有明确边界：

1. 还没有把源端 vlog 复制纳入 snapshot 协议
2. 还没有把 split / reshard 并入同一轮改造
3. 还没有把 snapshot install 扩展成更通用的大规模 region rebalance 机制

这些都可以做，但不属于当前阶段最值得一起推进的内容。

---

## 9. 总结

这次设计最关键的点不是“我们开始支持 SST snapshot 了”，而是：

> 在不破坏 migration 主线和 lifecycle contract 的前提下，把 snapshot install 的数据搬运层升级成了更像工业系统的形态。

这比单纯追求“更快搬数据”更重要。

因为如果为了搬得快而把：

- region truth
- snapshot contract
- rollback 语义
- publish 边界

一起搞乱，那这条路长期是维护不住的。
