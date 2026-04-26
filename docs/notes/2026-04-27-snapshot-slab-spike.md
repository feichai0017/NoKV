# 2026-04-27 SnapshotSlab Spike：现有 SST snapshot install 是否需要新 slab consumer

> 状态：**Spike note**（调研，未实现）。结论：v1 **不做** SnapshotSlab；
> 已有 SST snapshot install 已经覆盖 raft 路径的所有 use case。SnapshotSlab
> 的真正 niche 是 fsmeta `SnapshotSubtree` → AI dataset checkpoint export，
> 这个需求今天还没成形，等需求真正出现再做。
>
> 触发：slab substrate redesign note（`2026-04-27-slab-substrate.md` §6.2）
> 把 SnapshotSlab 列为 V1 候选 consumer 之一，但要求先 spike 调研，避免
> 跟 `2026-03-31-sst-snapshot-install.md` 已有的 SST snapshot install 重复
> 工作。本文是这次 spike 的结论。

---

## 1. 现有 SST snapshot install 已经做了什么

代码位置：
- `raftstore/snapshot/dir.go` — region-scoped 目录格式
- `raftstore/snapshot/meta.go` — 接口与 ImportResult
- `raftstore/snapshot/payload.go` — tar payload 编解码
- `raftstore/migrate/{init,expand}.go` — 调用方
- `engine/lsm/external_sst.go` — 导入路径
- `db_snapshot.go` — DB 级 wrapper

物理形态：

```
snapshot/
  snapshot.json     (region-scoped manifest)
  tables/
    000001.sst      (snapshot 专用 SST，已 materialize value)
    000002.sst
```

跨节点传输用 `archive/tar`，`writePayload(w io.Writer, ...)` 一边 walk
本地 snapshot dir 一边写入 `tw := tar.NewWriter(w)`，`io.Copy(tw, f)`
streaming。`unpackPayload(r io.Reader, dir string, ...)` 反向。

已经具备的属性：
- region-scoped self-contained（snapshot.json + tables/*.sst）
- value-log independent（export 时 materialize value）
- streaming export/import（不是先序列化整体到 buffer）
- staged install + rollback（`ImportResult.Rollback()`）
- 临时目录 cleanup
- tar 路径安全检查（`secureSnapshotPath`）

## 2. 之前 design note 给 SnapshotSlab 列的卖点 — 逐条核对

| 卖点（来自 slab-substrate v1 note） | 现状对比 | 是否仍然成立 |
|---|---|---|
| 跟主 LSM 物理隔离 | SST snapshot 已经是独立 per-region 文件 | ❌ 重复 |
| 跨节点 install 走 sendfile zero-copy | tar `io.Copy` 已经 streaming，sendfile 收益边际 | ❌ 边际 |
| region 删 → snapshot 删，无 GC | snapshot dir 是临时的，install 完即清理 | ❌ 重复 |
| 不需要独立 GC | 同上 | ❌ 重复 |

**所有 v1 note 列出的卖点都已经被 SST snapshot install 覆盖**。如果只是
为了"跟物理上分离的 slab 文件 sendfile"，收益不值得新增一个 consumer。

## 3. SnapshotSlab 真正可能的 niche

调研下来发现一个 SST snapshot install **没有**覆盖的 use case：

### 3.1 fsmeta `SnapshotSubtree` → 可消费的 storage artifact

`fsmeta/plan.go::PlanSnapshotSubtree` 当前返回 `SnapshotSubtreeToken`
（一个 MVCC read epoch）。后续 read 仍然走 LSM MVCC。**没有**把 snapshot
materialize 成单一文件的能力。

潜在需求：
- AI dataset checkpoint：训练任务想 freeze 一个 dataset 的目录树状态，
  导出成单文件（或多 segment）给训练 worker / 远端 cache 消费
- 跨集群 dataset replication：把一个 fsmeta subtree dump 给另一个集群
- Time-travel debug：把某个 epoch 的 subtree 物化下来给离线分析

这些场景跟 raft snapshot install 是不同 axis：
- raft snapshot：region-scoped，install 到对端 raft store
- fsmeta snapshot artifact：subtree-scoped，export 给非 NoKV 消费者

### 3.2 archive 长期保存

目前 raft snapshot 跑完 install 就被清理掉。如果想长期归档（S3 lifecycle
对接、合规留存），需要"持久化保留 + 索引"机制。tar 文件够用但缺结构化
metadata。SnapshotSlab 可以提供 sub-manifest 索引（slab id / class /
owner / state / frontier / checksum / path），跟 archive 系统对接更顺。

## 4. 结论 & 建议

**v1 不做 SnapshotSlab**。理由：

1. raft snapshot install 已经被 SST 路径完全覆盖
2. fsmeta dataset artifact 的真实需求今天还没出现，做了也是闲置
3. 真做的话，SnapshotSlab 的核心价值是 **fsmeta subtree materialization**，
   不是物理隔离 / sendfile，应该等 fsmeta 提出 export API 时再设计
4. 如果今天硬上 SnapshotSlab，会跟 SST snapshot install 二选一摇摆，
   未来 fsmeta export 真正出现时反而可能因为已经有"SnapshotSlab"的命名
   占位而设计被绑架

**保留作为 forward reference**：本次 slab substrate redesign note §6.2
的 SnapshotSlab 章节降级成 "future use case"，明确 trigger 条件：
**当 fsmeta `SnapshotSubtree` 需要 materialize 成 export artifact 时再
开独立 RFC**。

## 5. 给现有 SST snapshot install 留的两条小路

调研过程中观察到两个可能的 follow-up，**不属于 slab substrate 重构范围**，
但记下来供以后参考：

### 5.1 ExportPayload 全量到 buffer

`ExportPayload(...)` 用 `var payload bytes.Buffer` 全量先 buffer 再返回。
对超大 snapshot 这是内存压力。`ExportPayloadTo(w io.Writer, ...)` 已经
是 streaming 形态，调用方应该尽量用 `*To` 变体。这是 SST snapshot install
自己的 follow-up，不需要 slab。

### 5.2 sendfile in `io.Copy(tw, f)`

`writePayload` 内 `io.Copy(tw, f)`，`tw` 是 `*tar.Writer`，最终走到 `w`
（network conn）。如果 `w` 实现了 `ReaderFrom`（`*net.TCPConn` 是的），
`io.Copy` 会自动用 sendfile。所以现状下走 conn 时已经是 zero-copy，**前提是**
经过 tar 的中间 buffer 不破坏。tar header + body 的格式注定要先写 header
buffer 再写 body，body 那一段 `io.Copy` 在 *.tar 模式下仍然走 sendfile。
这块不需要为了 zero-copy 重做物理层。

## 6. 决策日志

- **不在 v1 做 SnapshotSlab**：现有 SST snapshot install 已覆盖。
- **保留命名占位但绑定 trigger 条件**：等 fsmeta export artifact 需求
  出现再开独立 RFC，避免"先做了再找 use case"。
- **ExportPayload 全量 buffer 标记为 follow-up**：跟 slab 重构无关，由
  raftstore/snapshot 自己 own。

## 7. 关联文档

- `2026-04-27-slab-substrate.md` — slab substrate 重定义，本文是其
  Phase 4 spike 的结论
- `2026-03-31-sst-snapshot-install.md` — 现有 SST snapshot install 设计
- `2026-04-25-snapshot-subtree-mvcc-epoch.md` — fsmeta SnapshotSubtree 的
  epoch 模型，未来 SnapshotSlab 的 trigger 来源
