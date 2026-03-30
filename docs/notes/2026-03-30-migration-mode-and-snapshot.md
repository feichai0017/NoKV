# 2026-03-30 migration 设计里最关键的不是命令，而是 mode 和 snapshot

这篇记录 migration 主线真正值钱的地方。表面上看，我们只是补了 `plan`、`init`、`expand` 这些命令；但真正让迁移成立的不是命令名，而是背后的状态协议。

---

## Context

NoKV 当前 migration 主线已经形成：

- `plan`
- `init`
- `serve`
- `expand`
- `remove-peer`
- `transfer-leader`

但如果只从 CLI 角度理解这套功能，很容易误判成“就是几条方便的运维命令”。

## Symptom

分布式迁移最容易犯的两个错误是：

1. **把迁移做成脚本串联**
2. **把 snapshot 当成一个模糊的大概念**

结果通常是：

- 目录状态不清楚
- 半迁移目录还能被普通 DB 打开
- 应用状态和 raft metadata 混在一起
- 增副本靠碰运气，不靠协议

## Investigation

当前 migration 真正成立，依赖两条主线：

### 1. workdir mode gate

`raftstore/mode` 现在有：

- `standalone`
- `preparing`
- `seeded`
- `cluster`

这不是 migration 私有状态，而是 workdir 运行形态协议。

最关键的一步是：

> `DB.Open()` 不再无条件认为目录永远是 standalone。

也就是说，一旦 workdir 进入 `preparing/seeded/cluster`，普通单机路径就不能继续把它当成简单本地 DB 来写。

### 2. snapshot 分层

NoKV 当前明确分成两种 snapshot：

- `raftstore/engine/snapshot.go`
  - raft durable metadata snapshot
- `raftstore/snapshot/snapshot.go`
  - logical region state snapshot

这条分层非常关键。

因为 migration 需要的不只是：

- index
- term
- conf state

它还需要：

- 某个 region key range 内的真实状态机内容

## Root cause

standalone 到 distributed 的桥接之所以难，不是因为“缺一个命令”，而是因为：

> 系统缺少一种能把已有 workdir 正式提升成 region state 的语义。

如果没有 mode gate，迁移中的 workdir 仍然可能被错误使用。  
如果没有逻辑 region snapshot，`expand` 和 seed bootstrap 就只能建立在不干净的目录复制或隐式状态假设上。

## Fix

当前 NoKV 的迁移协议是成立的，因为它做了三件对的事情：

1. 用 `mode` 把 workdir 生命周期写死。
2. 用 logical region snapshot 提供状态提升原语。
3. 用 `RaftAdmin` + membership orchestration 把 seed 扩成 replicated region。

换句话说，`migrate` 的命令只是入口，真正关键的是：

- 状态边界
- 快照边界
- 恢复边界

## Follow-ups

1. 下一阶段的提升不应该破坏这条分层  
   例如未来做 SST-based snapshot install，也应该继续保留：
   - raft metadata snapshot
   - region state snapshot
   这两层语义分离。
2. `mode` 不要继续塞杂项状态  
   保持它只是 workdir lifecycle contract。
3. 迁移文档里可以更明确强调：
   - CLI 是表层
   - protocol 才是本体
