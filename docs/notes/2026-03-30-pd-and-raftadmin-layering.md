# 2026-03-30 为什么 PD 和 RaftAdmin 不能混成一层

这篇记录当前 `PD` 与 `RaftAdmin` 的职责边界。这个边界看起来像“小命名问题”，实际上直接影响整个分布式系统的真相源和控制流是否会变脏。

---

## 背景

NoKV 现在同时有两层控制相关接口：

- `pd`
- `raftstore/admin`

两者都和 region、membership、leader、路由有关，很容易让人产生一个问题：

> 既然有 PD，为什么还需要 `RaftAdmin`？

## 问题

如果边界不清楚，系统很容易滑向两种坏形态：

1. **PD 越界写本地 truth**  
   control plane 直接变成 local state 的执行者。

2. **所有动作都塞进 store / CLI**  
   结果控制面、执行面、运维面全部混在一起。

这两种都不是长期可维护的架构。

## 分析

当前 NoKV 的分层本质上是：

### `RaftAdmin`

负责：

- `AddPeer`
- `RemovePeer`
- `TransferLeader`
- `RegionRuntimeStatus`

也就是：

> **leader store 上的执行面**

它的职责是把一个明确的 membership 操作落到真实的 region leader 上。

### `PD`

负责：

- route lookup
- allocator / TSO
- heartbeat 聚合
- cluster 视图
- 未来的调度和编排

也就是：

> **cluster 级 control plane**

## 根因

两者不能混成一层，根本原因是：

### control plane 不等于 execution plane

PD 最擅长的是：

- 看全局
- 做决策
- 提供目录服务
- 承担调度语义

但真正执行：

- conf change
- leader transfer
- snapshot install

这些动作的仍然是 region leader 所在的 store / peer runtime。

如果让 PD 直接越过这些路径写本地 state，会有三个问题：

1. 本地 truth 被破坏
2. 恢复路径和正常执行路径不一致
3. 分布式错误难以收敛，因为调度器开始兼做执行器

## 方案

当前 NoKV 的正确做法是：

1. 保留 `RaftAdmin` 作为 execution plane。
2. 保留 `PD` 作为 control plane。
3. 未来如果要做更高级 orchestrator，也应该是：
   - `PD` 决策
   - `RaftAdmin` 执行

而不是让 `PD` 直接篡改本地运行时。

## 后续

1. `RegionRuntimeStatus` 要保持克制  
   不要把它继续膨胀成万能 dump API。
2. `PD` 以后如果做 operator framework，应明确只编排，不直接改本地 truth。
3. 文档里应该长期强调：
   - `PD` 是 control plane
   - `RaftAdmin` 是 execution plane
