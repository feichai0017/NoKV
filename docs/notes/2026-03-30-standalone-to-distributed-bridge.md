# 2026-03-30 standalone 到 distributed 的桥接为什么值钱

这篇记录不是在描述某个单点实现，而是在解释 NoKV 目前最有产品价值的一条主线：**单机和分布式不是两套割裂系统，而是同一套数据面在不同运行形态下的演进。**

---

## Context

项目同时具备：

- `DB` 单机数据面
- `raftstore` 分布式执行层
- `pd` 控制面
- `migrate` 独立迁移协议

看起来像是在做两个系统，但当前的设计目标不是“单机一个产品、分布式另一个产品”，而是让两者共享同一套底层状态。

## Symptom

很多存储项目一旦走到分布式阶段，就会遇到一个非常实际的问题：

1. 单机阶段先跑得起来。
2. 后面需要扩展时，发现分布式实际上是另一套系统。
3. 最终只能：
   - 重新建集群
   - dump/import
   - 重新做工具链
   - 背一套新的运维心智

这会直接把“从小到大演进”的故事打断。

## Investigation

NoKV 当前这条桥接路径成立，核心靠的是几个设计决定同时为真：

1. `DB` 仍然是底层共享数据面  
   不是把 standalone 数据导出后再导入另一种存储格式。

2. `raftstore/localmeta` 与 `manifest` 分开  
   引擎元数据和分布式恢复元数据没有继续混在一起。

3. `raftstore/snapshot` 引入了逻辑 region snapshot  
   把“应用状态快照”和“raft durable snapshot metadata”分层。

4. `raftstore/mode` 把 workdir lifecycle 写成协议  
   `standalone`、`preparing`、`seeded`、`cluster` 不只是 CLI 状态，而是库级 gate。

5. `migrate` 把桥接过程显式化  
   `plan -> init -> serve -> expand` 不是脚本约定，而是正式控制流。

## Root cause

真正让单机和分布式难以衔接的，不是“技术栈不同”，而是**状态语义不同**。

单机系统通常只需要：

- engine manifest
- WAL
- SST
- VLog

分布式系统还需要：

- region identity
- peer identity
- raft durable state
- local recovery metadata
- control-plane view

如果这些额外语义没有被设计成可提升、可恢复、可验证的状态层，那么单机到分布式就只剩下粗暴搬运数据。

## Fix

NoKV 当前的修法，本质上是把“增加 distributed identity”这件事协议化了：

1. 先用 `mode` 锁住 workdir 生命周期。
2. 用 `migrate init` 为原 workdir 写入 seed region 语义。
3. 用逻辑 region snapshot 提供状态提升原语。
4. 用 `serve` 接管 seeded workdir 进入 cluster 语义。
5. 用 `expand/remove-peer/transfer-leader` 完成 membership reshape。

这条线的意义是：

> NoKV 的分布式能力不是独立产品，而是同一套系统的放大形态。

## Follow-ups

1. 后面如果继续推进，这条桥最该加强的是：
   - SST-based snapshot install
   - 更成熟的 PD orchestration
   - 更强的 migration observability
2. 这条设计值得在项目介绍里长期突出，因为它比“又做了一个 KV”更有辨识度。
