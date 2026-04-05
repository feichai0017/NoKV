# 2026-03-30 分布式测试为什么不能只靠黑盒，以及 failpoint 该怎么克制

这篇记录最近一轮分布式测试体系收口后的判断。重点不是“case 变多了”，而是测试开始按边界和故障模型分层，而不是只靠少量 happy-path 集成测试。

---

## 背景

最近这轮测试增强之后，NoKV 现在已经有：

- package-level protocol tests
- node-local integration tests
- multi-node deterministic integration
- restart / recovery tests
- transport chaos
- context propagation coverage
- publish-boundary failpoints
- `testcluster` 集群测试基础设施

## 问题

分布式系统如果只靠两类测试，很快就会失真：

1. **只靠单测**
   - 证明不了系统路径成立
2. **只靠黑盒集成**
   - 很难稳定打中 publish / persist / send 这些真正危险的边界

如果没有中间层，最后通常会出现两种结果：

- 测试很多，但抓不到真实故障点
- 或者为了命中中间态，把生产代码改得很脏

## 分析

当前 NoKV 的收法是两条线一起做：

### 1. `testcluster`

`raftstore/testcluster` 解决的是：

- 起 PD
- 起多个节点
- block/unblock peer
- restart node
- wait leader / hosted / scheduler mode

它的价值不是“让测试看起来优雅”，而是把多节点环境搭建从场景文件里抽出来，避免 migration test 变成脚本集合。

### 2. failpoint

`raftstore/failpoints` 现在打在这些地方：

- `AfterReadyAdvanceBeforeSend`
- `BeforeTransportSendRPC`
- `AfterSnapshotApplyBeforePublish`
- migration init 的 publish-boundary 阶段

这些点的共同特点是：

- 都是生命周期边界
- 都很难靠黑盒稳定命中
- 一旦出错，最容易留下半状态

## 根因

很多人对 failpoint 的直觉是“侵入性太强”。这个担心没错，但问题不是“要不要 failpoint”，而是：

> failpoint 是否只打在真正高价值、黑盒难覆盖、资源注入又不够精确的边界上。

只要打点开始进入：

- 业务分支
- 模糊条件
- 到处散落的测试开关

那它就会迅速退化成脏代码。

## 方案

当前 NoKV 的正确方向是：

1. 黑盒 cluster integration 负责系统级收敛。
2. `testcluster` 负责多节点环境和干扰控制。
3. failpoint 只负责命中高价值 publish / persist / send 边界。

也就是说：

> 不是“只靠 failpoint”，也不是“完全不用 failpoint”，而是分层使用。

## 后续

1. 新增 failpoint 时必须回答三个问题：
   - 为什么黑盒 integration 打不到？
   - 为什么 transport / FS fault injection 不能替代？
   - 它命中的具体生命周期边界是什么？
2. `testcluster` 要继续保持克制  
   它应该是 cluster harness，不要长成测试万能箱。
3. 后面可以继续补：
   - 更随机化的 transport chaos
   - 更强的 invariant helper
   - 更深的 multi-region / lock recovery 交错场景
