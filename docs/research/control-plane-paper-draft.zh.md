# NoKV 控制面论文草稿（第二版）

> 状态：中文论文初稿第二版。
> 定位：systems workshop / experience paper。
> 主张边界：不声称新的共识协议，不声称整体性能超越工业系统，不把所有工程细节都包装成创新点。本文聚焦一个更清晰的系统性质：**让承担 singleton duties 的 distributed KV coordinator 成为可重建服务层**。

## 标题候选

### 标题版本 A

**Rebuildable Coordinators：面向分布式 KV Singleton Services 的可重建控制面设计**

### 标题版本 B

**Replicate the Truth, Rebuild the Service：NoKV 的可重建控制器协议**

### 标题版本 C

**让带 Singleton Duties 的 Coordinator 可重建：NoKV 的 Rooted Control Plane**

## 摘要

现有分布式 KV 系统的控制面通常将 durable metadata truth、routing/scheduling service 与 singleton duties 绑定在同一个运维单元中。这样的设计工程上直接，也足够常见，但会带来两个长期问题。第一，控制面 service 实例崩溃、升级或替换时，truth 与 service 的失败域难以清晰分离。第二，一旦控制器承担全局唯一职责，例如 ID 分配、TSO 或 scheduler ownership，它的恢复就不再只是“加载一个读缓存”，而变成“如何在不依赖本地 durable state 的前提下，从正确的全局下界继续工作”。

本文研究的不是如何再发明一个新的 consensus protocol，而是一个更窄但更硬的问题：**一个带 singleton duties 的 distributed KV coordinator，能否不依赖 service-local durable recovery state，而从 rooted truth 被安全重建。** NoKV 给出的答案是肯定的，但前提是四个条件同时成立：durable truth 与 service/view 分离；coordinator ownership transfer 与 allocator lower bounds 通过同一 rooted metadata transition 原子表达；view staleness 通过显式 freshness contract 暴露给调用方；serialized rooted fence writes 通过 allocator window 退出稳态热路径。

本文的核心安全机制是 atomic lease-fence transition。`CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` 被编码为单条 rooted metadata event，在一次 durable append 中同时记录当前 coordinator holder 与 allocator lower bounds。新 coordinator 在接管时无需额外 RPC 即可继承 `IDFence` 与 `TSOFence`，从而消除 lease transfer 与 allocator 恢复之间的 TOCTOU 窗口。围绕这一机制，NoKV 将 `meta/root` 组织为 durable truth kernel，将 `coordinator` 组织为 rebuildable service/view layer，并通过 `RootToken`、`CatchUpState`、`DegradedMode` 与 `TailCatchUpAction` 将控制面 freshness 变成显式契约。

当前实现已覆盖 local、replicated 与 remote 三种 rooted authority 形态，并通过集成测试验证 separated deployment 下的 crash/recovery 语义。在 localhost 的 Apple M3 Pro 环境中，coordinator crash 后的新实例能够在约 95ms 平均、109ms P95 的时间内恢复发号；24 轮 chaos-style crash/reopen 测试保持 `AllocID` 全程严格单调，无重复、无回退。本文的贡献不在于一个“更快的控制器”，而在于一个可验证的系统性质：**对于承担 singleton duties 的 control-plane service，只要复制 truth 而不是复制整个 service，就可以把 coordinator 从难以恢复的大脑，收缩为 rooted truth 之上的可重建层。**

## 1. 引言

### 1.1 问题

分布式 KV 系统的数据平面往往已经具有明确的复制、提交与恢复边界，但控制面未必如此。工业系统里，cluster manager、leader lease、allocator、route service、scheduler 与 metadata persistence 经常被绑定在同一个逻辑整体中。这种设计不是错误，它只是偏向“复制整坨控制器”而不是“复制 truth、重建 service”。

当控制器只负责路由查询时，这种耦合代价还不算高；但当它承担 singleton duties，例如全局唯一 ID 分配、TSO 或调度所有权时，问题会变尖锐。系统不再只是回答“哪个 view 是最新的”，还必须回答：控制器崩溃后，新实例应该从哪里继续发号；旧实例最后一次持久化的 allocator 下界是什么；新实例如何证明自己接管的是一份与 ownership 一致的 truth，而不是若干个 loosely coupled 的 side effect。

这使得“恢复控制器”与“重建一个读视图”成为两件不同的事。一个普通 read view 可以通过 snapshot reload 重建；一个承担 singleton duties 的控制器则必须在重建时同时恢复 ownership、allocator lower bounds 与 rooted staleness 语义。本文关心的正是这个更硬的问题。

### 1.2 观察

本文的核心观察是：**带 singleton duties 的 coordinator 之所以难以重建，不是因为 view 不能重建，而是因为 ownership transfer 与 allocator progress 往往没有被放进同一份 durable truth。**

如果新 coordinator 的接管协议是：

1. 先获得 lease；
2. 再读取旧 allocator fence；
3. 再开始继续分配；

那么中间天然存在 TOCTOU 窗口。即便工程上可以通过安全余量、replay、额外 CAS 或时序假设把问题压过去，协议边界仍然不单一。系统能跑，但很难说清楚“接管到底从哪一刻开始是安全的”。

NoKV 的主张是：让这件事不再依赖一串 loosely coupled 的恢复动作，而依赖一条 rooted transition。

### 1.3 核心主张

本文主张如下系统性质：

> 一个承担 singleton duties 的 distributed KV coordinator，可以不依赖 service-local durable recovery state，而从 rooted truth 被安全重建。

NoKV 通过四个条件实现这一点：

- durable truth / service-view split
- atomic lease-fence transition
- explicit freshness contract
- hot-path windowing

这四个条件缺一不可。truth/service 不分离，则可重建性没有清晰含义；没有 atomic lease-fence transition，则 singleton duties 的接管不安全；没有 freshness contract，则分离后的 stale view 无法显式表达；没有 windowing，则 remote rooted authority 会被逐次 fence write 打进热路径。

### 1.4 贡献

本文的贡献集中在一个系统性质、一个核心安全机制与一组工程化实现结果。

第一，本文把 distributed KV control plane 的一个常被隐式处理的问题显式化：**带 singleton duties 的 coordinator 是否能从 rooted truth 重建，而不是依赖本地 durable 恢复状态。**

第二，本文提出 atomic lease-fence transition。`CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` 作为单条 rooted metadata event，在一次 durable append 中同时表达 ownership transfer 与 allocator lower bounds，从而把接管起点收敛成一个单一的 rooted truth 边界。

第三，本文实现并验证一套完整的 rooted control plane。`meta/root` 作为 durable truth kernel，`coordinator` 作为 rebuildable service/view layer，freshness 通过 `RootToken`、`CatchUpState`、`DegradedMode` 与 `TailCatchUpAction` 显式暴露。系统支持 local、replicated 与 remote 三种 rooted authority 接入方式。

第四，本文给出 crash/recovery、leader change 与 chaos-style monotonicity 的证据，展示这套协议不是文档化设计，而是已运行在真实代码上的可测试性质。

## 2. 背景与动机

### 2.1 复制整个控制器 vs 复制 truth

工业系统常见的控制面思路是复制整个控制器：服务逻辑、持久化元数据、singleton duties 与调度状态一起构成一个需要高可用的整体。这类设计当然可以工作，也确实在工业界被广泛采用。但它默认的恢复模型是：恢复一个“大脑”，而不是重建一个服务层。

本文不否认这种路径的有效性。相反，我们承认这通常是工程上更直接、也更成熟的方案。本文关心的是另一条路径：**只复制 truth，而把 service 变成可重建层。**

### 2.2 为什么普通 event sourcing 不够

表面上看，“truth + rebuildable view”很像 event sourcing 或 CQRS。但对于承担 singleton duties 的 coordinator，这个类比并不够。

普通 read view 的重建，只需要回答“状态是什么”；而 coordinator 的重建还必须回答“谁有权继续写”和“从哪里继续写”。前者是 snapshot replay 问题，后者是 ownership 与 lower bound 的一致性问题。这正是本文要解决的核心：**如何让一个拥有独占写职责的服务，也具备可重建性。**

### 2.3 设计目标

本文有四个设计目标。

- `coordinator` 崩溃、重启或替换时，不应模糊 durable truth。
- 新 holder 接管后，`AllocID` 与 `Tso` 不能重复、不能回退。
- stale view 必须显式暴露，而不是等到数据面失败后再由客户端反推。
- rooted authority 可以是 remote，但不能在稳态逐次分配路径上成为瓶颈。

## 3. 设计

### 3.1 Replicate the truth, rebuild the service

NoKV 将控制面拆成三层：

- `meta/root`：durable truth kernel
- `coordinator`：rebuildable service/view layer
- `raftstore`：执行平面

其中，只有 `meta/root` 保存 authoritative rooted truth；`coordinator` 持有 runtime view、allocator cache 与 scheduler runtime，但它不拥有 service-local durable recovery log。它的恢复起点来自 rooted truth，而不是来自自己持久化的一套额外状态。

### 3.2 Rooted metadata model

`meta/root` 不是通用字节流，而是 typed metadata log。它提供：

- checkpoint
- retained tail
- typed rooted events

这种设计的重要性在于：接管协议的关键字段不是散落在不同键值对里，而是通过 typed event 进入同一份 rooted truth。event kind 本身就是协议边界的一部分。

### 3.3 Atomic lease-fence transition

本文的核心安全机制是：

- `CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)`

这不是一个单纯的 lease 记录，而是一个完整的 rooted transition。它同时表达：

- 当前 holder 是谁
- lease 到什么时候过期
- 该 holder 接管时 allocator 的安全下界在哪里

这使新 coordinator 的恢复步骤收敛为：

1. campaign / renew rooted coordinator lease；
2. 从 returned lease 或 rooted snapshot 中读取 `IDFence` / `TSOFence`；
3. 直接 fence 本地 allocator，并继续服务。

系统不再需要“拿到 lease 之后，再单独去别处找 allocator 下界”。ownership transfer 与 allocator lower bound 出自同一条 rooted truth，这就是 TOCTOU 窗口被结构性消除的原因。

### 3.4 Explicit freshness contract

一旦 truth 与 view 被分离，staleness 就不该继续是隐式的。NoKV 通过如下字段把 freshness 提升为 route lookup contract 的一部分：

- `ServedRootToken`
- `CurrentRootToken`
- `RootLag`
- `CatchUpState`
- `DegradedMode`
- `Freshness`

调用方可以显式选择：

- `STRONG`
- `BOUNDED`
- `BEST_EFFORT`

因此，stale view 不再只能通过 `EpochNotMatch` 或 `NotLeader` 这样的数据面错误后验暴露，而是在控制面查询时直接被量化与返回。

### 3.5 Tail catch-up as rooted contract

为了让 view consumer 不必自行判断“该 reload 还是 install bootstrap”，NoKV 在 rooted `VirtualLog` 边界定义 `TailCatchUpAction`。一次 tail advance 被归类为：

- `Idle`
- `RefreshState`
- `AcknowledgeWindow`
- `InstallBootstrap`

这不是主创新点，但它让 freshness contract 与 rebuildable coordinator 站在同一套 rooted protocol 之上，而不是分散在若干 package-local 判断中。

### 3.6 Allocator windowing

如果每次 `AllocID` / `Tso` 都要远程推进 rooted fence，remote authority 会直接落入热路径，整个设计即便正确也不可用。因此 NoKV 使用 allocator window：

- 窗口内分配只在内存前进
- 只有窗口耗尽才推进 rooted fence

allocator window 不是论文主创新点，但它是这套体系具备工程可行性的必要条件。

## 4. 实现

### 4.1 `meta/root`

`meta/root` 提供 rooted `VirtualLog` contract，包括：

- checkpoint load/save
- committed tail read
- append committed event
- compaction
- bootstrap install
- tail observe / wait / subscribe

在状态机层，`CoordinatorLease` 被保存在 compact root state 中，并通过 `applyCoordinatorLeaseToState` 同时更新：

- `CoordinatorLease`
- `IDFence`
- `TSOFence`

因此 checkpoint restore 与 tail replay 都天然继承这三个字段的一致性关系。

### 4.2 `coordinator`

`coordinator` 通过统一的 `RootStore` 接入 rooted authority，并暴露：

- `GetRegionByKey`
- `AllocID`
- `Tso`
- scheduler ownership

`AllocID` 与 `Tso` 的正确性依赖两层约束：

- rooted write path 可写
- 当前实例持有有效 `CoordinatorLease`

在当前实现里，lease renew 具有 fast path；有效 lease 不会在每次请求上都进入重型写锁路径。lease renew 与 graceful release 之后，本地 allocator 会直接从 returned lease 的 `IDFence` / `TSOFence` 更新，而不是强制 reload 整个 rooted snapshot。

### 4.3 三种 rooted authority

NoKV 当前支持三种 rooted authority 形态：

- local：单节点文件 authority
- replicated：co-located 的 rooted raft authority
- remote：gRPC access layer over local/replicated authority

这三种模式共享同一 `RootStore` contract。remote 不是第三种 truth kernel，而是 rooted authority 的访问层。这个 distinction 在论文里必须明确，否则 reviewer 会误以为 remote 自己也是一套新的 metadata substrate。

### 4.4 当前实现状态

与本文主张直接相关的机制已经落地：

- `CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` typed event
- rooted snapshot apply 中的 lease/fence 原子恢复
- lease renew loop 与 clock-skew buffer
- explicit freshness contract
- `TailCatchUpAction`
- remote rooted API 与 leader redirect
- allocator window
- crash/recovery、leader change 与 chaos monotonicity integration tests

## 5. 评估

### 5.1 评估问题

本文不试图证明 NoKV 整体性能优于工业系统。评估只回答三个更窄的问题：

1. coordinator 是否真的可以在没有 service-local durable recovery state 的情况下重复 crash/reopen 且不回退；
2. ownership transfer 与 allocator lower bound 是否真的由 rooted truth 安全衔接；
3. remote rooted authority 在 windowing 保护下是否不会落入稳态热路径。

### 5.2 环境与边界

当前结果来自 Apple M3 Pro localhost 环境。控制面 benchmark 覆盖：

- in-process local/root access
- localhost TCP remote access
- process-separated remote root
- process-separated etcd CAS baseline

这些数字足以支撑 workshop 级别的因果论证，但不支撑大规模 multi-host 绝对性能结论。本文只把它们解释为“机制形状”和“恢复路径证据”，而不是生产级吞吐宣称。

### 5.3 Crash recovery correctness

`TestSeparatedModeCoordinatorCrashAndRecoveryPreservesAllocatorFence` 验证了最小接管正确性：

- old coordinator 分配 ID 后 crash
- new coordinator 用同一 stable `coordinator-id` 接管
- 新的 `AllocID` 严格大于旧实例最后返回值
- rooted `IDFence` 与 `CoordinatorLease.IDFence` 不回退

这直接证明，allocator lower bound 的恢复起点来自 rooted truth，而不是 service-local state。

### 5.4 Leader change safety

`TestReplicatedStoreCoordinatorLeaseFenceSurvivesLeaderChange` 验证了 rooted authority leader 切换后：

- `CoordinatorLease` 不丢失
- `IDFence` / `TSOFence` 不回退

这说明本文的安全主张不依赖某个固定 leader，而依赖 typed rooted metadata transition。

### 5.5 Chaos monotonicity

为了验证“可重建 coordinator”是一个性质而不是单次演示，本文增加 chaos-style integration test：

- 反复 crash/reopen 同一 stable coordinator
- 每轮分配一个固定 batch 的 `AllocID`
- 检查新一轮起点严格大于上一轮终点
- 检查 rooted `IDFence` 持续单调
- 检查新一轮 lease campaign 继承上一轮 fence

在当前 localhost 环境下，24 轮测试、每轮 256 个 ID 的结果为：

- `first=1`
- `last=230256`
- `avg_recovery≈95.8ms`
- `p50≈94.8ms`
- `p95≈109.0ms`

全程 `AllocID` 严格单调，无重复、无回退。这个测试比单次 recovery latency 更重要，因为它展示的是一种 recovery model，而不是一次偶然成功的重连。

### 5.6 Hot-path viability

当前 benchmark 显示，在默认窗口配置下：

- process-separated NoKV remote rooted authority 的稳态 `AllocID` 延迟约为 `1.0us`
- process-separated etcd CAS baseline 的稳态延迟约为 `0.4us`

当窗口退化为每次分配都推进 fence 时：

- NoKV remote rooted path 上升到约 `9.8ms`
- etcd CAS baseline 上升到约 `3.9ms`

这些数字说明的不是“NoKV 比 etcd 快”或“更慢”，而是：**serialized remote fence write 才是主要代价，windowing 才是让 rebuildable coordinator 可用的工程条件。** NoKV 的单次 remote rooted write 更重，因为它承载的是 typed rooted semantics，而不是单一 CAS key。

### 5.7 小结

当前证据支持以下结论：

- NoKV 已经实现了一个不依赖 service-local durable state 的 rebuildable coordinator。
- atomic lease-fence transition 让 singleton duties 的接管起点变得单一且可验证。
- chaos monotonicity 证明这不是一次性的恢复技巧，而是可重复的系统性质。
- windowing 使 remote rooted truth 在稳态下退出热路径，从而让该设计具备工程可用性。

## 6. 讨论

### 6.1 这不是“完全无状态的 coordinator”

本文不能声称 coordinator 是严格意义上的 stateless service。它仍然持有：

- runtime cluster view
- scheduler state
- allocator windows
- diagnostics state

更准确的说法是：**它不依赖 service-local durable recovery state 来恢复 singleton duties。** 这是一个更严格、也更可 defend 的说法。

### 6.2 这也不是“新的 consensus protocol”

本文没有改变 root authority 的复制协议，也不提供新的 quorum algorithm。本文的贡献是：在既有 rooted truth 之上，给 control-plane recovery 提供一种更单一的协议边界。

### 6.3 为什么工业界不一定这样做

工业界并不是“不知道这种分离”，而是往往更偏好复制整个控制器：

- 系统边界更直接
- 旧方案已经够用
- 额外的 freshness/lease/window 协议复杂度未必值得

所以本文不主张“NoKV 一定更适合所有系统”。本文主张的是：**如果你想把 coordinator 从 authority 降级成可重建 service 层，那么这些协议条件是必须回答的。**

### 6.4 当前局限

当前工作仍有边界：

- benchmark 主要来自 localhost
- 尚未给出大规模 multi-host 结果
- freshness contract 的“减少无效 stale-route round trip”价值还缺专门对照实验
- 本文展示的是 workshop 级别的系统性质与工程验证，而不是 full-paper 级别的大规模性能结论

## 7. 相关工作

### 7.1 Delos

Delos 提供了 service logic 与 log substrate 解耦的 `VirtualLog` 思路，是 NoKV `meta/root` 的直接启发来源。但 Delos 并不直接处理带 singleton duties 的 rebuildable coordinator 问题，也不提供 atomic lease-fence transition。NoKV 建立在 Delos 风格的 rooted truth abstraction 之上，但本文的贡献不在 abstraction 本身，而在于把 singleton recovery 收敛成单一 rooted transition。

### 7.2 TiKV / PD

PD 代表了更典型的“复制整个控制器”路径。它当然可以工作，也解决了 allocator failover 问题，但其恢复通常由多个逻辑步骤、外部持久化和安全余量共同完成。本文不声称 NoKV 全面优于 PD，只强调一种不同的 recovery model：从 rooted truth 重建 service，而不是恢复一个携带 durable state 的 controller。

### 7.3 Kafka KRaft

KRaft 具有 typed metadata records，也具备 metadata quorum 与 controller 的分工，但 controller 与 metadata quorum 紧耦合，并不以 rebuildable service/view layer 为核心抽象。KRaft 与本文在 typed metadata 上有相似性，但没有提出“让带 singleton duties 的 coordinator 从 rooted truth 重建”的问题设定。

### 7.4 FoundationDB

FoundationDB 有 coordinator 角色，也有清晰的 cluster coordination 设计，但它并不以完整 typed rooted metadata log 的形式暴露控制面 truth，也不以本文这种 atomic lease-fence transition 组织 singleton recovery。本文与其共享的是“truth 与 service 不必完全同构”的系统思想。

## 8. 结论

本文不是在提出一个全新的分布式控制面范式，也不是在宣称一个全面优于工业系统的架构。本文做的事情更窄，但也更具体：它把带 singleton duties 的 distributed KV coordinator 的恢复问题，从“恢复一个复杂的大脑”改写成“从 rooted truth 重建一个 service 层”。

NoKV 的核心答案是：复制 truth，而不是复制整个 service；让 ownership transfer 与 allocator lower bounds 由同一 rooted transition 原子表达；让 staleness 通过显式 freshness contract 暴露；让 remote rooted authority 通过 windowing 退出稳态热路径。由此，coordinator 可以失去“本地 durable 状态载体”的身份，而保留“可重建服务层”的角色。

如果本文只留下一个核心结论，那就是：

> **对于承担 singleton duties 的 distributed KV control plane，真正关键的不是把 coordinator 拆成几个进程，而是能否让它在失去本地 durable recovery state 之后，仍然从一份单一的 rooted truth 被安全重建。**
