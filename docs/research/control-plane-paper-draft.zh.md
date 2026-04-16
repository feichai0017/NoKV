# Rebuildable Coordinators：将接管关键状态根植于单一 Truth Boundary 的分布式 KV 控制面设计

> 状态：中文完整 workshop 论文草稿。
> 定位：systems workshop / experience paper。
> 主张边界：不声称新的共识协议，不声称整体性能超越工业系统，不把通用工程实践包装成研究创新。本文聚焦一个更窄但更硬的系统性质：**让承担 singleton duties 的 distributed KV coordinator 成为 rooted truth 之上的可重建服务层。**

## 摘要

许多分布式 KV / 数据库系统会在同一个 control-plane authority boundary 内同时承载 metadata truth、routing/scheduling service 与 singleton duties。TiKV/PD、Kafka KRaft 与 YugabyteDB 的 master/catalog 路径都属于这一类：durable metadata truth 与 controller service 在同一运维单元内共同演化。也存在重要反例，例如 FoundationDB 把 coordinators 与 cluster controller 分离，而 CockroachDB 则根本不采用集中式 coordinator。本文讨论的不是“所有现代系统都融合 truth 与 service”，而是更窄的一类系统问题：**当 control-plane service 还承担全局唯一职责，例如 ID 分配、TSO 或 scheduler ownership 时，truth/service split 之后的 takeover-critical state 应如何定义。**

对于这类系统，真正困难的不是“把 view 重新加载出来”，而是“如何在没有 service-local durable state 的前提下，从正确的全局下界继续工作”。换句话说，真正困难的不是重建 view，而是把接管所需的关键状态收敛进一个单一、可验证的 authority boundary。

本文研究的不是如何再发明一个新的 consensus protocol，而是一个更窄但更可验证的问题：**一个带 singleton duties 的 distributed KV coordinator，能否不依赖 service-local durable recovery state，而从 rooted truth 被安全重建。** NoKV 给出的答案是肯定的，但前提是四个问题被同时回答：durable truth 与 service/view 必须分离；coordinator ownership transfer 与 allocator lower bounds 必须通过同一 rooted metadata transition 原子表达；view staleness 必须通过显式 freshness contract 暴露给调用方；serialized rooted fence writes 必须通过 allocator window 退出稳态热路径。

本文的核心安全机制是 atomic lease-fence transition。`CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` 被编码为单条 rooted metadata event，在一次 durable append 中同时记录当前 coordinator holder 与 allocator lower bounds。新 coordinator 在接管时无需额外 RPC 即可继承 `IDFence` 与 `TSOFence`，从而消除 lease transfer 与 allocator 恢复之间的 cross-authority TOCTOU 窗口。围绕这一机制，NoKV 将 `meta/root` 组织为 durable truth kernel，将 `coordinator` 组织为 rebuildable service/view layer，并通过 `RootToken`、`CatchUpState`、`DegradedMode` 与 `TailCatchUpAction` 将控制面 freshness 提升为显式契约。更准确地说，当前 prototype 已经给出一个 **rooted takeover certificate 的最小雏形**：ownership 与 allocator lower bounds 已经进入同一 rooted transition，但尚未被完全对象化为独立 protocol artifact。

当前实现已经覆盖 local、replicated 与 remote 三种 rooted authority 形态，并通过集成测试验证 separated deployment 下的 crash/recovery 语义。在 localhost 的 Apple M3 Pro 环境中，coordinator crash 后的新实例能够在约 `98.1ms` 平均、`103.0ms` P95 的时间内恢复发号；24 轮 chaos-style crash/reopen 测试保持 `AllocID` 全程严格单调，无重复、无回退；不同 holder 的 contested failover 测试验证了 lease 过期后的安全接管与 fence 继承。本文的贡献不在于一个“更快的控制器”，而在于一个可验证的系统性质：**对于承担 singleton duties 的 control-plane service，只要接管关键状态被根植于同一份 rooted truth，就可以把 coordinator 从难以恢复的 durable controller，收缩为 rooted truth 之上的可重建层。**

## 1. 引言

### 1.1 问题背景

分布式 KV 系统的数据平面通常已经具有清晰的复制、提交与恢复边界，但控制面未必如此。许多工业系统会在同一个控制器体系里同时承载 cluster manager、leader lease、allocator、route service、scheduler 与 metadata persistence。这种设计不是错误，它只是偏向“复制整坨控制器”而不是“复制 truth、重建 service”。

当控制器只负责路由查询时，这种耦合代价还不算高；但当它承担 singleton duties，例如全局唯一 ID 分配、TSO 或调度所有权时，问题会迅速尖锐。系统不再只是回答“哪个 view 是最新的”，还必须回答：控制器崩溃后，新实例应该从哪里继续发号；旧实例最后一次 rooted allocator 下界是什么；新实例如何证明自己接管的是一份与 ownership 一致的 truth，而不是若干个 loosely coupled 的 side effect。

这使得“恢复控制器”与“重建一个读视图”成为两件不同的事。一个普通 read view 可以通过 snapshot reload 重建；一个承担 singleton duties 的控制器则必须在重建时同时恢复 ownership、allocator lower bounds 与 rooted staleness 语义。本文关心的正是这个更硬的问题。

### 1.2 核心观察

本文的核心观察是：**带 singleton duties 的 coordinator 之所以难以重建，不是因为 view 不能重建，而是因为 ownership transfer 与 allocator progress 往往没有被放进同一份 durable truth。**

如果新 coordinator 的接管协议是：

1. 先获得 lease；
2. 再读取旧 allocator fence；
3. 再开始继续分配；

那么中间天然存在 TOCTOU 窗口。即便工程上可以通过安全余量、replay、额外 CAS 或时序假设把问题压过去，协议边界仍然不单一。系统能跑，但很难说清楚“接管到底从哪一刻开始是安全的”。

NoKV 的主张是：让这件事不再依赖一串 loosely coupled 的恢复动作，而依赖一条 rooted transition。

如果把这个观察再压缩一层，真正值得研究的不是“lease 存在哪里”，而是：**接管关键状态能否被压缩成一个单一 rooted object 所表达的边界。** 当前实现已经让 ownership transfer 与 allocator lower bounds 进入同一 transition；下一步最强的升级方向，则是把这条 transition 明确提升成一个 takeover certificate，而不是继续停留在若干字段的工程组合上。

### 1.3 论文主张

本文主张如下系统性质：

> 一个承担 singleton duties 的 distributed KV coordinator，可以在不依赖 service-local durable recovery state 的前提下，从一份单一的 rooted truth 中安全重建。

NoKV 通过四个条件实现这一点：

- durable truth / service-view split
- atomic lease-fence transition
- explicit freshness contract
- hot-path windowing

更精确地说，前两个条件定义 correctness boundary，后两个条件定义 usability boundary。truth/service 不分离，则可重建性没有清晰含义；没有 atomic lease-fence transition，则 singleton duties 的接管不安全；没有 freshness contract，则分离后的 stale view 无法显式表达；没有 windowing，则 remote rooted authority 虽然仍然正确，但会被逐次 fence write 打进热路径而失去工程可用性。本文因此不再使用“缺一不可”的笼统表述，而明确区分 correctness 条件与可用性条件。与此同时，本文也主动收窄 claim：当前已完成的是 rooted transition、freshness contract 与 windowed serving 的最小闭环；更强的 `takeover certificate`、`lease-backed allocation budget` 与 typed degraded-state protocol 仍属于下一步可升级方向，而不是本文已经完成的贡献。

### 1.4 贡献

本文的贡献集中在一个系统性质、一个核心安全机制与一组工程化实现结果。

第一，本文把 distributed KV control plane 的一个常被隐式处理的问题显式化：**带 singleton duties 的 coordinator 是否能从 rooted truth 重建，而不是依赖本地 durable 恢复状态。**

第二，本文提出 atomic lease-fence transition。`CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` 作为单条 rooted metadata event，在一次 durable append 中同时表达 ownership transfer 与 allocator lower bounds，从而把接管起点收敛成一个单一的 rooted truth 边界。当前稿件只主张这是一条 **minimal rooted takeover substrate**，而不把它过度包装成已经完整对象化的 certificate protocol。

第三，本文实现并验证一套完整的 rooted control plane。`meta/root` 作为 durable truth kernel，`coordinator` 作为 rebuildable service/view layer，freshness 通过 `RootToken`、`CatchUpState`、`DegradedMode` 与 `TailCatchUpAction` 显式暴露。系统支持 local、replicated 与 remote 三种 rooted authority 接入方式。

第四，本文给出 crash/recovery、不同 holder 的 contested failover、leader change 与 chaos-style monotonicity 的证据，展示这套协议不是文档化设计，而是已运行在真实代码上的可测试性质。

## 2. 背景与动机

### 2.1 复制整个控制器 vs 复制 truth

许多工业系统会选择复制整个控制器：服务逻辑、持久化元数据、singleton duties 与调度状态一起构成一个需要高可用的整体。这类设计当然可以工作，也确实在工业界被广泛采用。但它默认的恢复模型是：恢复一个“大脑”，而不是重建一个服务层。

本文不否认这种路径的有效性。相反，我们承认这通常是工程上更直接、也更成熟的方案。本文关心的是另一条路径：**只复制 truth，而把 service 变成可重建层。** 这条路径真正困难的部分，不在于“view 能不能 reload”，而在于“takeover-critical state 能不能不跨边界地恢复”。

### 2.2 为什么普通 event sourcing 不够

表面上看，“truth + rebuildable view”很像 event sourcing 或 CQRS。但对于承担 singleton duties 的 coordinator，这个类比并不够。

普通 read view 的重建，只需要回答“状态是什么”；而 coordinator 的重建还必须回答“谁有权继续写”和“从哪里继续写”。前者是 snapshot replay 问题，后者是 ownership 与 lower bound 的一致性问题。这正是本文要解决的核心：**如何让一个拥有独占写职责的服务，也具备可重建性。**

### 2.3 设计目标

本文有四个设计目标。

- `coordinator` 崩溃、重启或替换时，不应模糊 durable truth。
- 新 holder 接管后，`AllocID` 与 `Tso` 不能重复、不能回退。
- stale view 必须显式暴露，而不是等到数据面失败后再由客户端反推。
- rooted authority 可以是 remote，但不能在稳态逐次分配路径上成为瓶颈。

### 2.4 失败模型与非目标

本文假设 rooted authority 本身提供持久化与复制语义；coordinator 可以崩溃、重启、替换，也可以在 remote 模式下与 rooted authority 解耦部署。本文不处理 Byzantine 对手，不研究新的 quorum protocol，也不声称 route lookup 的 `STRONG` 模式提供一般意义上的 linearizability。本文关注的是更窄的性质：**coordinator 的 singleton recovery 不依赖 service-local durable recovery state。**

## 3. 设计

### 3.1 Replicate the truth, rebuild the service

NoKV 将控制面拆成三层：

- `meta/root`：durable truth kernel
- `coordinator`：rebuildable service/view layer
- `raftstore`：执行平面

其中，只有 `meta/root` 保存 authoritative rooted truth；`coordinator` 持有 runtime view、allocator cache 与 scheduler runtime，但它不拥有 service-local durable recovery log。它的恢复起点来自 rooted truth，而不是来自自己持久化的一套额外状态。

这个拆分的目标不是把组件拆得更多，而是把协议边界收得更单一。对于 route lookup、ID allocation 与 TSO 来说，真正 authoritative 的内容只有 rooted truth；而 coordinator 的职责是消费这份 truth，形成可服务的 view，并持有一个受约束的 singleton ownership。

### 3.2 Rooted metadata model

`meta/root` 不是通用字节流，而是 typed metadata log。它提供：

- checkpoint
- retained tail
- typed rooted events

这种设计的重要性在于：接管协议的关键字段不是散落在不同键值对里，而是通过 typed event 进入同一份 rooted truth。event kind 本身就是协议边界的一部分。

在 NoKV 中，control-plane state 不是一堆松散的 `key -> value`，而是一个可重放、可压缩、可 install bootstrap 的 rooted state machine。对本文关心的问题来说，重点不是“event-sourcing 是否时髦”，而是：**singleton recovery 的关键信息是否被收敛进同一个状态机边界。**

### 3.3 Atomic lease-fence transition

本文的核心安全机制是：

- `CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)`

它不是“先拿 lease、再到别处读 fence”的组合动作，而是一个完整的 rooted transition。它同时表达：

- 当前 holder 是谁
- lease 到什么时候过期
- 该 holder 接管时 allocator 的安全下界在哪里

这使新 coordinator 的恢复步骤收敛为：

1. campaign / renew rooted coordinator lease；
2. 从 returned lease 或 rooted snapshot 中读取 `IDFence` / `TSOFence`；
3. 直接 fence 本地 allocator，并继续服务。

系统不再需要“拿到 lease 之后，再跨另一个 authority 或另一组键单独去找 allocator 下界”。ownership transfer 与 allocator lower bound 出自同一条 rooted truth，这就是 TOCTOU 窗口被结构性消除的原因。本文因此不把 recovery 描述成字面意义上的 “O(1) step”，而把它描述成：**recovery 所需的 takeover-critical information 来自同一份 rooted authority，无需跨多个独立 authority 组合恢复。**

从研究边界上看，这条 rooted transition 也可以被理解为 **takeover certificate 的最小形式**：它已经足以绑定 holder、expiry 与 allocator lower bounds，并为 same-holder rebuild 与 contested failover 提供共同起点。本文当前还没有把 certificate 完全对象化，例如显式 root version、issued term、budget epoch 等字段仍未进入统一协议对象；但这正是当前实现最自然、也最值得继续深挖的升级方向。

### 3.4 从协议视角看 takeover

为了更具体地说明本文的协议边界，可以把一次 takeover 分成三种场景：

1. **same-holder rebuild**：同一个 `coordinator-id` crash/reopen 后重新 campaign；
2. **contested failover**：旧 holder 崩溃但未 release，新 holder 在 lease 过期后接管；
3. **root leader change**：rooted authority 的 leader 切换，但 compact state 中的 lease/fence 不变。

三种场景的共同点在于：allocator lower bound 始终不来自 service-local durable state，而来自 rooted snapshot 或 rooted lease transition。不同之处在于 availability 边界：same-holder rebuild 更接近“快速重建”；contested failover 则受到 lease TTL 的显式限制；leader change 主要考察 rooted replication 是否保持 compact state 单调。

### 3.5 Explicit freshness contract

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

因此，stale view 不再只能通过 `EpochNotMatch` 或 `NotLeader` 这样的数据面错误后验暴露，而是在控制面查询时直接被量化与返回。这里的 `STRONG` 也需要精确定义：它表示“当前 coordinator 没有已知 rooted lag，且由当前 rooted leader service”，而不是一般意义上的 linearizable route lookup。

### 3.6 Tail catch-up as rooted contract

为了让 view consumer 不必自行判断“该 reload 还是 install bootstrap”，NoKV 在 rooted `VirtualLog` 边界定义 `TailCatchUpAction`。一次 tail advance 被归类为：

- `Idle`
- `RefreshState`
- `AcknowledgeWindow`
- `InstallBootstrap`

这不是主创新点，但它让 freshness contract 与 rebuildable coordinator 站在同一套 rooted protocol 之上，而不是分散在若干 package-local 判断中。对 workshop 论文来说，这一点的价值不在 novelty，而在可审查性：reader 能明确看到 coordinator 的 catch-up 行为由哪个协议边界驱动。进一步说，本文把 freshness 当成 **serving contract** 的一部分，而不是单纯的内部 health bit；当前实现已具备这个 contract 的骨架，但还没有把 `AUTHORITATIVE / BOUNDED_STALE / DEGRADED` 这类更强的 client-visible serving classes 完全对象化。

### 3.7 Allocator windowing

如果每次 `AllocID` / `Tso` 都要远程推进 rooted fence，remote authority 会直接落入热路径，整个设计即便正确也不可用。因此 NoKV 使用 allocator window：

- 窗口内分配只在内存前进
- 只有窗口耗尽才推进 rooted fence

allocator window 不是论文主创新点。它不提供新的 correctness 保证，但它决定 remote rooted truth 能否退出稳态热路径，因此是这套体系具备工程可行性的必要条件。

更具体地说，windowing 的作用不是改变安全边界，而是改变代价形状：从“每次分配都命中 rooted write path”变成“多数分配只在本地 allocator 上前进”。这也是本文将它归类为 usability condition 而非 correctness condition 的原因。下一步更强的表达方式，则是把 window 从“工程优化”进一步上升为 **lease-backed allocation budget**：也就是把当前 holder 在当前 rooted lease 下被授权消耗的一段 allocator 预算显式协议化。

### 3.8 协议伪代码

为了避免本节停留在口头描述，下面给出与 NoKV 当前实现一一对应的协议伪代码。它们不是抽象算法，而是对实际代码边界的压缩表达。

#### 3.8.1 Coordinator lease campaign

```text
function EnsureCoordinatorLease():
    now = Clock.Now()
    if CurrentLease.usableBy(self, now):
        return OK

    idFence  = LocalIDAllocator.CurrentFence()
    tsoFence = LocalTSOAllocator.CurrentFence()

    lease = Root.CampaignCoordinatorLease(
        holder   = self.id,
        expires  = now + leaseTTL,
        idFence  = idFence,
        tsoFence = tsoFence,
    )

    LocalLease = lease
    LocalIDAllocator.FenceTo(lease.IDFence)
    LocalTSOAllocator.FenceTo(lease.TSOFence)
    return OK
```

这个过程对应当前代码中的 `ensureCoordinatorLease()`，其关键点不是“拿到 lease”，而是 campaign 返回值本身已经携带了接管所需的 allocator 下界。

#### 3.8.2 Rooted state apply

```text
function ApplyCoordinatorLease(event):
    state.CoordinatorLease = {
        HolderID = event.HolderID,
        Expires  = event.Expires,
        IDFence  = event.IDFence,
        TSOFence = event.TSOFence,
    }

    if event.IDFence > state.IDFence:
        state.IDFence = event.IDFence

    if event.TSOFence > state.TSOFence:
        state.TSOFence = event.TSOFence
```

这个 apply 过程保证 checkpoint restore 与 tail replay 都能看到同一份 compact rooted truth，而不是把 lease 和 allocator 下界拆成多份独立恢复动作。

#### 3.8.3 Contested failover

```text
coordinator A:
    allocates IDs up to x
    crashes without release

coordinator B:
    try CampaignLease() -> rejected while A's lease active
    wait until lease expiry
    CampaignLease() -> granted with IDFence >= x
    AllocID() returns y where y > x
```

这个场景对应本文评估中的 contested failover 测试，用来证明不同 holder 的 takeover 仍然遵守同一 rooted boundary。

#### 3.8.4 Freshness-aware route lookup

```text
function GetRegionByKey(key, requestedFreshness):
    state = CurrentReadState()

    if requestedFreshness == STRONG:
        require state.servedByLeader
        require state.rootLag == 0

    if requestedFreshness == BOUNDED:
        require state.catchUpState != BOOTSTRAP_REQUIRED
        require state.rootLag <= bound

    return {
        descriptor,
        servedRootToken,
        currentRootToken,
        rootLag,
        catchUpState,
        degradedMode,
    }
```

这段逻辑说明 freshness contract 不是额外附加的 debug 信息，而是 route lookup 的一部分返回语义。

## 4. 实现

### 4.0 代码映射

为了让本文主张可以直接落到代码，本节先给出关键协议与实现位置的对应关系：

- `EnsureCoordinatorLease`：`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/coordinator/server/service.go`
- `ApplyCoordinatorLease`：`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/meta/root/state/types.go`
- remote rooted client redirect：`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/meta/root/remote/client.go`
- rooted storage adapter：`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/coordinator/storage/root.go`
- same-holder rebuild / contested failover / chaos monotonicity：`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/coordinator/integration/separated_mode_test.go`

这些映射的意义不是列文件名本身，而是强调：本文讨论的协议边界在当前仓库里已经有对应实现，而不是 paper-only 设计。

### 4.0.1 关键实现片段

下面给出三段与论文主张直接对应的真实接口签名，用来把本文的协议边界落到具体代码。

`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/coordinator/server/service.go`

```go
func (s *Service) ensureCoordinatorLease() error
func (s *Service) reserveIDs(count uint64) (uint64, uint64, error)
func (s *Service) reserveTSO(count uint64) (uint64, uint64, error)
```

这组函数负责 coordinator 侧的 fast path、lease campaign 与 allocator fence 继承。

`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/meta/root/state/types.go`

```go
type CoordinatorLease struct {
    HolderID        string
    ExpiresUnixNano int64
    IDFence         uint64
    TSOFence        uint64
}
```

这里定义的是本文真正关心的 takeover-critical state，而不是普通的只读 view。

`/Volumes/mac Ds - Data/WorkSpace/GitHub/NoKV/coordinator/storage/root.go`

```go
func (s *RootStore) SaveAllocatorState(idCurrent, tsCurrent uint64) error
func (s *RootStore) CampaignCoordinatorLease(
    holderID string,
    expiresUnixNano, nowUnixNano int64,
    idFence, tsoFence uint64,
) (rootstate.CoordinatorLease, error)
```

这组接口说明：当前实现里“lease transition 的 rooted 化”与“平时 allocator fence 的推进”是两条不同的 rooted write path。前者是本文主安全边界，后者是工程性持久化路径。

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

这里还有一个需要向 reviewer 说清楚的实现事实：当前 `SaveAllocatorState(idFence, tsoFence)` 仍然是 `IDFence` 与 `TSOFence` 的顺序推进，而不是单条复合 rooted event。本文的核心安全主张并不建立在这条路径之上，而建立在 `CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` 这一条 atomic rooted transition 上。换句话说，平时的 fence 推进仍然是工程性持久化；真正消除 cross-authority TOCTOU 的是 takeover 时的 rooted lease event。

### 4.3 三种 rooted authority

NoKV 当前支持三种 rooted authority 形态：

- local：单节点文件 authority
- replicated：co-located 的 rooted raft authority
- remote：gRPC access layer over local/replicated authority

这三种模式共享同一 `RootStore` contract。remote 不是第三种 truth kernel，而是 rooted authority 的访问层。这里还有一个必须说清楚的语义边界：在 remote 模式下，coordinator 侧的 `IsLeader()` 不再承担 rooted leader 判定，写入是否能够进入真正的 rooted leader 由 remote client 的 redirect 逻辑负责；因此 coordinator 侧的安全 gate 主要落在 `CoordinatorLease` 上，而不是本地 peer 身份检查上。

### 4.4 控制面可观测性

为了支撑调试与论文评估，当前实现还暴露了一组控制面 diagnostics：

- 当前 `CoordinatorLease` holder、expiry、fence
- allocator 窗口上界
- rooted lag
- catch-up 状态

这些内容不是协议本身的一部分，但它们对验证 rebuildability 很关键。对于一个 workshop 论文来说，可观测性并不只是工程附属品，它直接决定 reviewer 是否能把文中 claim 映射到运行系统。

### 4.5 当前实现状态

与本文主张直接相关的机制已经落地：

- `CoordinatorLeaseGranted(holder, expires, idFence, tsoFence)` typed event
- rooted snapshot apply 中的 lease/fence 原子恢复
- lease renew loop 与 clock-skew buffer
- explicit freshness contract
- `TailCatchUpAction`
- remote rooted API 与 leader redirect
- allocator window
- crash/recovery、不同 holder failover、leader change 与 chaos monotonicity integration tests

## 5. 评估

### 5.1 评估问题

本文不试图证明 NoKV 整体性能优于工业系统。评估只回答四个更窄的问题：

1. coordinator 是否真的可以在没有 service-local durable recovery state 的情况下 crash/reopen 且不回退；
2. ownership transfer 与 allocator lower bound 是否真的由 rooted truth 安全衔接；
3. 不同 holder 的 contested failover 是否在 lease 过期后安全接管；
4. remote rooted authority 在 windowing 保护下是否不会落入稳态热路径。

### 5.2 环境与边界

当前结果主要来自 Apple M3 Pro localhost 环境。控制面 benchmark 覆盖：

- in-process local/root access
- localhost TCP remote access
- process-separated remote root
- process-separated etcd CAS baseline

此外，仓库中还保留了一组 `tc netem delay 1ms` 的补充结果，说明当 remote TCP 路径被显式注入小规模网络延迟后，windowing 保护下的稳态分配仍停留在微秒级，而 no-window 路径则继续维持毫秒级退化。本文把这些结果作为“代价形状不变”的补充证据，而不是独立的性能卖点。

### 5.3 Crash recovery correctness

`TestSeparatedModeCoordinatorCrashAndRecoveryPreservesAllocatorFence` 验证了最小接管正确性：

- old coordinator 分配 ID 后 crash
- new coordinator 用同一 stable `coordinator-id` 重建并接管
- 新的 `AllocID` 严格大于旧实例最后返回值
- rooted `IDFence` 与 `CoordinatorLease.IDFence` 不回退

这直接证明，allocator lower bound 的恢复起点来自 rooted truth，而不是 service-local state。它覆盖的是 same-holder rebuildability，而不是 contested holder failover。

### 5.4 Recovery latency

`TestSeparatedModeCoordinatorRecoveryLatency` 对 same-holder rebuild 的恢复延迟做了重复测量。在当前 localhost 环境中：

- `avg ≈ 98.1ms`
- `p50 ≈ 95.8ms`
- `p95 ≈ 103.0ms`

这里的恢复延迟包含：remote rooted store 打开、bootstrap load、view restore 与第一次 lease campaign。本文不把它解读为生产级 SLA，而把它解读为：**在没有 service-local durable recovery state 的前提下，rebuildable coordinator 的冷恢复成本仍然落在百毫秒量级。**

### 5.5 Contested failover

`TestSeparatedModeCoordinatorContestedFailoverPreservesAllocatorFence` 验证了更严格的 takeover 路径：

- coordinator-A 以 `c1` 身份持有 lease 并分配一段 ID
- A crash，且不显式 release
- coordinator-B 以不同 holder `c2` 立即尝试接管，预期失败
- 在 lease TTL 过期后，B 重新 campaign 并成功接管
- B 的首个 `AllocID` 严格大于 A 的最后返回值
- rooted `CoordinatorLease.HolderID` 切换为 `c2`，且 `CoordinatorLease.IDFence` 继承 A 的下界

这个测试把“同一 holder 重建”与“不同 holder 竞争式接管”分开验证，也把系统的不可用窗口边界显式化：当旧 holder crash 且未 release 时，新 holder 的接管等待主要由 lease TTL 决定。

当前代码默认配置为：

- `leaseTTL = 10s`
- `renewIn = 3s`
- `clockSkewBuffer = 500ms`

因此在默认运行参数下，contested failover 的最坏不可用窗口上界约为一个 lease TTL，而不是无限等待。为了把该边界压缩进可重复测试，本文的 contested failover integration test 使用更短的参数：

- `leaseTTL = 150ms`
- `renewIn = 40ms`

在这个测试配置下，旧 holder crash 后，新 holder 的第一次立即接管会被拒绝；随后在 `3s` 的 Eventually 窗口内完成 takeover，证明等待边界是显式 lease timeout，而不是隐式状态漂移。本文把这段等待解释为 rebuildable singleton service 的显式 availability cost，而不是隐藏在“自动恢复”表述中的免费接管。

### 5.6 Leader change safety

`TestReplicatedStoreCoordinatorLeaseFenceSurvivesLeaderChange` 验证了 rooted authority leader 切换后：

- `CoordinatorLease` 不丢失
- `IDFence` / `TSOFence` 不回退

这说明本文的安全主张不依赖某个固定 leader，而依赖 typed rooted metadata transition。

### 5.7 Chaos monotonicity

为了验证“可重建 coordinator”是一个性质而不是单次演示，本文增加 chaos-style integration test：

- 反复 crash/reopen 同一 stable `coordinator-id`
- 每轮分配一个固定 batch 的 `AllocID`
- 检查新一轮起点严格大于上一轮终点
- 检查 rooted `IDFence` 持续单调
- 检查新一轮 lease campaign 继承上一轮 fence

在当前 localhost 环境下，24 轮测试、每轮 256 个 ID 的结果为：

- `first = 1`
- `last = 230256`
- `avg_recovery ≈ 110.3ms`
- `p50 ≈ 108.8ms`
- `p95 ≈ 120.5ms`

全程 `AllocID` 严格单调，无重复、无回退。这个测试比单次 recovery latency 更重要，因为它展示的是一种 recovery model，而不是一次偶然成功的重连。

### 5.8 Hot-path viability

当前 benchmark 显示，在默认窗口配置下：

- process-separated NoKV remote rooted authority 的稳态 `AllocID` 延迟约为 `1.0us`
- process-separated etcd CAS baseline 的稳态延迟约为 `0.4us`

当窗口退化为每次分配都推进 fence 时：

- NoKV remote rooted path 上升到约 `9.8ms`
- etcd CAS baseline 上升到约 `3.9ms`

这些数字说明的不是“NoKV 比 etcd 快”或“更慢”，而是：**serialized remote fence write 才是主要代价，windowing 才是让 rebuildable coordinator 可用的工程条件。** NoKV 的单次 remote rooted write 更重，因为它承载的是 typed rooted semantics、rooted state apply 与 checkpoint/compaction 相关开销，而 etcd CAS baseline 只是在更新单个 key。本文因此把 etcd baseline 用作“代价形状对照组”，而不是用来宣称绝对性能优势。

### 5.9 小结

当前证据支持以下结论：

- NoKV 已经实现了一个不依赖 service-local durable state 的 rebuildable coordinator。
- atomic lease-fence transition 让 takeover-critical state 被收敛进单一的 rooted authority boundary。
- contested failover 证明不同 holder 的接管也沿着同一 rooted authority 边界完成。
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

### 6.3 与 PD 风格恢复的区别

本文不把 NoKV 与 TiKV/PD 的差别简单描述成“RPC 更少”或“步骤更少”。更准确的说法是：PD 风格恢复依赖多个逻辑状态项的独立读取与组合，而 NoKV 将 singleton recovery 所需的关键 allocator lower bounds 作为 ownership transition 的一部分 rooted 化。两者的本质区别在于 **atomicity boundary**，而不是单纯调用次数。

### 6.4 为什么工业界不一定这样做

工业界并不是“不知道这种分离”，而是往往更偏好复制整个控制器：

- 系统边界更直接
- 旧方案已经够用
- 额外的 freshness/lease/window 协议复杂度未必值得

所以本文不主张“NoKV 一定更适合所有系统”。本文主张的是：**如果你想把 coordinator 从 authority 降级成可重建 service 层，那么这些协议条件是必须回答的。**

### 6.5 当前局限

当前工作仍有边界：

- benchmark 主要来自 localhost
- 尚未给出大规模 multi-host 结果
- freshness contract 的“减少无效 stale-route round trip”价值还缺专门对照实验
- `SaveAllocatorState` 在当前实现里仍然是 `IDFence` 与 `TSOFence` 的顺序推进，而不是单条复合 rooted event
- 本文展示的是 workshop 级别的系统性质与工程验证，而不是 full-paper 级别的大规模性能结论

### 6.6 当前最值得继续深挖的创新点

如果继续沿这条路线往前推，本文最值得投入的不是“再做一个更快的 coordinator”，而是把当前已经出现的协议边界继续对象化。按价值排序，当前最强的三个升级方向如下。

第一，**把 atomic lease-fence transition 升级成显式 rooted takeover certificate**。当前实现已经把 holder、expiry 与 allocator lower bounds 放进同一 rooted transition，但尚未把它们对象化为独立协议 artifact。若继续推进，这个 certificate 还应显式携带 root version、issued term 或等价 generation，从而把 “谁有权接管” 与 “从哪里安全继续” 绑定成一个更可审计的对象。

第二，**把 freshness 从内部状态字段升级成更硬的 serving contract**。当前 `RootToken`、`CatchUpState` 与 `DegradedMode` 已经把 staleness 暴露给调用方，但仍然更像实现上的可观测状态。更强的方向是把 serving state 明确收成 `AUTHORITATIVE / BOUNDED_STALE / DEGRADED` 之类的 client-visible classes，并为每一类定义 route lookup、allocator 与 takeover 后的可服务边界。

第三，**把 allocator window 升级成 lease-backed allocation budget**。当前 windowing 主要被写成工程上的热路径优化；更强的表述则是：当前 holder 在当前 rooted lease 下被授权消耗一段 allocator 预算，这段预算本身也属于 authority boundary 的一部分。这样一来，windowing 不再只是 amortization，而变成 authority-preserving delegation。

### 6.7 当前最合理的推进方式

基于当前 artifact，本工作最合理的推进方式不是继续扩大系统范围，而是沿着上面的协议对象逐步加硬：

- 先把当前 rooted transition 明确重命名为 minimal takeover substrate；
- 再把 takeover certificate 与 serving contract 明确外化；
- 最后再决定是否值得把 budget epoch、typed degraded states 与更完整的 publication-style verifier 纳入同一 protocol。

这一路线的价值在于：它不会把题目炸成新的 consensus 或 metadata-kernel 论文，但能把当前已经成立的 workshop 级主张继续推向更强、也更可审查的系统边界。

## 7. 相关工作

### 7.1 Delos

Delos 提供了 service logic 与 log substrate 解耦的 `VirtualLog` 思路，是 NoKV `meta/root` 的直接启发来源。但 Delos 并不直接处理带 singleton duties 的 rebuildable coordinator 问题，也不提供 atomic lease-fence transition。NoKV 建立在 Delos 风格的 rooted truth abstraction 之上，但本文的贡献不在 abstraction 本身，而在于把 singleton recovery 收敛成单一 rooted transition。

### 7.2 TiKV / PD

PD 代表了更典型的“复制整个控制器”路径。它当然可以工作，也解决了 allocator failover 问题，但其恢复通常由多个逻辑步骤、外部持久化和安全余量共同完成。本文不声称 NoKV 全面优于 PD，只强调一种不同的 recovery model：从 rooted truth 重建 service，而不是恢复一个携带 durable state 的 controller。

### 7.3 Kafka KRaft

KRaft 具有 typed metadata records，也具备 metadata quorum 与 controller 的分工，但 controller 与 metadata quorum 紧耦合，并不以 rebuildable service/view layer 为核心抽象。KRaft 与本文在 typed metadata 上有相似性，但没有提出“让带 singleton duties 的 coordinator 从 rooted truth 重建”的问题设定。

### 7.4 FoundationDB

FoundationDB 是本文必须正面承认的反例：它确实把 coordinators 与 cluster controller 分离，因此“不再把 truth 与 service 绑定在同一进程里”本身并不是本文的创新。本文与 FoundationDB 的差别在于问题设定与协议边界：FoundationDB 的 coordinators 保存极小的 bootstrap truth，而本文讨论的是一个显式 typed rooted metadata log；FoundationDB 的恢复依赖整套 cluster controller / sequencer 协调路径，而本文把 singleton takeover-critical state 直接根植到单条 rooted lease event 中。换句话说，本文不是在声称“truth/service split 是新思想”，而是在声称“对带 singleton duties 的 KV coordinator，takeover-critical state 可以被根植到单一 rooted truth 中”。

### 7.5 Zanzibar 与 freshness tokens

Google Zanzibar 使用 zookie 作为 externally visible consistency token，说明“把 freshness 显式暴露给调用方”并不是一个全新思想。NoKV 与其不同之处在于：本文的 freshness token 不是面向全局授权快照，而是面向 rooted control-plane view 的 staleness、catch-up 与 degraded mode。本文不把 freshness contract 作为主创新点，而把它视为 rebuildable control plane 的必要补充语义。

### 7.6 CockroachDB

CockroachDB 并不是本文的直接同类，因为它没有一个承担 `AllocID` / `TSO` 这类 singleton duties 的集中式 coordinator。它通过 range descriptor 与 in-band metadata dissemination 处理 stale routing，本质上属于“把 metadata authority 深度嵌入数据平面”的路径。本文把它列为边界案例：它说明不是所有现代系统都需要本文讨论的 coordinator recovery model，也因此再次限定了本文的适用范围。

## 8. 结论

本文不是在提出一个全新的分布式控制面范式，也不是在宣称一个全面优于工业系统的架构。本文做的事情更窄，但也更具体：它把带 singleton duties 的 distributed KV coordinator 的恢复问题，从“恢复一个复杂的大脑”改写成“如何把接管关键状态根植到单一 truth boundary 中，再从这份 truth 重建一个 service 层”。

NoKV 的核心答案是：复制 truth，而不是复制整个 service；让 ownership transfer 与 allocator lower bounds 由同一 rooted transition 原子表达；让 staleness 通过显式 freshness contract 暴露；让 remote rooted authority 通过 windowing 退出稳态热路径。由此，coordinator 可以失去“本地 durable 状态载体”的身份，而保留“可重建服务层”的角色。

如果本文只留下一个核心结论，那就是：

> **对于承担 singleton duties 的 distributed KV control plane，真正关键的不是把 coordinator 拆成几个进程，而是能否把 takeover-critical state 根植到同一份 rooted truth 中，使服务在失去本地 durable recovery state 后仍可被安全重建。**
