# Eunomia: A Service-Level Correctness Class for Authority Handoff Continuation Legality in Distributed Control Planes

## 摘要

分布式控制面里长期存在一类容易被误判成“局部实现 bug”的错误：rooted authority 其实已经正确切换，但 service layer 仍持有 stale、partial 或未完成 handover 的 authority 视图，于是旧 reply 或不完整 successor continuation 穿过 authority transition 边界并被系统继续接受。我们把这类错误命名为 **authority-gap anomalies**。对 etcd、Kafka KRaft、CockroachDB、TiKV、YugabyteDB、MongoDB 与 FoundationDB 的公开 issue、修复讨论和 maintainer 说明进行核查后，我们发现现有系统通常只在单条 path 上分别补 `seal`、era fencing 或 successor coverage，例如 Chubby 式 sequencer、Kafka KIP-320、CockroachDB 的 read-summary 路径和 MongoDB 的 `readConcern: linearizable` 都覆盖了其中一部分，但没有任何系统把这些义务统一成一个完整的 service-level correctness class（参见 [Chubby](https://www.usenix.org/conference/osdi-06/presentation/chubby-lock-service-loosely-coupled-distributed-systems)、[KAFKA-6880](https://issues.apache.org/jira/browse/KAFKA-6880)、[CockroachDB #66562](https://github.com/cockroachdb/cockroach/issues/66562)、[MongoDB SERVER-17975](https://jira.mongodb.org/browse/SERVER-17975)）。

本文提出 **Eunomia**，把 authority handoff 后“继续服务是否合法”定义成一个 service-level correctness class。Eunomia 由四条最小保证组成：`Primacy`、`Inheritance`、`Silence` 与 `Finality`；并通过 rooted `Tenure / Legacy / Handover`、reply witness verifier 与离线审计器共同落地。我们在 NoKV 中实现了第一个可执行的 Eunomia rooted skeleton，并给出独立的 `eunomia-audit` 工具；形式化方面，`Eunomia.tla` 已扩展为 repeated handoff model，在 `MaxEra=3` 下通过 `3924` 个 distinct states、深度 `20` 的 TLC 检查，同时 `LeaseOnly`、`TokenOnly`、`ChubbyFencedLease` 与 `LeaseStartOnly` 给出对照反例；系统证据方面，我们在 etcd `v3.6.10` 上捕获了真实 delayed reply 轨迹，并对 `etcd-io/etcd#21638` 给出 client-visible stale-success 证据，同时在 NoKV 中精确复现了 CockroachDB `#66562` 的 issue schedule。

本文的目标不是提出新的 consensus，也不是声称 NoKV 在整体性能上优于工业系统。本文要证明的是：对 authority-bound continuations 而言，工业系统缺的不是更多 path-specific patch，而是一个统一的 handoff legality 语义；Eunomia 使这件事第一次能够被实现、被审计、被形式化，并被同一套词汇跨系统消费。

## 1. 引言

### 1.1 问题：authority 已切换，错误为什么还会继续发生

考虑 etcd 的两个公开问题：在 [`#15247`](https://github.com/etcd-io/etcd/issues/15247) 中，raft 层已经完成 leader 切换，但旧 leader 上的 lessor goroutine 仍在以过时的 authority 视图执行 revoke；在 [`#20418`](https://github.com/etcd-io/etcd/issues/20418) 中，进程 pause 恢复后同类 delayed reply 又以另一种触发条件出现。两次问题的共同点很直接：**truth layer 是对的，service layer 还是错的**。CockroachDB [`#66562`](https://github.com/cockroachdb/cockroach/issues/66562) 则展示了另一种形态：新的 range lease 虽然已经拿到，但它的起点没有覆盖旧 lease 已服务过的 future-time read frontier，于是 successor 从协议上并不具备继续服务的资格。Kafka KRaft 的 [`KAFKA-15489`](https://issues.apache.org/jira/browse/KAFKA-15489) 以及后续的 [`KAFKA-14154`](https://issues.apache.org/jira/browse/KAFKA-14154)、[`KAFKA-15911`](https://issues.apache.org/jira/browse/KAFKA-15911) 也反复暴露了同一种结构：authority transition 已经发生，但旧 reply、旧 controller view 或未完成 handover 的 successor 仍在系统里活动。

这类问题不等于“leader 切换处理得不够快”，也不等于“某条 RPC 少了一个 if”。它们暴露的是一个更基础的问题：**authority handoff 之后，系统缺少统一的服务合法性语义。** 现有系统通常只证明“谁是当前 authority”，却没有统一证明“旧 reply 何时必须失效、successor 必须覆盖 predecessor 到哪里、以及这次 handoff 何时才算真正结束”。经典正确性条件如 linearizability 适合描述并发对象语义 [Herlihy and Wing 1990](https://dl.acm.org/doi/10.1145/78969.78972)，而 Chubby、Delos 这类系统则主要提供 authority substrate 与 handoff 载体（[Chubby](https://www.usenix.org/conference/osdi-06/presentation/chubby-lock-service-loosely-coupled-distributed-systems), [Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)）；它们都没有把“handoff 后继续服务是否合法”单独上升成一个 service-level correctness class。

### 1.2 差距：现有修复为什么总是局部有效

我们核查的公开证据表明，工业界并不缺修复 authority-gap 的技巧，而是缺一个统一的语义对象。Kafka 的 KIP-320 在 fetch 路径上用 era fencing 避免 zombie replica 继续服务；CockroachDB 在部分 read-summary 路径上引入 successor coverage；MongoDB 的 `readConcern: linearizable` 通过 rooted election metadata 强化 read path；TiKV 和 YugabyteDB 也分别在 follower read、leader transfer 或 metadata cache 路径上补过类似约束（参见 [KAFKA-6880](https://issues.apache.org/jira/browse/KAFKA-6880), [CockroachDB #36431](https://github.com/cockroachdb/cockroach/issues/36431), [TiKV PR #6343](https://github.com/tikv/tikv/pull/6343), [YugabyteDB #20124](https://github.com/yugabyte/yugabyte-db/issues/20124)）。这些修复能消掉单条 path 上的触发器，但它们通常不同时提供：

1. predecessor 退休后的 rooted seal；
2. old reply 的 era-based inadmissibility；
3. successor 对 predecessor 历史 frontier 的覆盖义务；
4. handoff 最终完成的 finality condition。

这就是 recurrence 反复出现的原因。一个系统可以补 era fence，但仍然漏 successor coverage；也可以补 read-summary coverage，但仍然没有明确 finality。最近两年的 verification 工作，例如 [CCF](https://www.microsoft.com/en-us/research/wp-content/uploads/2024/07/nsdi25spring-final392.pdf)、[RPRC](https://www.usenix.org/system/files/nsdi25-ding.pdf)、[Remix](https://marshtompsxd.github.io/pub/eurosys25_remix.pdf) 与 [T2C](https://www.usenix.org/system/files/osdi25-lou.pdf)，已经展示了 trace validation、protocol refinement checking 和 checker synthesis 的力量；但它们大多验证已有协议或实现，而不是定义一个新的 handoff legality 语义。本文要补的不是又一个 patch，也不是又一个 checker，而是两者共同缺失的那个 correctness object。

### 1.3 核心思想：把 handoff legality 定义成一个 correctness class

本文提出 **Eunomia**。Eunomia 关注的不是“谁当前持有 authority”，而是**authority 从 predecessor 切到 successor 之后，什么样的 continuation 仍然合法**。它用四条最小保证把这个问题固定下来：

- **Primacy**。任一时刻只能有一条 live authority lineage。
- **Inheritance**。successor 不只是新的，还必须覆盖 predecessor 已经服务到的 frontier。
- **Silence**。一旦 predecessor 被 seal，该代 reply 之后必须机械失效。
- **Finality**。一次 handoff 不能停在半开状态，必须显式走完 close / reattach。

Eunomia 不发明新的 consensus，也不要求所有控制面都采用相同实现。它只要求 authority-bound continuation duties 在 handoff 时提供一组最小而统一的协议对象：rooted `Tenure / Legacy / Handover`，以及可由 reply、trace 或 snapshot 消费的 witness。NoKV 里的 rooted truth kernel、`eunomia-audit`、以及 `Eunomia.tla` / `EunomiaMultiDim.tla` 正是围绕这组语义对象构建的。

正文里我们尽量只保留这套最小词汇：

- `Tenure`：当前 active authority record
- `Legacy`：前任 authority 的冻结边界
- `Handover`：一次 handoff 的收尾记录
- `Era`：authority era
- `Witness`：判断 handoff 是否安全的证明对象

论文与实现统一使用 `Tenure / Legacy / Handover`、`Era`、`MandateFrontiers` 这组名字，不再保留旧别名。

### 1.4 贡献

本文的贡献可以概括为四点。

1. **问题定义。** 我们把一类长期散落在 etcd、Kafka、CockroachDB、TiKV、YugabyteDB、MongoDB 与 FoundationDB 中的错误，统一命名为 **authority-gap anomalies**，并将其与一般 stale read、一般 leader failover 或一般 cache inconsistency 区分开来。
2. **语义贡献。** 我们提出 **Eunomia + 四条最小保证**，把 authority handoff 后的 continuation legality 定义为一个独立的 service-level correctness class，而不是若干 patch primitive 的松散组合。
3. **系统与工具。** 我们在 NoKV 中实现了第一个可执行的 rooted skeleton，并提供独立的 `eunomia-audit`，使 NoKV trace 与外部系统 trace 可以落到同一 anomaly vocabulary 上。
4. **证据链。** 我们给出 repeated handoff 的 TLA+ 正反模型、etcd 的 live delayed-reply 证据、以及 CockroachDB `#66562` 的 exact issue-schedule reproduction，用同一套最小保证词汇解释这些表面不同的问题。

### 1.5 本文声明什么，不声明什么

为了避免把问题定义、设计语义、实验结果和未来路线混在一起，本文把 claim 收得很窄。

**本文声明：**

- authority-gap anomalies 是一类可跨系统复发的 bug class；
- Eunomia 为这类 bug class 提供最小 service-level semantics；
- NoKV 和 `eunomia-audit` 证明这组语义可以被实现、被审计、被形式化；
- multi-dimensional frontier duties 可以被同一组 handoff semantics 消费。

**本文不声明：**

- 我们提出了新的 consensus protocol；
- NoKV 在整体吞吐或延迟上优于工业系统；
- 当前 artifact 已经给出完整的 unbounded proof；
- 当前 etcd / CRDB 证据已经等价于 upstream regression proof 或 live race replay；
- Eunomia 能在网络分区期间魔法般消除所有 partition-local stale reply。

这组边界对 OSDI 级别的写作很重要。本文的目标不是把所有 control-plane correctness 一口吃掉，而是把 authority-bound continuation 的 handoff legality 单独占住。

## 2. 背景、问题模型与设计目标

### 2.1 适用系统与 authority scope

Eunomia 不针对所有 distributed control plane，只针对**authority-bound continuation duties**。一个 duty 只有在满足以下四个条件时，才属于本文范围：

1. 它存在由单个 authority holder 持续推进的一段 continuation；
2. holder transition 时，这段 continuation 必须被显式 retirement，而不是天然允许多方并发读取；
3. transition 之后，old reply 是否还能被接受，构成一个明确的合法性问题；
4. detached segment 结束时，需要一个显式 finality 才能把这段 continuation 合法收口。

按这个定义，authority scope 不只包括 raft leader。etcd [`#21638`](https://github.com/etcd-io/etcd/issues/21638) 在单节点嵌入式 etcd 上就能触发 authority-gap anomaly，这说明 authority 也可以是一个对象级 lease，而不必是 node-level leader。本文关心的 authority 至少覆盖五层：

- **node-level**：如 raft leader、Kafka controller；
- **object-level**：如 etcd lease、ZooKeeper session；
- **range-level**：如 CockroachDB range leaseholder、TiKV region leader；
- **session-level**：如 MongoDB primary election session；
- **transaction-level**：如 2PC coordinator decision object。

NoKV 当前最自然的 duty 是 `AllocID`、`TSO` 和 `GetRegionByKey`。本文的主轴是前两类 monotone duties；`GetRegionByKey` 只作为次轴，因为它在 NoKV 当前实现里仍然绑定在 coordinator lease 上。反过来，像 PD APF 或 smart client 那种 freely replicated、每次都只做 bounded-freshness answer 的 metadata lookup，不在 Eunomia 当前最稳的 scope 里。

### 2.2 失败模型与 fault vocabulary

本文假设 rooted authority 自身仍然提供复制与持久化语义。我们关注的 failure envelope 是：**rooted authority 仍然正确，但 service layer 暂时拿不到完整 truth，或者仍在按旧 truth 继续服务。** 在这个前提下，本文主要覆盖下面几类 fault：

- `delayed_reply`：旧 reply 在 handoff 之后才到达调用方；
- `revived_holder`：旧 holder 恢复后继续输出旧代 reply；
- `root_unreach`：service layer 暂时无法访问 rooted truth；
- `lease_expiry`：authority 自然过期后 successor 接手；
- `successor_campaign`：successor 通过 fresh acquire 或 transfer 接手；
- `budget_exhaustion`：detached 期间 local budget 或 frontier 用尽；
- `descriptor_publish_race`：metadata publish 与 answerability 之间发生竞态。

这不是本文声称的“最小充分 fault set”，而是当前 artifact 的工作 vocabulary。它的作用是统一 NoKV integration tests、TLA+ model、`eunomia-audit` 的 anomaly kinds，以及 etcd / CRDB 两条外部证据线。

### 2.3 设计目标

本文的设计目标有四个。

1. **让部分服务继续可用。** 当 root 暂时不可达时，系统仍能在明确边界内继续一部分有价值的 authority-bound duties，而不是一律 fail-stop。
2. **让 continuation legality 可判定。** 每条 reply 都必须附着足够证据，让调用方或审计器判断它为什么仍然合法。
3. **让 successor 覆盖义务显式化。** successor 的合法性不能只来自“我更年轻”，还必须来自它对 predecessor frontier 的覆盖。
4. **让 handoff 可收口。** detached period 不能永远停留在“看起来没炸”的半开状态，必须最终走到 rooted close / reattach，并可被离线审计。

### 2.4 非目标

本文不试图：

- 在 detached 模式下维持全部 control-plane write；
- 取代 Chubby、Delos、PD、KRaft、CockroachDB 或 FoundationDB 的整体设计；
- 重新定义一般意义上的 linearizability、serializability 或 session guarantees；
- 在网络分区期间阻止所有 partition-local stale reply；
- 用少量 localhost 数据支撑“比工业系统更快”的泛化结论。

更准确地说，Eunomia 要保证的是：一旦 rooted authority 重新可见，旧 era reply 必须变成 inadmissible，successor 必须覆盖 predecessor frontier，而且整段 detached period 必须能被 seal、close、reattach 和 audit 收口。它保证的是 **post-transition legality**，不是“任何时候都绝不短暂出错”的魔法语义。

## 3. 协议抽象：Auditable Authority Handoff

### 3.1 AuthorityHandoffRecord

本文的核心协议对象不是普通 lease，也不是“给 lease 再加几个字段”。当前 artifact 里，协议核实际上由 **三个 rooted persisted object** 与 **一个 protocol-side projection** 组成：

```text
Lease{
    holder,
    expiry,
    epoch,
    issued_at,
    mandate,
    lineage_digest
}

Seal{
    holder,
    epoch,
    mandate,
    frontiers,
    sealed_at
}

Finality{
    holder,
    legacy_era,
    successor_era,
    legacy_digest,
    stage,
    confirmed_at,
    closed_at,
    reattached_at
}
```

在 NoKV 的当前实现里，这三个 rooted object 分别 materialize 为 `Tenure / Legacy / Handover`。其中，`AuthorityHandoffRecord` 不是额外的第四种 persisted event，而是从当前 rooted lease 与 frontier 视图投影出来的协议对象：

```text
AuthorityHandoffRecord{
    holder,
    expiry,
    epoch,
    issued_at,
    mandate,
    lineage_digest,
    frontiers
}
```

它表达的不是“当前谁是 owner”这一件事，而是以下内容的一次性绑定：

- 谁被授权继续服务；
- 被授权服务哪些 authority-bound duties；
- 该次 authority instance 相对于哪一个 rooted cursor 生效；
- 它继承自哪个 predecessor seal；
- 它当前负责覆盖哪些 duty frontier。

这里有一个必须说清的实现边界：`IDFence / TSOFence` 当前仍存在于 rooted `State` 中，用来承接 NoKV 的 allocator truth；但它们已经**不再是 lease 对象本身的字段**。当前 artifact 的协议核已经把 lease 本身压缩成 authority truth，而把 monotone / descriptor / lease-start frontier 都收进 seal 的 `frontiers` 与 protocol-side duty-frontier projection。

### 3.2 ContinuationWitness 与 Duty Frontier

`AuthorityHandoffRecord` 只定义“谁对哪段 frontier 负责”；真正让一次继续服务可验证的，是 reply evidence。当前 artifact 中，这层 evidence 以两种形态落地，但共享同一 verifier 语义：调用方必须能判断“这条 reply 属于哪一代 authority、消耗或回答到了哪一个 frontier，以及它此刻是否仍然 admissible”。

对 `monotone duties`，这层证据由显式 `ContinuationWitness` 承担。它至少要回答两件事：

1. 这次 reply 属于哪个 authority handoff；
2. 这次 reply 消费到了哪个 duty frontier。

对 `monotone duties`：

- `AllocID` / `Tso` 的 frontier 是不重叠区间；
- holder 只能消耗当前 handoff 显式负责的 monotone frontier；
- successor 必须覆盖 predecessor seal 中记录的 duty frontier。

对 `metadata-answer duties`：

- `GetRegionByKey` 的 frontier 不是单个数值区间，而是一个可判定的 answerability basis：
  - `required_root_token`
  - `descriptor_frontier`
  - `max_root_lag = Δ`
  - `sync_health`
- holder 可以继续回答，但只能在 rooted token、descriptor frontier 与 admissibility basis 允许的范围内，并携带足够协议证据；
- 超出 frontier 后必须拒答。

这使 continuation 不再依赖“缓存里似乎还有数据”，而依赖 rooted frontier 与可消费的 reply evidence。

### 3.3 ContinuationWitness

本文的目标不是只返回“答案”，而是返回“答案为什么此刻还能合法继续”。这里的 `ContinuationWitness` 更准确地说是 **一类非密码学协议证据**：对 monotone duties，它已经是显式协议对象；对 metadata-answer duty，它当前体现在 `GetRegionByKeyResponse` 上一组可被 client verifier 消费的字段中，而不是复用同一个 struct。

对 `GetRegionByKey`，当前 reply basis 至少携带：

```text
{
    descriptor,
    epoch,
    served_root_token,
    required_root_token,
    descriptor_revision,
    max_root_lag,
    sync_health,
    serving_class
}
```

对 `AllocID` / `Tso`，当前显式 monotone witness 至少携带：

```text
{
    first,
    count,
    epoch,
    consumed_frontier
}
```

`ContinuationWitness` 的目的不是为了美观，而是为了两个具体收益：

1. client / gateway / raftstore 可以验证 reply 是否仍在合法 frontier 内；
2. detachment 期间的服务可以在 close / reattach 时被 audit，而不是只能依赖日志回放后的善意解释。

### 3.4 HandoverWitness 与 Finality Protocol

这是本文真正最值钱的部分，也是和大多数 prior art 拉开距离的地方。

authority handoff 真正困难的，不是“离线时继续发一点号”，而是：

**重新接回 root 之后，系统如何可审计地证明这段 handoff 没有越过 authority boundary。**

本文要求一个最小 `HandoverWitness` protocol，而不只是口头上的“不变量”。当前 artifact 的最小 rooted handover 命令是：

1. **seal**
   - current holder 停止继续服务，并把当前 era 的 mandate frontier 冻结到 rooted `Legacy.frontiers`。
2. **confirm**
   - successor 以 `lineage_digest + successor frontier projection` 显式确认自己覆盖 predecessor seal。
3. **close**
   - rooted truth 验证 lineage 与 coverage 成立，并把这段 detached period materialize 为 closed。
4. **reattach**
   - handover 结束后，当前 authority instance 被显式标成 reattached。

从概念上看，Eunomia 仍然包含 `Seal → Cover → Close` 这条 handover logic；但在当前代码里，`cover` 没有被单独做成一个新的 persisted stage，而是折叠进 `confirm + finality audit` 这组 rooted 判定里。

在这个最小 lifecycle 之上，本文要求三个核心 finality properties：

1. **No overlap**
   - 不同 live handoff 的同类 frontier 绝不重叠。
2. **Successor coverage**
   - successor frontier 必须覆盖 predecessor 已消费 frontier。
3. **Post-seal inadmissibility**
   - seal 之后，旧 era reply 必须变成机械不可接受。

这使 reattach 不再是“回来同步一下”，而是 `Close` 之后的显式 rooted 生命周期阶段。

### 3.5 Eunomia

在这套协议下，本文真正想定义的新边界不是“还能继续服务”，而是：

> **只有能被合法收口的 continuation，才是合法 continuation。**

也就是说，一段 detached singleton-duty continuation 只有在以下条件同时成立时，才算 finality-complete：

- 它属于一个显式 handoff era；
- 它的 reply 都能被 `ContinuationWitness` 验证；
- predecessor 最终被 seal；
- successor 显式覆盖 predecessor frontier；
- post-seal reply 机械失效；
- rooted truth 最终生成 `Close / Reattach` 记录。

### 3.6 四条最小保证

为了让全文的 claim 不只停在 framing，而是形成最小可检查语义，本文把关键安全边界压缩成四条最小保证。它们的中心不是“系统里存在若干有用字段”，而是：

> **每个合法 continuation 都必须沿同一条 authority lineage 向后可追到 rooted issue，向前可收口到 seal / cover / close。**

当前实现的最小保证包含下面四条不变量：

1. **Primacy**
   - 任一时刻，对于同一 duty class，系统至多存在一个 live handoff frontier 可以继续合法推进。
2. **Inheritance**
   - monotone duty 的 frontier 只增不减；successor 至少覆盖 predecessor 已消费 frontier。
3. **Silence**
   - seal 之后，旧 era reply 必须变成机械不可接受。
4. **Finality**
   - detached period 结束后，系统必须能够证明 predecessor 已 seal、successor 已 cover、旧 reply 已失效，并被 rooted truth 正式 close。

这四条保证的作用有两层。对协议设计而言，它告诉我们 `AuthorityHandoffRecord / ContinuationWitness / HandoverWitness` 必须暴露哪些字段才算完成语义闭环；对验证和审计而言，它又提供了一个比“整个 consensus protocol 是否正确”更窄、更可操作的 service-level target。更关键的是，它把本文的中心从“再造一种 lease”收窄为“定义 detached authority-bound continuations 的最小 lineage semantics，并让它可被 proof、trace validation 与 runtime checker 同时消费”。

### 3.7 旧范式为什么不够

为了让本文从“一个更完整的协议设计”进入“一个新的系统性质”，这里把最关键的负结果方向提前说清楚：

> **工作命题：任何只提供 `lease + fence + budget/window`，但没有显式 `HandoverWitness` 和 rooted `Close` 语义的系统，都无法在存在 delayed old reply、old holder revival 与 root temporary unavailability 的 fault schedule 下，同时机械保证 `No overlap`、`Successor coverage` 与 `Post-seal inadmissibility`。**

这条命题的意义不是现在就声称本文已经完成 formal proof，而是明确指出本文真正要证明的不是“新字段有用”，而是：

- 旧范式可以局部继续服务；
- 但旧范式无法把 detachment 的合法结束做成 first-class safety condition；
- 因此它不能构成 Eunomia。

也正因为如此，本文最值得投入的下一层贡献不该只是“再补几个 handoff 字段”，而应是把 `Eunomia` 与这四条最小保证变成：

- TLA+ / proof 可写的 formal target；
- runtime / offline audit 可消费的 checker contract；
- cross-system pilot 可复用的 bug-class vocabulary。

## 4. NoKV 设计：从最小 rooted substrate 到 authority handoff skeleton

### 4.1 `meta/root`：typed rooted truth kernel

NoKV 当前已经把 `meta/root` 收成最小 truth kernel：

- typed rooted events
- checkpoint + committed tail
- materialized rooted state machine
- local / replicated / remote 三种 rooted authority 形态

这一层最重要的价值不是“有个 log”，而是：**service layer 可以消费统一 truth boundary，而不是依赖散落的外部持久化 side effects。**

### 4.2 `coordinator`：service / view layer，而不是完整 authority brain

NoKV 当前把 `coordinator` 组织成：

- route / metadata-answer service
- singleton-duty host
- rooted view consumer
- detached-capable proposal gate

这里最重要的边界是：

- durable truth 在 `meta/root`
- runtime view 与 service 在 `coordinator`
- continuation 的合法性来自 rooted handoff / frontier，而不是本地 durable recovery state

### 4.3 当前实现已经提供的最小 substrate

NoKV 当前 artifact 已实现以下关键机制：

- rooted `Tenure / Legacy / Handover`
- rooted snapshot apply 中的 lease / seal / handover recovery
- rooted allocator truth `IDFence / TSOFence`
- protocol-side `MandateFrontiers` 投影
- `RootToken`、`CatchUpState`、`DegradedMode`
- route lookup freshness contract
- monotone reply evidence 与 client-side era verifier
- remote rooted API 与 leader redirect
- same-holder rebuild / contested failover / leader change / chaos monotonicity tests

这套机制已经足以证明：

- takeover-critical state 可以被 rooted 化；
- service layer 可以从 rooted truth 重建；
- remote rooted authority 不必在 steady-state 每次请求都落入热路径。

### 4.4 当前实现设计：admissibility、late-reply rejection 与 successor admission

如果只写“有 handoff object、有 witness、有 handover skeleton”，paper 仍然容易被 reviewer 读成高层协议草图。更稳的做法，是把当前实现的设计边界直接写清楚，尤其是哪些 reply 允许继续、哪些必须拒绝、以及 successor 何时才算合法接管。

对 monotone authority-bound continuation duties，当前 admissibility 的最小实现边界是：

1. 当前节点持有该 duty class 的 live handoff；
2. 本次消费仍然落在该 handoff 显式负责的 frontier 内；
3. monotone reply 必须携带 `epoch` 与 `consumed_frontier`；
   其中 `era + consumed_frontier` 已经构成当前 legality witness 的最小语义核；
4. 一旦调用方已观察到更大的 era、`SealRecord(g_old)` 或 `Close/Reattach`，旧 era reply 必须被拒绝，而不是再交给后续逻辑兜底。

对 metadata-answer duty，当前目标边界则更保守。它不主张“一切 stale answer 都不合法”，而是要求 route admissibility 至少受下面这些条件约束：

1. 当前节点处于 rooted authoritative mode，或持有 metadata-answer duty 的 live handoff；
2. `served_root_token >= required_root_token`；
3. `descriptor_revision >= required_descriptor_revision`；
4. 若请求显式要求 bounded serving，则 `root_lag <= max_root_lag`；
5. `CatchUpState != BootstrapRequired` 且 `sync_health == HEALTHY`。

这条设计的意义不是把 `GetRegionByKey` 重新包装成“更聪明的 stale route”，而是让 metadata-answer path 也拥有 handoff-bound answerability contract。当前实现已经有一版 client-side verifier 闭环，但它和 monotone witness 不是同一个协议对象：metadata-answer path 目前通过 `served_root_token / descriptor_revision / max_root_lag / sync_health / epoch` 这一组 response basis 来完成 admissibility 检查。

同样重要的是 old-holder late reply rule。当前实现不再幻想网络里不存在迟到回复；它直接把 response legality 写成协议条件：只要 consumer 已经观察到以下 rooted 证据之一，旧 era reply 就必须变成 inadmissible：

- `SealRecord(g_old, ...)`
- 一个更新的 live handoff `g_new > g_old`
- 已提交的 `Close / Reattach` rooted records

successor admission 也必须是 rooted 的，而不能靠本地猜测。当前实现只接受两类 successor 接管：

- `clean seal`
  - rooted truth 已记录 predecessor seal；
  - successor 显式引用该 seal 的 predecessor digest；
  - successor 的 floors/frontiers 覆盖 predecessor sealed frontier。
- `forced takeover after predecessor became non-continuable`
  - rooted truth 仍记录 predecessor handoff；
  - 但 predecessor 已因 lease expiry 且更大 era 提交而失去继续资格；
  - successor 的 frontier 必须从 rooted state 选择，而不是从本地缓存猜测。

这组实现边界直接决定了本文后续 formal / checker / audit 的 target：不是证明一个理想协议，而是证明 NoKV 当前这套 admissibility、successor coverage 与 late-reply rejection 是否足以逼近 `Eunomia + 四条最小保证`。

### 4.5 当前实现与目标协议的差距

为了让 claim 诚实且可 defend，本文明确区分：

**已经实现的最小 substrate**

- rooted lease-fence transition
- freshness-aware route lookup
- windowed monotone serving
- rebuildable service/view layer

**仍未完全实现的 authority handoff / finality protocol**

- persisted schema 仍未完全 duty-generic 化
- metadata-answer path 仍未收敛到和 monotone duty 完全同构的 witness object
- live CRDB adapter / state projection 仍未落地
- 更强的自然迟到 reply transport 证据仍未完成

因此本文并不把当前 artifact 夸大为“已经完成所有协议泛化”，而是把它定位为：**一套已经运行的 rooted substrate，加上 monotone-duty path 上已落地的 client verifier、snapshot/trace audit 与外部 issue-line evidence，足以支撑 authority handoff / finality 的 formal、checker 与跨系统主线。**

从协议视角，正文只保留三类最小对象，而不再额外依赖独立的“协议移植说明”：

- `Rooted handoff record`
  - rooted lease；
  - predecessor linkage；
  - successor frontier coverage 结果。
- `Reply witness`
  - legality 核只依赖 `era + consumed_frontier`；
  - metadata-answer path 当前使用一组 response basis 做同一 verifier 语义，而不是复用 monotone witness struct。
- `Rooted finality stage`
  - `pending_confirm -> confirmed -> closed -> reattached`；
  - `Finality` 在 NoKV 里被 materialize 为 `Handover`，用来承接 `confirm / close / reattach` lifecycle；这不要求移植者照抄内部事件拆分。

也就是说，**外部 checker、diagnostics 与论文 claim 统一只对 `finality stage` 说话；NoKV 的事件拆分属于实现细节。**

### 4.6 代码映射

为避免 paper-only 设计，关键机制在当前代码中已有明确映射：

- `coordinator/server/service.go`
  - lease campaign / renew
  - freshness-aware `GetRegionByKey`
  - allocator window refill
- `meta/root/state/state.go`
  - compact rooted state
  - 在 NoKV 中 materialize 的 rooted `Tenure / Legacy / Handover`
  - apply-time lease/fence recovery
- `coordinator/storage/root.go`
  - rooted storage adapter
- `coordinator/audit/*`
  - rooted snapshot audit projection
  - finality / reattach anomaly evaluation
- `meta/root/remote/*`
  - remote rooted API 与 redirect
- `coordinator/integration/separated_mode_test.go`
  - crash / recovery / contested failover / routing split

### 4.7 代码落点与实现优先级

为了让 paper 不再依赖独立设计笔记，当前最小代码落点和实现顺序也直接收在正文里。

最小代码落点如下：

- `pb/coordinator/coordinator.proto`
  - monotone-duty reply evidence；
  - metadata-answer 所需的 `required_descriptor_revision`、`descriptor_revision` 与相关 health/freshness 字段
- `meta/root/state/state.go`
  - `Tenure / Legacy / Handover` 在 NoKV 中对应的 rooted state 实现
  - unified duty-frontier projection；当前 `AllocID/TSO/GetRegionByKey` 只是三个具体 frontier 实例，而不是四条保证的固定上限
- `meta/root/event/types.go`
  - handoff era、predecessor linkage、close status 等事件类型
- `coordinator/server/service.go`
  - lease campaign / renew；
  - monotone-duty budget/refill；
  - metadata-answer admissibility gate
- `coordinator/client/*`
  - monotone-duty `ContinuationWitness` verifier；
  - metadata-answer verifier；
  - stale-era reject / retry；
  - metadata path 上 non-zero detached era reject
- `coordinator/audit/*` + `cmd/nokv/cmd_eunomia_audit.go`
  - rooted snapshot audit projection；
  - anomaly surface；
  - 最小离线 `eunomia-audit` CLI
- `coordinator/integration/*`
  - old-holder late reply；
  - clean seal；
  - forced takeover after expiry；
  - coverage / non-overlap；
  - finality audit 基础用例

如果目标只是把 artifact 做完整，很多动作都可以并行；但为了保持对象、证据和实验主线一致，实现优先级应固定成下面这条线：

1. `AuthorityHandoffRecord`
2. `ContinuationWitness + client-side verifier`
3. `old-holder late reply rejection`
4. `HandoverWitness + Close`
5. `eunomia-audit`
6. `formal invariant / TLA+ skeleton`
7. `cross-system pilot`

这条顺序的本质不是“先做哪个模块更方便”，而是：

- 先让对象成立；
- 再让 witness 被真实消费；
- 再让 finality 成立；
- 最后才让 formal、checker 与跨系统 bug-class 证据站住。

## 5. 评估与证据计划

### 5.1 评估必须严格绑定四桶 claim，而不是继续堆 benchmark

当前 paper 的问题已经不是“还能不能再多做几组实验”，而是**每一类 claim 需要什么证据才能成立**。因此评估必须按四桶 claim budget 组织，而不是按系统 benchmark 惯性罗列。

对本文而言，最关键的三类 claim 分别是：

- `Guaranteed property`
- `G1`：`Eunomia` 要求 detached continuation 只有在 `Attached -> Issue -> Active -> Seal -> Cover -> Close -> Reattach` 合法闭环后才算完成；
  - `G2`：四条保证要求每个合法 reply 都能向后追到唯一 authority lineage，向前收口到 seal / cover / close；
  - `G3`：sealed era 的旧 reply 必须在 client / gateway / resolver 路径上机械 inadmissible。
- `Measured effect`
  - `M1`：root degradation 下保住了多少有用 singleton-duty utility；
  - `M2`：`ContinuationWitness + verifier` 在 steady-state 带来多少正常路径税；
  - `M3`：`Eunomia guarantees` 作为 bug-class vocabulary 能否在外部系统里抓到真实 violation schedule。
- `Design hypothesis`
  - `H1`：`authority-gap anomalies` 是一类跨系统 bug class，而不是 NoKV 局部命名；
- `H2`：对 multi-dimensional frontier duty 而言，只拥有 era check、rooted seal 或 successor coverage 中的一部分，仍无法在同一 fault vocabulary 下同时保证 `G1/G2/G3/G4`。
  - `H4`：工业界已经在不同系统里局部实现了 Eunomia primitive，但尚未有单一系统把它们统一成 finality-complete protocol。

一旦按这个骨架重排，评估的主轴就会很清楚：**先站住 `G1/G2/G3/G4`，再测 `M1/M2/M3`，最后再讨论 `H1/H2` 的外延。**

### 5.2 当前已经兑现的 guaranteed-property 起点

NoKV 现在还没有完整兑现 `Eunomia`，但它已经不再是 paper-only design。当前 artifact 至少已经兑现了三层起点：

- rooted takeover-critical state 已进入 `meta/root`，当前 artifact 中由 `Tenure / Legacy / Handover` materialize 出第一版 handover skeleton；
- same-holder rebuild、contested failover、leader change 与 chaos-style monotonicity tests 已经证明 authority lineage 不会在最基本路径上回退；
- `AllocID/Tso` 的 monotone-duty 路径以及 `GetRegionByKey` 的 metadata-answer 路径上，reply evidence 都已经开始进入 client-side verifier，而不再只是 observability 字段；metadata-answer 的 `serving_class / sync_health / freshness` 也开始由 provider 与 caller 共享同一套 contract projection，而不是各自维护一套漂移中的条件分支。

这组证据支撑的最强、也是当前最诚实的结论是：

> **NoKV 已经实现了 `Eunomia` 的 first rooted skeleton，并让 monotone duties 与 metadata-answer path 的 `ContinuationWitness` 都开始进入调用方判定逻辑；metadata-answer path 上 non-zero detached era 的 late-reply rejection 已经进入 client-side verifier，而 zero-era attached 路径也已经收口成显式 attached contract：只有 authoritative、fresh、healthy、root-lag 为 0 的 reply 才会被接受，并通过 attached floor 单调推进来拒绝 token / revision 回退。独立 `eunomia-audit` 现在已经有了 rooted-snapshot evaluator 与最小 reply-trace 输入的离线 CLI，但更强的 cross-service trace audit 与 negative-result formalization 仍未完成。**

换句话说，当前 artifact 已经足以防止“全文只是 naming exercise”这一类攻击，但还不足以让 `G1/G2/G3/G4` 的证据链完全闭合。

### 5.3 要把 `G1/G2/G3/G4` 站住，还缺三条 formal / checker 证据链

当前最值得投入的，不是更多系统吞吐图，而是下面三条直接绑定 correctness center 的证据链。

#### 5.3.1 Formal line：`Eunomia.tla` 与 contrast-family specs

必须把 `Eunomia + 四条最小保证` 从 prose 压成 machine-checkable object。第一步不是做一个庞大的全系统模型，而是先做两份最小 spec：

- `spec/Eunomia.tla`
  - 编码 `Attached -> Issue -> Active -> Seal -> Cover -> Close -> Reattach`；
  - 直接把 `Primacy`、`Inheritance`、`Silence` 与 `Finality` 写成最小 invariants。
- `spec/LeaseOnly.tla`
  - 刻意拿掉显式 `seal / cover / close`；
  - 允许 delayed old reply 在 successor 已出现后仍被观察到；
  - 作为 `H2` 的第一版 model-checked counterexample surface。
- `spec/TokenOnly.tla`
  - 只保留 bounded-freshness token；
  - 调用方按 lag budget 判新鲜度，但不跟 era lineage 绑定；
  - 用来暴露“freshness evidence 不等于 authority lineage”。
- `spec/ChubbyFencedLease.tla`
  - 保留 per-reply sequencer / highest-seen era 这类 disciplined client fencing；
  - 让 stale reply rejection 能站住；
  - 但仍故意不引入 rooted `seal / cover / close`，从而暴露 successor coverage 与 detached-period finality 仍缺显式对象。

当前 repo 里的 formal line 已经不再是单个正模型配一个 straw baseline：`Eunomia.tla` 在当前 `MaxEra=3 / MaxFrontier=2` 的配置下，已经从单次 cycle 扩成 repeated rooted handoff model；TLC 当前穷举出 `22876` 个 generated / `3924` 个 distinct states、深度 `20`，并保持 `G1/G2/G3/G4` 成立，同时也 machine-check 更强的 `G2_PrimacyInductive`。`LeaseOnly.tla` 与 `TokenOnly.tla` 都会给出 old-era reply 在 successor 出现后仍被交付的 counterexample；`ChubbyFencedLease.tla` 则进一步说明：即使 stale-reply rejection 由 per-reply sequencer / highest-seen discipline 站住，若没有 rooted `seal / cover / close`，successor 仍可能没有覆盖 predecessor 已经服务过的 frontier；`EunomiaMultiDim.tla` 与 `LeaseStartOnly.tla` 则已经形成正/反对称 artifact，说明同一 successor-coverage discipline 还能消费 CRDB `#66562` 这类 lease-start frontier。更重要的是，当前模型已经不再把 `reply` 当成单一槽位，也不再让 `Seal` 直接清空 reply；它显式区分了仍在网络中的 `inflight` replies 和当前正在被调用方接纳的 `delivered` reply，因此 `post-seal inadmissibility` 约束的是 caller admission，而不是假设 rooted `Seal` 会主动撤销网络中的旧 reply。当前这些 TLC / Apalache 结果也已以 sanitized artifact 形式 check-in 到 repo，而不再只是一次性命令输出。

但这条 formal line 现在仍然只是 **bounded model + stronger invariant skeleton**，还不是完整证明。它依旧保留了三个必须诚实承认的边界：第一，状态空间仍是 toy scale，而不是接近真实系统规模；第二，虽然 `Eunomia.tla` 已经支持 repeated handoff，但 TLC / Apalache 仍只在有限常量和有限长度下检查它，当前还不是 TLAPS / Basilisk 风格的真正 unbounded proof；第三，contrast family 现在虽然已经扩到 `LeaseOnly / TokenOnly / ChubbyFencedLease / LeaseStartOnly`，但仍只是 service-level family，而不是对工业级 Chubby/Spanner/Zanzibar 全部 consumer discipline 的完整反驳。换句话说，当前 formal line 的职责是**收紧边界、暴露 obligation、避免全文只剩 prose**，而不是单独完成 `H2` 的最终证明。

这条证据链的目的非常明确：**让 reviewer 不能再轻易把本文压回 “lease + fence + window 的重命名”。**

#### 5.3.2 Checker line：witness 必须真的被消费和拒绝

现在 monotone duties 上的第一版 client verifier 已经落地，metadata-answer path 上也已经接入了最小 client-side witness verifier；但当前 artifact 还缺完整 checker contract：

- `old-holder late reply rejection`
  - 需要端到端证明 sealed era 的旧 reply 在 failover 之后会被机械拒绝，而不是“理论上应当拒绝”；
- metadata-answer verifier
  - 当前已经把 `required_root_token / descriptor_revision / serving_class / sync_health` 接入 client-side admissibility；
  - 当前也已开始拒绝 non-zero detached era 的 stale metadata reply；
  - 仍需继续把 zero-era attached 路径建模清楚，并把更强 resolver/gateway consumption 补齐；
- gateway / resolver consumption
  - 需要至少有一条不是“客户端单点消费”的 verifier 路径，避免 witness 退化成只服务单一调用栈的局部字段。

如果 reply evidence 没有被下游实际消费，`ContinuationWitness` 仍然只是格式更漂亮的 telemetry。

#### 5.3.3 Audit line：`eunomia-audit` 必须成为独立 artifact

`HandoverWitness` 真正值钱的地方不在 server 端多写了几个 record，而在于 detached period 是否能在之后被独立审计。当前 artifact 至少要有一版最小 `eunomia-audit`：

- 输入：
  - handoff log
  - reply trace
  - seal / cover / close / reattach records
- 输出：
  - overlap across generations
  - uncovered frontier
  - post-seal legal reply
  - lineage mismatch
  - confirm-before-reattach

当前代码已经不再停在“服务内部观测”：finality 判断先被抽成 snapshot-level evaluator，随后又接成了离线 `nokv eunomia-audit --workdir ...` 最小 CLI；进一步地，reply-level anomaly 也已经有了最小 JSON trace 输入，可直接报告 `post-seal accepted reply` 这类 authority-gap 信号。说明 `eunomia-audit` 的消费面已经开始从 server diagnostics 走向“独立消费 rooted snapshot + 最小 reply trace”。当前 artifact 在外部系统这条线也已经超过单纯的 schedule replay：`etcd-read-index` trace 可以被投影成统一 reply-trace vocabulary，repo 内同时具备 schedule projector、live script runner、multi-member pause/resume harness、真实 delayed unary `Range` reply capture 与 raft WAL 摘要采集。同一 fault schedule 下，NoKV 还提供了一个 control experiment，结果为 0 anomaly。换句话说，当前 artifact 已经具备 `schedule -> raw trace -> checker` 与 `live capture -> raw trace -> checker` 两条可执行管线，而不只是 repo 内的测试重放。它当然还不是当前 upstream regression proof，也不是无代理自然迟到 reply 的抓包；但它已经把 `H3` 从“未来希望支持外部系统”推进到了“外部 trace 形状与 fault-injected live trace 都已经能被 checker 真消费”。更强的跨服务 trace、第二外部系统 adapter 与完整 anomaly vocabulary 仍未完成。

没有这一层，本文仍然更像“协议设计 + 集成测试”；有了这一层，它才开始像“bug class + audit framework + reference implementation”。

### 5.4 Measured-effect 评估应按 `M1/M2/M3` 组织

在 `G1/G2/G3/G4` 没站住前，性能图只会稀释中心贡献；但一旦 correctness center 成形，`M1/M2/M3` 就必须非常清楚。

#### 5.4.1 `M1`：utility preserved under root degradation

当前 artifact 只围绕两类 duty：

1. `AllocID / Tso`
2. `GetRegionByKey`

这是故意的 scope freeze，而不是保守。它刚好覆盖：

- monotone authority-bound continuation；
- 被 NoKV 架构绑定到 coordinator lease 上的 metadata-answer continuation。

这一组实验必须至少和以下基线比较：

当前 artifact 真正支持的不是 6 个外部系统 baseline，而是 **同一协议骨架内部的 ablation variants**。更准确地说，当前 `benchmark/eunomia` 测的是：

- fault scenario：`seal_path`、`late_reply`、`budget_exhaustion`、`root_unreach`
- protocol switch：`DisableSeal / DisableBudget / DisableClientVerify / DisableReplyEvidence / DisableReattach / FailStopOnRootUnreach`

因此 `M1` 当前回答的问题应写成：

> **在同一 rooted truth、同一 fault vocabulary、同一 client/runtime implementation 下，去掉哪一类 Eunomia primitive 会先丢失 utility，或者先越过 legality boundary。**

这比“拿 6 个外部 baseline 做一张统一柱状图”弱，但它和 repo 内实际 artifact 是一致的。`lease-only`、`token-only`、`cache-only`、`window-only`、`follower-serving / dissemination-only` 这些更宽泛的系统对照，目前只能作为 qualitative contrast 或 formal contrast 出现在讨论与 related-work 里，不能在 `§5.4.1` 里写成已经由当前 benchmark artifact 完整支撑的 baseline set。

也正因为 current artifact 已经把 `DisableSeal / DisableBudget / DisableClientVerify / DisableReplyEvidence / DisableReattach / FailStopOnRootUnreach` 这组 ablation switch 对象化，后续 `M1` 不需要再从零重构 protocol path；它要做的是沿着同一套 service/client switch，把 fault matrix 和 detached utility measurement 做完整，而不是继续扩大 baseline 名单。

#### 5.4.2 `M2`：steady-state tax of witness + verifier

这一组评估应单独测量 authority-bound continuation 的 verifier tax：

- `AllocID/Tso` 的 witness carrying + verifier 检查开销；
- `GetRegionByKey` 次轴 admissibility 检查相对 NoKV 自身无 verifier 形态的额外代价；对 APF / smart-client 这类多读 freshness contract，本身就不在 Eunomia 范围内，不应被误写成同一类 verifier tax 对照；
- 这些正常路径税是否被 budget amortization 吸收，还是已经高到让 `Eunomia` 只剩概念价值。

当前 repo 已经补了第一版 `AllocID/Tso/GetRegionByKey` witness/verifier tax benchmark harness，而且这组 benchmark 已经迁到独立的 `benchmark/eunomia` 子包，避免 correctness package 内混入 paper-facing microbenchmark。因而 `M2` 不再停留在纯口头计划；但当前 benchtime 仍然只是 smoke-run 级别，它的职责是先把“是否能独立拆出 verifier tax”这件事落实为 artifact，而不是提前包装成稳定性能结论。这里最重要的不是画很多 latency percentile，而是把 **加 verifier 与不加 verifier 的增量成本** 从总吞吐里单独拆出来。

#### 5.4.3 `M3`：cross-system bug-class evidence

如果 `Eunomia guarantees` 最后只能解释 NoKV 自己，它仍只是较强的 protocol artifact。要把同一 vocabulary 说明成跨系统 bug class，必须把它投到外部系统上，至少形成 2-3 个最小 violation writeup。目标不是“黑很多系统”，而是证明：

> **authority-gap anomalies 是跨系统 bug class；NoKV 只是第一个以 `Eunomia + 四条最小保证` 口径收口它的 reference implementation。**

当前 repo 已经有了这条线的一个更像正式 artifact 的起点：`eunomia-audit` 不仅能直接吃 `etcd-read-index` 形状的外部 trace，也已经支持 `etcd-lease-renew`。在 `benchmark/eunomia` 里，前者已经配齐 schedule projector、live script runner、multi-member pause/resume harness 与真实 delayed unary `Range` reply capture。具体来说，当前 read-index harness 运行在 3-node etcd `v3.6.10` cluster 上：研究者可以先用 schedule projector 写出最小 issue-like 顺序，也可以直接启动 live script 和 multi-member harness，通过 `SIGSTOP/SIGCONT` 暂停当前 leader，让 cluster 经历一次真实 election；随后一个只拦截 unary `Range` 的薄代理会从旧 leader 捕获真实 `RangeResponse`，在外侧暂存该回复，等 successor 推进 revision 之后再放行。与此同时，repo 里还会抓取各成员的 raft WAL 摘要，把 `HardState / entry tail / snapshot head` 作为外部证据一起记到 evaluation logs。这个 artifact 的意义不是证明“当前 etcd 还会自动吐出旧回复”，因为 reply delay 仍由 harness 诱导；但它已经把 external pilot 从“脚本重放一个旧 revision”推到了“捕获并延后释放一条真实 live etcd in-flight reply，并附带 WAL-side causality evidence”这一层。

lease path 上，这条线已经不再只是 `#15247` 风格的服务端 lessor race 讨论。当前 artifact 在未修改的 upstream etcd `v3.6.10` 上，仅用公开 `Grant/KeepAlive/Revoke` API，就能复现一个 client-visible buffered stale-success scenario：`Revoke` 已在更高 revision 完成之后，调用方仍可以从 `clientv3.KeepAlive` 的 buffered channel 中读到更早的 `TTL>0` success reply；`eunomia-audit` 会把它投成 `accepted_keepalive_success_after_revoke`。进一步地，一个本地 Layer-6 风格的 revoke-revision floor gate 会把同一 schedule 的 anomaly 降为 0。这一点的意义不是宣称“我们发现了一个新的 etcd server-side lessor race”，而是展示 **Eunomia 的 client-side witness verifier 正好能关闭这类 client-visible continuation legality gap**。因此，当前 etcd pilot 已经同时覆盖了两个真实子族：`#20418` 更接近的 **reply-level stale read-index family**，以及 `KeepAlive/Revoke` 更接近的 **client-visible stale-success family**。更关键的是，当前 artifact 还额外补了一个 NoKV control experiment：在同一 delayed old reply / successor observed era 的 fault schedule 下，NoKV 的 client-side verifier 会机械拒绝旧 reply，因此 `eunomia-audit` 对 NoKV control trace 报告 0 anomaly。这还远不是“etcd pilot 完成”：

- 它还不是 live etcd cluster replay；
- 当前 live smoke 只证明真实 etcd 响应已能进入 checker，不等于 issue 已经被重现；
- 当前 live script 仍是单节点、单 key、无 failpoint 的最小执行骨架；
- 当前 multi-member harness 已有真实 leader pause/resume、真实 delayed unary `Range` reply capture 与 re-election，但 delayed reply 仍不是 etcd 内部自动迟到，而是由 harness/代理主动扣留后再释放；
- 还没有 fault injector / failpoint harness；
- 还没有把 issue `#15247/#20418` 的公开 schedule 完整重建成 script。

但它已经让 `M3` 的最低落脚点不再是纯 prose：**external trace shape、adapter、anomaly vocabulary 与 repo 内可复现 fixture 已经开始对齐。**

### 5.5 Detection Methodology: eunomia-audit 如何识别这些 anomaly

Eunomia 除了定义 "什么是合法 continuation"，还**同时定义了如何检测违反**。`eunomia-audit` CLI 与 [`coordinator/audit/`](coordinator/audit/) 包组成 3 层 audit 消费面。

**Tier A — Snapshot audit**。给定一个 rooted snapshot（NoKV workdir 或任意第三方系统的 rooted state 投影），检查 snapshot-level anomaly。[`coordinator/audit/report.go`](coordinator/audit/report.go) 的 `SnapshotAnomalies` 提供 13 种 anomaly kind：

```go
type SnapshotAnomalies struct {
    SuccessorLineageMismatch    bool  // Inheritance violation
    UncoveredMonotoneFrontier   bool  // Inheritance monotone gap
    UncoveredDescriptorRevision bool  // Inheritance descriptor gap
    SealedEraStillLive   bool  // Silence violation
    ClosureIncomplete           bool  // Finality violation
    MissingConfirm              bool
    MissingClose                bool
    CloseWithoutConfirm         bool
    CloseLineageMismatch        bool
    ReattachWithoutConfirm      bool
    ReattachWithoutClose        bool
    ReattachLineageMismatch     bool
    ReattachIncomplete          bool
}
```

每个字段对应一个可机器判定的谓词，映射到 §3.6 的一条保证。

**Tier B — Reply-trace audit**。给定一段 reply trace（JSON 格式），逐 record 检查每个 reply 是否跨越 authority-transition boundary。当前 [`coordinator/audit/trace.go`](coordinator/audit/trace.go) 的 `EvaluateReplyTrace` 识别三类 anomaly；其中 successor-gap family 会按 adapter / duty 细化成更具体的名字：

```go
type ReplyTraceAnomaly struct {
    Index          int    // trace 中位置
    Kind           string // "post_seal_accepted_reply" / "accepted_read_index_behind_successor" / "accepted_keepalive_success_after_revoke" / ...
    Duty           string
    Era uint64
    Reason         string
}
```

**Tier C — Cross-system adapter**。[`coordinator/audit/trace_adapter.go`](coordinator/audit/trace_adapter.go) 支持多种 input 格式，把外部系统 trace 投影成 Eunomia 中立 schema：

```go
const (
    ReplyTraceFormatNoKV          ReplyTraceFormat = "nokv"
    ReplyTraceFormatEtcdReadIndex ReplyTraceFormat = "etcd-read-index"
    ReplyTraceFormatEtcdLeaseRenew ReplyTraceFormat = "etcd-lease-renew"
)
```

当前已经跑通两条 etcd adapter：

- `etcd-read-index`：把 etcd ReadIndex 相关事件（`member_id` / `read_state_era` / `successor_era` / `accepted`）投影成 `ReplyTraceRecord`
- `etcd-lease-renew`：把 `KeepAlive/Revoke` 相关事件（`member_id` / `response_revision` / `revoke_revision` / `accepted`）投影成 `ReplyTraceRecord`

未来扩展目标包括：

- `kafka-metadata`：Kafka controller metadata 事件
- `crdb-range-lease`：CockroachDB range lease 状态变化
- `yb-tablet-lease`：YugabyteDB master-reported tablet lease

**关键 invariant**：**anomaly kind 的根语义是 protocol-level 定义，但允许 duty-specific specialization**。NoKV 与 etcd 的 trace 都投影到同一套 `ReplyTraceAnomaly.Kind` 空间：generic family 仍保留 `accepted_reply_behind_successor`，而具体 duty 可以细化成 `accepted_read_index_behind_successor`、`accepted_keepalive_success_after_revoke` 这类更利于审计与写作的名字；这让 `eunomia-audit` 成为跨系统统一 checker，而不是一组 per-system ad-hoc grep rule。

**与 Jepsen 的类比**：Jepsen 把 linearizability / serializability 作为可判定 property，设计黑盒 workload 生成 trace 后做 model-checked 校验。`eunomia-audit` 把 Eunomia 作为可判定 property，同时消费 rooted snapshot 与 reply trace 做 deterministic audit。差别在于：Jepsen 针对已知 correctness class（linearizability），`eunomia-audit` 针对本文定义的新 class（Eunomia）。

#### 5.5.1 Detection 完整性声明

当前 `eunomia-audit` 的 detection 覆盖度：

- **Primacy**：snapshot 层检测 `SealedEraStillLive`；trace 层检测 `accepted_reply_behind_successor` family（例如 `accepted_read_index_behind_successor`、`accepted_keepalive_success_after_revoke`）
- **Inheritance**：snapshot 层检测 `SuccessorLineageMismatch` / `UncoveredMonotoneFrontier` / `UncoveredDescriptorRevision`
- **Silence**：trace 层检测 `post_seal_accepted_reply`
- **Finality**：snapshot 层检测 `ClosureIncomplete` / `MissingConfirm` / `MissingClose` / `ReattachIncomplete` 等 6 类

4 条最小保证在 Tier A + Tier B 检测面上**完整覆盖**——任何违反 Eunomia 的 trace 都会被至少一种 anomaly kind 捕获。**Tier C cross-system adapter 的缺口在于外部系统 schema 映射，不在 audit 算法**——即 adapter 补齐后，detection 语义保持统一。

#### 5.5.2 案例研究：etcd v3.6.10 buffered KeepAlive after Revoke

除了 `SIGSTOP/SIGCONT` + delayed `RangeResponse` 那条 fault-injected read-index live demonstration，当前 artifact 还在 **未修改的 upstream etcd `v3.6.10`** 上复现了一条更靠近 client API 的 authority-gap scenario。schedule 非常短：

1. `Grant(TTL=3)`；
2. `Put(key, lease)`；
3. 调用 `clientv3.KeepAlive(...)`，但先不 drain 返回的 buffered channel；
4. 调用 `Revoke(...)`，得到更高的 `revision=R3`；
5. 再从 channel 读取一条更早的 `TTL>0` keepalive success，且其 `revision=R2 < R3`。

这一现象的意义是：**服务端 lessor 已经在更高 revision 完成 revoke，但 client-visible API 仍允许调用方观察并接受来自更早 authority view 的 success reply。** `eunomia-audit` 会把这条 trace 投成 `accepted_keepalive_success_after_revoke`。更重要的是，当前 repo 还给出了一条最小的 Layer-6 风格 proof-of-concept：只要在 client side 维护一个 `revoke_revision floor`，并在向上层交付 keepalive success 前做一次 `response_revision >= revoke_revision` 检查，同一 schedule 的 anomaly 就会降为 0。

**Upstream filing**: 该 scenario 已于 **2026-04-19 作为 [`etcd-io/etcd#21638`](https://github.com/etcd-io/etcd/issues/21638) 公开 file**，标题 *"clientv3: LeaseKeepAlive channel may yield buffered pre-revoke success after LeaseRevoke returns"*，当前状态 **OPEN**，包含最小 Go 复现脚本和 observed output `ttl=3 success_revision=2 revoke_revision=3`。这让本文的 F1 finding 从"内部复现" 升级为 **community-visible 可独立复核的 upstream evidence**：任何 reviewer 都可以通过 issue 链接 + 内嵌的 ~50 行 Go 程序独立重现并检查我们对 Layer-6 gate 的 claim。

这里必须刻意把 claim 收紧：**这不是新的 server-side lessor race 证明**。当前 etcd server 端已经专门修过 renew/revoke race；我们这里观察到的，是一个基于 buffered channel 语义的 **client-visible continuation legality gap**（issue body 原文亦显式声明："This report is not claiming a new server-side lessor race. The issue is specifically about the client-visible `clientv3.KeepAlive` channel behavior after revoke."）。它的价值不在于"证明 etcd server 还坏着"，而在于展示 **Eunomia Witness Layer client-side verifier** 恰好给出了一种统一、机械、低开销的关闭方式。

### 5.6 当前最小证据门槛：哪些是必须先完成的，哪些是后置优化

如果要让本文的 claim 与 artifact 对齐，以下几项应被视为最小证据门槛，而不是 nice-to-have：

- `spec/Eunomia.tla` 给出 `G1/G2/G3/G4` 的第一版 machine-checkable skeleton；
- contrast-family specs 至少给出 `LeaseOnly / TokenOnly / ChubbyFencedLease` 三种旧范式 counterexample surface；
- monotone-duty `old-holder late reply rejection` 至少要通过一个显式 `F.delayed_reply` adversarial path 端到端打穿；
- `eunomia-audit` 能在日志/trace 上报告最小 anomaly 类别；
- 至少 2 个外部系统形成完整 violation writeup。

相反，下面这些都属于第二层优化：

- production trace replay
- Pareto killer figure
- concurrent work 包装
- 更一般的 detached writes
- scheduler ownership / placement publication

顺序不能反过来。**如果 formal、checker 与 cross-system evidence 还没站住，再多 benchmark 也只会把本文重新拉回“一个更完整的 control-plane 设计”。**

## 6. 讨论

### 6.1 为什么这比“rebuildable coordinator”更强

`rebuildable coordinator` 只回答：

- 崩溃之后能否安全恢复；
- singleton lower bounds 是否不回退。

`auditable authority handoff` 回答的是更强的问题：

- root 暂时不可达时，哪些服务仍然可以继续；
- 可继续的每次服务如何带协议证据；
- detached period 如何被审计并重新并入 root。

因此本文真正的升级，不是“把现有 workshop 结果做大”，而是**把 recovery 问题改写成 authority handoff / finality 问题。**

### 6.2 为什么已有方案单独都不够

如果本文只说“我们把几个已有想法放在一起”，那这篇论文不应该成立。要降低 prior-art risk，必须更严格地说明：**现有 pieces 各自解决一部分问题，但单独都无法形成 finality-complete authority handoff 的完整协议闭环。**

最接近的几类 pieces 及其缺口如下。

#### 6.2.1 Lease-only 不够

lease-only 方案可以回答“当前谁是 owner”，但回答不了：

- detached 期间可以继续到哪个 frontier；
- metadata-answer reply 是否仍然在可证明边界内；
- successor 如何审计 predecessor 已消费 budget。

因此 lease-only 最多提供 ownership gate，不能提供 finality-capable continuation contract。

#### 6.2.2 Token-only 不够

freshness token 可以回答“至少这么新”，却回答不了：

- 当前 reply 是否由合法 authority 发出；
- monotone duty 是否仍在 cert 允许的 frontier 内；
- detached period 是否能够在之后被 reattach / audit。

因此 token-only 可以描述新鲜度，不能描述 authority-bound answerability。

#### 6.2.3 Cache-only 不够

metadata cache 可以提高 availability，但如果没有显式 handoff 边界与 witness，它只意味着“本地有份旧答案”，而不意味着：

- 这份答案此刻仍有合法服务边界；
- 回答方确实没有越过 authority frontier；
- stale-but-usable 与 stale-and-must-stop 之间有清楚分界。

因此 cache-only 可以支持 best-effort service，不能支撑 auditable authority handoff。

#### 6.2.4 Window-only 不够

allocator window 可以把 remote rooted writes 退出 steady-state 热路径，但如果它只是 local optimization，就无法回答：

- 当前 window 是否仍受 live cert 保护；
- 不同 holder 的 budget 是否重叠；
- root 重连后 predecessor window 如何被 successor 覆盖并审计。

因此 window-only 最多改变代价形状，不能独立构成 authority-preserving delegation。

#### 6.2.5 真正的新边界

本文真正试图占住的边界，不是任一 piece 本身，而是：

> **handoff object、continuation witness 与 handover witness 是否能一起被 protocolized 成单一 control-plane artifact，并在 detached period 结束后被 rooted truth 合法 close。**

如果这个边界不成立，本文就会塌回 known pieces 的组合；如果它成立，本文讨论的就是一个独立而更强的问题类。

### 6.3 为什么这不等于 follower serving / cache / token 的简单组合

现有系统已分别展示：

- follower-serving metadata query
- metadata cache
- consistency token
- role split
- rooted log abstraction

但本文要主张的不是这些机制分别存在，而是：

**handoff object、continuation witness 与 handover witness 是否能一起被 protocolized 成单一 control-plane artifact。**

更重要的是，本文在这里必须主动收窄 uniqueness claim。对 **single-epoch / single-tuple frontier** 的 duty，例如一个单一 epoch tuple 就能完整描述 predecessor frontier 的 fetch/read 路径，Eunomia 往往是 overkill：TiKV `PR #6343/#9240` 或 Kafka `KAFKA-6880` 这类 path-specific patch，几十到一百多行逻辑就可能足够。但对 **multi-dimensional frontier** 的 duty，例如 timestamp cache、read summary、lease summary、tablet leader lease set 或 descriptor publication frontier，piecemeal patch 的 recurrence 明显更高，因为 predecessor frontier 不是单个整数，而是集合、区间或 lattice。本文真正要 defend 的 therefore 不是“Eunomia 对所有 authority transition 都必需”，而是：**在 multi-dimensional frontier duty 上，Eunomia 是第一个把 rooted seal、era-based inadmissibility、successor coverage 与 finality-complete audit 放进同一协议的统一解。**

如果没有 handoff object、continuation witness、successor coverage 与 handover witness 这四层同时成立，continuation 就仍然是局部实现技巧，而不是新抽象。

### 6.4 为什么 scope 必须收窄

这类题目的最大风险不是“idea 不够大”，而是 “scope 爆炸后 artifact 变虚”。因此本文刻意做三层收缩：

1. 主轴只保留 `AllocID / Tso`，`GetRegionByKey` 只作为 NoKV 架构下的次轴示例
2. 本文只主张 finality-complete continuation，而非 full control-plane availability
3. rooted authority 本身仍被假设为正确复制，不在本文内重做 consensus

除此之外，scope 还必须在 duty 语义上再收一层：**Eunomia 不是给所有 control-plane duty 用的，而是给 authority-bound continuation 用的。** 如果一个 duty 天然可以 multi-reader 并发服务、每次调用都是 stateless answer、freshness contract 已足够保证正确性，那么它压根不需要 seal / close，也不属于本文问题。只有当 duty 的 service 形态本身绑定在 authority holder 上，且换 holder 时必须回答“old reply 还能不能被接受、successor 需要覆盖 predecessor 什么 frontier、detached segment 如何完成 finality”时，Eunomia 才有意义。

除此之外，本文还额外做了一层 **frontier-type 收缩**：我们不主张 Eunomia 在所有 authority-bound continuation 上都比 path-specific fencing 更优。更准确的说法是：

- 对 single-tuple frontier，per-RPC epoch / term check 可能已经足够；
- 对 multi-dimensional frontier，piecemeal fencing / local workaround 更容易 recurrence；
- 本文 claim 的 novelty 与 necessity 主要落在后者，而不是前者。

这种收缩不是退让，而是为了让 claim 真正可 defend。

### 6.5 设计权衡

当前设计刻意选择了 **implementability 优先于极致最小性**。这意味着 paper 必须主动承认几处 trade-off，而不是假装它们不存在：

- `Frontier` 目前仍以 `AllocID / Tso / GetRegionByKey` 三个具体 duty 实例落地，而不是已经把 persisted schema 完全做成 duty-generic algebra。这个选择服务于当前 artifact 的可实现性，但也意味着当前 scope 必须写窄。
- monotone legality witness 已经收敛到 `era + consumed_frontier`；这让协议核更干净，但也意味着 paper 不能再把旧版 `budget_epoch` 写成当前 artifact 的中心字段。
- rooted lifecycle 在实现上仍保留 `Seal / Close / Reattach` 这些工程上易落地的分段对象，而不是把所有 finality event 折叠成理论上更小的单对象状态机。这个选择偏实现清晰，而不是最小事件数。
- ablation 目前仍以一组小范围 experimental switches 驱动；当前代码已经开始补命名 preset 与合法性检查，但还没有把整个实验面完全提升成 paper-model enum。

这些都不是当前 artifact 的致命问题；但它们解释了为什么本文应被定位为 **good design, not yet perfect design**。当前阶段的目标是先把 correctness class、checker 与 reference implementation 站住，而不是一次就把所有 persisted algebra 做到最泛化。

### 6.6 当前局限

当前 artifact 仍有明确边界：

- rooted allocator state 仍是 NoKV-specific truth，而非完全 duty-generic schema
- `GetRegionByKey` 侧的 metadata-answer admissibility 已落成 client verifier，但仍只是次轴证据
- `seal / confirm / close / reattach` 已形成第一版 handover skeleton，且 `eunomia-audit` 已同时支持 snapshot-level 与 trace-level audit；但更强的跨系统 adapter 和自然 transport 证据仍未完成
- 主要结果仍来自 localhost、targeted integration tests 与 issue-line live harness，尚缺 live CRDB replay、multi-host 与 WAN 证据

因此当前最诚实的定位是：

> 本文提出一类更强的问题与协议方向；NoKV 当前 artifact 已经实现最小 rooted substrate，并验证了 authority handoff / finality 所需的关键起点，但尚未完成完整 claim 所需的全部证据。

## 7. 相关工作与差异边界

### 7.1 相关工作必须按“离 `G1` 还差什么”分组，而不是按系统名字排队

如果按系统名逐个 survey，reviewer 很容易把本文读成“把 Chubby、Spanner、PD、KRaft、Scylla、Consul 各拿一点再重新命名”。更稳的写法是直接按 **离 `Eunomia + 四条最小保证` 还差什么** 来分组。

| 组别 | 代表工作/系统 | 已经很强的轴 | 仍缺什么 | 与本文的核心差异 |
| --- | --- | --- | --- | --- |
| authority transfer / truth placement | Chubby, Spanner/Megastore, Delos, FoundationDB, etcd, KRaft, Scylla | fencing、epoch/time bound、truth placement、recovery authority | detached continuation 的显式 frontier 与 finality | 它们强在“谁是 authority”；本文要占住“authority gap 结束时如何合法收口” |
| bounded continuation / reply evidence | Zanzibar, Consul, CockroachDB, PD/APF, CephFS/RGW, Pulsar, Cassandra/DSE | freshness token、stale read、metadata dissemination、partial answerability | authority lineage、successor coverage、post-seal inadmissibility | 它们强在“还能答多少、答得多新”；本文要再加“为什么此刻合法、之后如何完成 finality” |
| verification / checker | CCF, RPRC, Remix, T2C, Basilisk | trace validation、runtime checker、spec-to-code alignment | 新语义本身的定义 | 它们主要检查已有语义；本文试图定义 `Eunomia + 四条最小保证` 这组新的 service-level semantics |
| 本文 | `Eunomia + 四条最小保证 + eunomia-audit + NoKV` | finality as safety property；checker-consumable lineage semantics | 尚需 formal、checker、cross-system evidence 完整落地 | 不是更好的 lease，而是更窄但更硬的 authority-gap correctness class |

因此，本文真正要 defend 的不是某个 token、window 或 cache，而是：

> **在 rooted authority 仍然正确存在时，authority-gap anomalies 能否被 `Eunomia + 四条最小保证` 统一刻画，并由 `eunomia-audit` 与 NoKV 共同消费。**

如果把 `finality` 也按强弱层次重新摆放，相关工作的真实边界会更清楚：

| 系统/路线 | 当前 finality 形态 | 局部性 | 残余 gap | 与本文差异 |
| --- | --- | --- | --- | --- |
| TiDB APF | KV-layer reject + client-go fallback | per-layer、隐式 | PD 层仍允许 bounded stale，finality 不在同一 protocol object 内 | 本文要把 finality 提升到 protocol-level，而不是留给下游兜底 |
| Consul | `stale/default/consistent` + `LastContact/KnownLeader` | API-contract | legality 由调用方解释，而非 rooted close/seal | 本文要让 admissibility 与 lineage 在协议内闭环 |
| CockroachDB | `#36431` 为 observed-ts path 提供结构性 seal；`#66562` 仍 open | per-path | successor coverage 仍非统一 guarantee | 本文要统一不同 path 的 finality 语义，而不是逐 issue 打补丁 |
| etcd | leader / lessor 边界被持续加固 | semi-structural | 不同 trigger 反复命中同类旧 era 行为 | 本文直接把这类 gap 升成显式 handoff / finality 语义 |
| KRaft | epoch + partial seal | partial spec realization | `KAFKA-15911` 仍 unresolved | 本文把“spec 里想到 finality、实现里漏掉”变成 checker-consumable invariant |
| YugabyteDB | workaround + deployment discipline | trigger-specific | `#24575` 说明 class-level finality 仍未形成 | 本文要的是 class-level finality，而不是单 trigger workaround |
| NoKV | rooted `Seal / Finality` + monotone-duty verifier | protocol skeleton | metadata-answer verifier 已起步，`eunomia-audit` 已能消费 rooted snapshot、最小 reply trace 与 fault-injected live etcd trace；但仍缺无代理自然迟到 reply 捕获、第二外部系统 adapter 与完整 formal | 当前最接近 unified protocol-level finality，但仍是 first rooted skeleton |

### 7.2 许多工作已经把 `authority transfer` 做得很强，但没有把 `finality` 做成 safety property

Chubby、Spanner/Megastore、Delos、FoundationDB、etcd、KRaft 与 Scylla 这一组工作都非常强，而且必须正面承认。它们已经回答了很多本文绝不能重复 claim 的问题：

- Chubby 回答了 stale actor exclusion 与 fencing；
- Spanner/Megastore 回答了 epoch、lease 与 time-bound consistency；
- Delos 回答了 truth/service split；
- FoundationDB 回答了 recruited singletons 与 deterministic recovery engineering；
- etcd 与 KRaft 回答了 metadata quorum、leader/controller transition 与 fail-stop boundary；
- Scylla 把 eventually consistent metadata 收回强一致 authority。

但这组工作的共同特点也很清楚：**它们的中心都不是 detached period 的合法 continuation，更不是 detached period 的合法 finality。** 换句话说，它们强在“谁是 authority、何时必须停、truth 放在哪里”；本文想多占住的一层则是：

- continuation 必须绑定 handoff lineage；
- reply 必须携带可消费的 legality witness；
- predecessor 必须被 seal；
- successor 必须显式 cover predecessor frontier；
- sealed old-era reply 之后必须变成 mechanical reject。

这就是为什么本文不能把自己写成“更好的 lease/epoch/fencing”，而必须写成 **finality as first-class safety condition**。

### 7.3 另一组工作已经把 `bounded continuation` 或 `reply evidence` 做强，但这里必须区分 bug、tradeoff 和局部 finality

Zanzibar、Consul、CockroachDB、PD/APF、CephFS/RGW、Pulsar、Cassandra/DSE 这组近邻展示的不是 authority transfer，而是另一条现实压力：**在不完全新鲜、甚至 authority 不完全稳定时，系统仍然想回答一部分请求。** 但这组材料不能再被一锅端地写成“全是 bug”；更准确的说法是：其中一部分是历史 bug，一部分是公开 tradeoff，还有一部分已经体现了 path-specific、layer-specific 或 procedure-specific finality guard。

这条线上，现有工作已经给出大量局部强解：

- Zanzibar 的 zookie 是非常强的 freshness evidence；
- Consul 公开承认 `stale/default/consistent` 三种读契约，并暴露 `LastContact/KnownLeader` 让调用方自行决定；
- CockroachDB 一边通过 cache/dissemination 处理 metadata answerability，一边又在 `#66562`、`#36431` 这类 issue 中暴露 lease frontier 的现实缺口；
- PD/APF 说明 metadata-answer path 的 bounded stale / reject / fallback 在工业界非常真实，而且它已经在 KV 层形成一版局部 finality guard；
- CephFS/RGW 说明 metadata authority gap 会演化成 pause、failover 与 master-promotion discipline，其中 RGW 的 multisite 文档本质上是一份 operator-side finality runbook；
- Pulsar、BookKeeper 与 Cassandra/DSE 则说明 cached owner lookup、schema agreement 与 later invalidation 是长期现实。

但它们大多还停在：

- “可以答”
- “答得有多新”
- “答错后如何 repair / retry / failover”

而不是：

- “为什么这次 reply 仍属于合法 authority lineage”
- “successor 是否覆盖 predecessor sealed frontier”
- “旧 era reply 在 finality 之后是否机械 inadmissible”

因此，这一组工作确实会吃掉本文的 stale/cache/token/window 叙事；但它们仍没有把这些局部 contract 统一成 `Eunomia guarantees` 意义上的 finality-complete semantics。更直白地说：**它们已经证明工业界在做 finality，但做法离散、层次不一、对象不统一。**

### 7.4 最近两年的 verification / checker 工作很强，但它们主要检查已有语义，不定义新语义

这两年 CCF Smart Casual Verification (NSDI'25)、RPRC (NSDI'25)、Remix (EuroSys'25)、T2C (OSDI'25) 与 Basilisk (OSDI'25) 共同打开了一条新 research genre——**"define correctness object + build checker + find bugs in real systems"**。本文必须明确与这 5 篇的 positioning，否则极易被看作这条 train 上的 follower。

#### 7.4.1 逐篇定位

- **Remix (Ouyang et al., EuroSys'25)** — *Multi-Grained Specifications for Distributed System Model Checking and Verification*。核心：用**分层粒度的 spec** 提高 model checking 的实用性，在 ZooKeeper 中找到 **6 个历史未发现 bug**。和本文相似：both formal + empirical。**关键区别**：Remix 验证 **ZooKeeper 已有的 spec**（其 spec 作为 ground truth），本文定义 **新的 service-level correctness class（Eunomia）**——Remix 的 spec-refinement 框架原则上**可以被用来 validate Eunomia**，这反而说明本文为 future Remix-style 工作提供了 target。
- **Basilisk (Zhang et al., OSDI'25)** — *Using Provenance Invariants to Automate Proofs of Undecidable Protocols*。核心：用 **provenance invariant** 自动证明无限状态分布式协议。和本文相似：**把 invariant 作为 first-class proof object**。**关键区别**：Basilisk 针对 **consensus-level correctness proof**（Paxos 变种），本文针对 **service-level correctness class**——它们在**抽象层上正交**：Basilisk 证明 consensus 正确后，本文回答 "consensus 正确的前提下 service layer 是否合法"。两者可以 stack——**Eunomia 的四条保证将来可用 Basilisk 风格 provenance invariant 做 unbounded proof**。
- **T2C (Lou et al., OSDI'25)** — *Deriving Semantic Checkers from Tests to Detect Silent Failures*。核心：从 system tests 中**挖掘并泛化** invariant，变成 production runtime checker。在 HDFS / YARN / Cassandra / ZooKeeper 上抓到 **15 个 silent failure**。和本文相似：both 有独立 checker artifact。**关键区别**：T2C 从**已有 test 抽取** invariant，本文**显式定义** invariant（四条保证）——T2C 的方法回答 "invariant 藏在哪里"，本文回答 "invariant 应该是什么"。**二者互补**：未来 T2C 风格工作可以反向审计 Eunomia guarantees 是否已在现有 system tests 里隐含存在。
- **CCF Smart Casual Verification (Howard et al., NSDI'25)** — 把 CCF（Microsoft 机密共识框架）的 **TLA+ spec 与 C++ impl 绑定**，trace validation 接入 CI，prevented multiple production bugs。和本文相似：formal spec + impl trace。**关键区别**：CCF 针对 **单一系统自己的 spec**（vertical），本文针对 **跨系统 authority-gap class**（horizontal）。**CCF 的 CI-integrated trace validation 给本文 Tier B live pilot 提供直接模板**——我们可以借鉴其 trace validation 技术栈。
- **RPRC / Ellsberg (Ding et al., NSDI'25)** — *Runtime Protocol Refinement Checking*。核心：**不改目标系统代码**，只抓 message trace，check 实现是否 refine 标准 Raft。在 etcd / ZooKeeper / Redis Raft 上抓到多个 refinement violation。和本文相似：black-box cross-system。**关键区别**：RPRC 针对 **consensus protocol refinement**，本文针对 **service-level authority continuation**。RPRC 的 "不改被测系统代码" 原则直接对应本文 Tier B/C pilot 的 adapter 设计约束。

#### 7.4.2 本文和这 5 篇的关系图

这 5 篇组成一个抽象金字塔，本文**不是复制哪一篇，而是占住它们之间的新坐标**：

```
        Consensus-level proof (Basilisk)
              ↑
     Consensus-level spec (CCF, RPRC, Remix)
              ↑
     [Service-level correctness class: Eunomia]  ← 本文
              ↑
     Application-level invariant (T2C)
```

**Eunomia 填补了"consensus 已正确 ↓ application 已正确"之间的 service-layer 空白**——这正是 authority-gap anomaly 真实发生的 layer。

#### 7.4.3 positioning statement

本文因此相对这 5 篇的精确 positioning 可以压成一句话：

> *"Where Remix / RPRC / CCF validate existing consensus specifications and T2C mines invariants from existing tests, we define a new service-level correctness class (Eunomia) that these verification frameworks could in turn validate. Where Basilisk automates proofs for existing consensus protocols, our Authority Lineage Invariants define a new target that Basilisk-style provenance automation could discharge at unbounded scale. Eunomia is complementary to, not a follower of, this recent wave—it defines the correctness object these checkers can consume."*

这段定位的作用，是把本文从“另一篇 checker paper”收回到“定义 correctness object”的位置。

#### 7.4.4 本文可从这波 paper 借鉴什么

虽然 positioning 上不同，但这 5 篇的工程实践可直接借用：

- **CCF 的 TLA+ trace validation** → 本文 §5.3 formal line 与 §4 impl 之间的 bridge 工作可以直接沿用此模式
- **RPRC 的黑盒 adapter 设计** → 本文 Tier B/C live pilot 的 etcd/CRDB adapter 可以借其 schema
- **T2C 的 invariant 泛化** → 本文 future work 可以反向从 NoKV 现有 integration test 抽 Eunomia 隐含 invariant
- **Remix 的 multi-grained spec** → Eunomia.tla 未来扩展可采其分层策略减少 state explosion
- **Basilisk 的 provenance invariant** → 四条保证未来做 unbounded proof 时可借其 automation 技术

**本文不装 orthogonal；同时也不装 follower——是把这 5 条路径的 "correctness object that deserves verification" 位置正式命名并实现**。

#### 7.4.5 Classical prior art: consistency classes, view-change, 与 invariant taxonomy

§7.4 前四小节定位的是 2024-2025 的近邻 verification paper。但还有一组**经典 prior art** 和本文形式上更像、甚至在 reviewer 心里第一反应就会联想到的 —— 如果我们不正面承认 + 拉清边界，"novelty" 会直接被打回 5/10。这一小节处理 3 个必须点名的老祖宗。

**vs Bayou session guarantees** (Terry et al., PDIS 1994)

Bayou 定义了 4 条 client-centric session guarantees —— `Read-Your-Writes`、`Monotonic Reads`、`Writes-Follow-Reads`、`Monotonic Writes` —— 每条都是可机械 check 的 predicate。**形式上**，这和本文 4 条保证 (`Primacy`、`Inheritance`、`Silence`、`Finality`) 几乎一模一样：都是"4 条 axiomatic predicate 定义一个 consistency class"。

**关键差异**（本文 vs Bayou）在于 **observer axis**：

| 维度 | Bayou session guarantees | Eunomia guarantees |
|---|---|---|
| Observer | 单个 client 的 session | authority lineage 本身 |
| 问题 | "这个 client 看到的顺序是否自洽" | "这个 reply 是否跨越 authority transition 边界" |
| 不相容 | Bayou 关心 client 在不同 server 间看到的 read/write 序 | Eunomia 关心 server 产出的 reply 跨 authority era 是否仍被接受 |

CRDB `#66562` 可以直接证明这两个维度的正交性：`#66562` 是**单 client 单 key 的 read-then-write**，没有 session ordering 违反；Bayou 的 4 条 guarantees 全部可以 trivially 满足。但**authority boundary 被越过了**——这是 Bayou 的 vocabulary **无法表达** 的 anomaly。

**vs Viewstamped Replication view change** (Liskov & Cowling, 2012 revision)

VR 的 view change 协议**已经**有"旧 view 被 retire"这个显式概念。每次 view change 后，old view 里 pending 的 request 必须失效 —— 这听起来就是 `Silence` 的 consensus-level 祖先。审稿人看到 Eunomia 的 finality 第一反应会是 **"这不就是 view change 的 service 层泛化"**。

**关键差异**是 **frontier scalarity**：

- VR view 只用一个 `(view_number)` scalar 标识 old vs new；retirement 的语义是"epoch > old_epoch 即 new"
- Eunomia 处理的 frontier 是 **multi-dimensional**：predecessor served-read summary 是一组 `(key, ts)` tuples、range lease 的 coverage 是 `(start_key, end_key)` 区间、duty frontier 是 `{AllocID, TSO, GetRegionByKey, LeaseStart}` 的映射

CRDB `#66562` 就活在这个差异上：成功的 successor campaign 既要 `epoch > predecessor_era`（VR 能表达），**又要** `successor.lease_start > max(predecessor.served_reads)`（VR **无法表达**，因为 served-read frontier 不是 scalar 而是 summary）。VR 作为 consensus 协议足够工作；但 VR 的 formalism 作为 service-level class **不足以覆盖 multi-dim frontier**。

**vs Kondo invariant taxonomy** (Zhang et al., OSDI 2024)

Kondo 提出 `Regular Invariants`（Ownership、Monotonicity、Message）+ `Protocol Invariants` 的分类学，目标是**加速证明现有 consensus 协议的正确性**。这个分类方法论在形式上也和本文四条保证很像：都是"一组 named invariant 加 taxonomy"。

**关键差异**是**层次**：

- Kondo 是 **consensus-level methodology**: 用 taxonomy 帮你证明一个 Raft / Paxos / VR 变种的 safety 性质，且这个性质由**协议设计者**给出
- Eunomia 是 **service-level correctness class**: 定义**在任何 correct consensus 之上**依然可能被违反的一组 property。Eunomia 显式 assume consensus 已经正确（paper §2.2 "rooted authority 本身仍提供持久化与复制语义"）

换句话说 Kondo 和 Eunomia **在同一个 artifact 中可以叠加**: Kondo 帮你证 raft 正确 → raft 把 rooted authority 保证好 → Eunomia 继续在 service 层捕捉 rooted authority 正确也救不了的 bug（如 etcd `#15247` 中 raft 已切换但 lessor 持 stale view 的 race）。这两者在**抽象层次上正交**。

**vs Kleppmann fencing token** (Kleppmann 2016)

Kleppmann 的 fencing token 是业界最被广泛引用的 **per-action** stale-reply rejection 机制。本文 §7.6 已经把它列入 "partial reinventions of Eunomia primitives"，这里再补一条 structural difference：

- fencing token = **per-action unilateral check**（每次 I/O 自己携带 token，resource 侧看到更小的 token 就拒）
- Eunomia finality = **一次性显式 retirement event**（gen Y 正式 closed 后，所有 cert < Y 的 reply 机械失效，不需要每次单独 per-action check）

前者是"分摊到每次动作的防御"；后者是"一次性声明的 boundary"。ChubbyFencedLease.tla contrast spec 正是保留 per-reply sequencer + highest-seen discipline（= fencing token 的 machine-checked 版本），**去掉** finality event —— 仍然产出 counterexample。这是"Eunomia 不等于 fencing + rebrand" 的**机器证据**。

**小结**：Bayou / VR / Kondo / Kleppmann 合起来**不覆盖** Eunomia。每一条都只沾到 Eunomia 的一面：Bayou 沾 4-axiom 形式、VR 沾 retirement event、Kondo 沾 taxonomy 方法、Kleppmann 沾 stale-reply rejection。**没有任何一条同时有这 4 个属性**：(1) service-level (not consensus-level) + (2) authority-lineage-centric (not client-centric) + (3) multi-dim frontier (not single epoch) + (4) explicit finality event (not per-action fencing)。这就是本文四条保证的 novelty boundary。

### 7.5 authority-gap anomalies 的 primary-source 分档：哪些是严格 bug，哪些只是 tradeoff 或 procedure

如果只有架构图和设计文档，reviewer 仍可能说本文只是在包装一个“理论上可能存在”的压力。更硬的写法不是“多引用几个系统”，而是把证据严格分档。

#### 7.5.1 Strict positive evidence：真正应计入 bug-class 的样本

经过更严格的 primary-source 清理后，当前最干净、最值得放进正文主线的 strict-hit 集合已经扩展到 **14 个样本、跨 7 个系统**：

- **etcd（3）**
  - `#15247`：Raft 层已经 step down，但旧 lessor 仍继续 keepalive / revoke；
  - `#12528`：高 CPU trigger 下旧 leader step down 后仍 revoke lease；
  - `#20418`：process pause 后 revision 长时间不前进，却仍接受 read / write。
- **Kafka KRaft（4 strict + 1 adjacent）**
  - `KAFKA-15489`：两个不同 epoch 的 controller 并存，并把 stale metadata 返回给客户端；
  - `KAFKA-14154`：controller soft failure 后 stale controller 仍推动 ISR / leader 相关状态；
  - `KAFKA-16248`：zombie leader 仍接受 fetch leader epoch 并返回错误 offset range，最终只能靠 consumer 侧 cache/workaround 补洞；
  - `KAFKA-6880` / KIP-320：zombie replica 必须被 fenced，这一条是更早、也更结构性的 same-class 前史；
  - `KAFKA-15911` **不再计入 strict positive set**，而是降级为 adjacent residual gap：它更像 follower-progress / audit gap，而不是干净的 stale-era reply signature。
- **TiKV（2）**
  - `PR #6343`：leader transition 后 follower read / ReadIndex path 的 term-check 漏洞；
  - `PR #9240`：transferring leader 之后 stale read index 再次出现。
- **CockroachDB（2）**
  - `#66562`：新 lease 没有覆盖旧 lease 已服务的 future-time read frontier，且 issue 长期开着；
  - `#23749`：更早的 observed timestamp / successor coverage 问题，说明这类 frontier gap 不是单次历史事故。
- **YugabyteDB（2）**
  - `#20124`：leader change 后 tracked leader lease 可能 stale；
  - `#24575`：RF 变化后类似 leaderless-tablet 错误 recurrence，runbook 仍要求 operator step-down。
- **MongoDB（1）**
  - `SERVER-17975`：node 认为自己仍是 primary，于是 safe-service read without quorum confirmation，直接落在 authority-gap signature 上。
- **FoundationDB（1 supporting lead）**
  - 公开资料显示 old GRV proxy 仍可能给出 stale read version；这条当前更适合作 supporting lead，而不是 canonical strict bug。

这一组材料的共同价值在于：**它们不是 tradeoff，也不是 operator manual；它们是公开 bug / issue / PR / release-note evidence，直接表明 rooted authority 正确存在时，service layer 仍可能跨 era 或跨 predecessor frontier 回答。**

更重要的是，maintainer 公开讨论已经不止一次承认“当前 fix 不是 general fix”。这类原话非常值得在 receipts 附录里保留：

- etcd `#20418`：ahrtr 明言 *“I'm still not convinced myself that the PR fixes the root cause of the issue.”*
- etcd `#15247`：mitake 明言 *radical long-term solution* 被暂时搁置，当前 merged fix 只覆盖 practical cases；
- etcd `#12528`：tangcong 直接说 *“we need to spend more time to find a simple and safe way to fix it”*；
- CockroachDB `#66562`：andreimatei 认为 *“This issue should stay open ... the issue has good discussion.”*

这些 quote 的价值不在于“多几句狠话”，而在于它们把 `H2` 从作者视角的负结果怀疑，抬成了 maintainers 自己公开承认“局部修复不等于 general finality”的实证。

此外，当前 artifact 还在 **未修改的 upstream etcd `v3.6.10`** public client API 上复现了一条 client-visible scenario：调用方在 `Revoke` 已返回更高 revision 之后，仍可从 `clientv3.KeepAlive` 的 buffered channel 里读到更早的 `TTL>0` success reply。**该 scenario 已于 2026-04-19 作为 [`etcd-io/etcd#21638`](https://github.com/etcd-io/etcd/issues/21638) 公开 file，当前 OPEN**。我们仍然刻意**不**把它计入上面的 14 个 strict-hit（那 14 条都是**历史已 merge / unresolved** 的 issue/PR），而是把 #21638 单独定位为 **"Eunomia-guided new finding in vanilla upstream"** 证据；上表的 14 个条目与 #21638 加起来就是 paper 当前在工业系统上的全部 community-visible evidence。

#### 7.5.1a Negative evidence from industry taxonomies: 为什么 "class naming" 本身就是贡献

如果 authority-gap anomalies 真的是一类复发 10 年的 bug class，为什么业界最权威的 reliability taxonomies 从来没给它命名？我们把这个问题正面回答：**因为"class-level naming" 和"列举 bug 实例"是两种不同的 intellectual 动作**，工业 taxonomy 传统上只做后者，而本文的核心贡献恰好是前者。

具体取证：我们检查了 2025 年两个业界最权威的 reliability glossary：

- **Jepsen Distributed Systems Glossary** (Kingsbury, 2025-07-15)
- **Antithesis Distributed Systems Reliability Glossary** (Antithesis + Jepsen, 2025-10-20)

两者合计覆盖 **100+ 条**目，包括 concurrency theory、consistency models、various faults、testing approaches。但我们系统性检索这两个 glossary 的结果是：

- **没有 "stale leader" 条目**
- **没有 "zombie leader" 条目**
- **没有 "authority transition" 条目**
- **没有 "finality" / "fencing completion" 条目**
- **没有任何 multi-invariant class 把 "stale-reply-after-leader-change" 作为一个命名的 bug-class family**

Antithesis glossary 在 `Clock Drift` 条目下**提到过**这个 phenomenon："a Raft implementation might use timeout-based leases to allow leaders to respond to read requests without checking with other nodes first. If clocks were to drift, an old leader could believe that it still held a lease while a new leader had actually been elected." —— 但这是**作为 Clock Drift 的后果**被讨论的，**不是作为独立命名的 class**。

这个 negative finding 有三层意义：

1. **本文的 class naming 不是重复劳动**。14 个 strict-hit bug 都能被复现、被 Jepsen 测到，但它们在工业 taxonomy 里**以单独 bug 身份独立存在**，而不是被视为同一 class 的不同 instance。把离散 bug 组织成一个 named correctness class 本身就是 intellectual contribution，而不是 "packaging existing work"。
2. **Eunomia 的 correctness-class-level framing 补的正是这个 gap**。Jepsen/Antithesis 擅长 "描述 + 复现 bug"，Kondo/Basilisk 擅长 "证明 consensus 协议"；**本文擅长 "把 service-level 发生的 bug class 变成机器可 check 的 predicate 集合"**，这是前两者空出来的 layer。
3. **审稿人可以直接交叉验证**。paper 引用这两个 glossary 的 URL 就是 falsifiable claim —— 任何 reviewer 只要在 2025 年这两个 URL 上搜我们列出的 class 关键词，就能立刻验证 "没有命名" 这件事是真的。

换言之：**如果业界已经有 name 了，我们就没 contribution；业界还没 name，本文第一次给 name 就是 contribution**。这和 linearizability (Herlihy & Wing 1990) 在它的年代也是 "给一个 practitioners 已经 vaguely 感受到的 phenomenon 一个 precise class 名字" 是同构的贡献 shape。

**Filed-but-unnamed 的补充证据**：就在我们整理本文 F1 (`etcd#21638`) 期间，我们又一次正面验证了上面第 1 点 —— 即使我们给 etcd 正式 file 了一个 Eunomia-guided finding，issue 本身仍然只能在 **"LeaseKeepAlive channel may yield buffered pre-revoke success"** 这种**描述性短语**下存在；社区没有、也没法用一句话把它 map 到一个已命名的 authority-gap class。这正是 "class-level vocabulary 缺位"的 operational 证据：**即便 bug 已被报告，仍然没有 class-level name 去组织它**。Eunomia 的 `accepted_keepalive_success_after_revoke` anomaly kind 正是对这个缺位的补充。

#### 7.5.1b Per-Bug Eunomia Defense and Detection Catalog

为了让 paper 可防御到 per-bug 粒度，下表给出 14 个 strict-hit + 1 supporting lead，再加 1 个当前 artifact 自己复现出的 untracked live finding 的完整四元组（**trigger / bug signature / Eunomia 协议防御 / eunomia-audit 检测规则**）。每个 anomaly kind 都对应 `coordinator/audit/report.go` 的 `SnapshotAnomalies` 或 `trace.go` 的 `ReplyTraceAnomaly` 字段。

| Bug | Trigger | Bug signature | Eunomia 协议防御 | eunomia-audit 检测规则 |
| --- | --- | --- | --- | --- |
| **etcd `#15247`** | slow fdatasync → lessor 与 raft desync | Raft 已 step down，旧 X lessor 仍 emit revoke | Gate Layer `eunomiaGate` 查 `HolderID != me → ErrLeaseOwner`；Witness Layer `advanceWitnessEraFloor` 拒旧 `epoch` | `post_seal_accepted_reply` |
| **etcd `#12528`** | high CPU → lessor 恢复后 check 滞后 | 同 `#15247` | 同上 | 同上 |
| **etcd `#20418`** | process pause → revision 不前进却接受读写 | 同 `#15247` 的第三 trigger | 同上 + `eunomiaGate` 额外 `ActiveAt` 检查 | 同上 |
| **etcd clientv3 KeepAlive buffered after Revoke** *(untracked public scenario, as of 2026-04-19)* | `Grant → KeepAlive → Revoke → drain buffered channel` | `Revoke` 已返回更高 revision，但 buffered channel 里仍可读到更早的 `TTL>0` keepalive success | Witness Layer `advanceWitnessEraFloor` / revoke-revision floor gate；无需 server-side lessor 变更 | `accepted_keepalive_success_after_revoke` |
| **KAFKA-15489** | 分区 → 两个 epoch controller 并存 | 两边各自向 broker 返 stale metadata | Witness Layer 分区愈合后拒旧 gen；Audit Layer 报 authority uniqueness 违反 | `accepted_reply_behind_successor` |
| **KAFKA-14154** | controller soft failure → stale controller 推 ISR | Silence 在非 fetch RPC 上缺 | Gate Layer uniform `eunomiaGate`（不分 RPC） | `post_seal_accepted_reply` |
| **KAFKA-16248** | zombie leader 接受 fetch 返回 OFFSET_OUT_OF_RANGE | 同上 | 同上 + Witness Layer consumer-side verifier | 同上 |
| **KAFKA-6880 (KIP-320)** | zombie replica 未 fenced（结构前史） | Primacy / Silence precursor | Layer 3 era-based fence + Gate Layer uniform | `illegal_reply_era` |
| **KAFKA-15911 (adjacent)** | leader progress timeout 只实现 "无 fetch" 一支 | Finality 不完整 | Layer 3 强制每段 detached 有 rooted close event | `closure_incomplete` |
| **CRDB `#66562` (OPEN)** | new lease 未覆盖旧 lease 已服务的 future-time read frontier | Inheritance on a multi-dimensional frontier | 当前 artifact 已实现 **NoKV-native exact issue-schedule reproduction**：按 issue body 直接跑 `n1 future read -> n1→n2 cooperative transfer -> n2 early expiry -> n3 brand-new lease -> write@8`；关闭 coverage 时 bug 可复现，打开 coverage 时 rooted check 机械拒绝。served frontier 当前已经挂到既有 `Legacy.frontiers` mandate map；在 NoKV 中，这个 mandate 以 `MandateLeaseStart` materialize，并由 rooted snapshot、`crdb-lease-start` trace adapter 与 `EunomiaMultiDim / LeaseStartOnly` formal pair 共同消费。当前仍**不声称** live CRDB race replay | `lease_start_coverage_violation` |
| **CRDB `#36431`** | observed-timestamp 在 lease 换手后失效 | Inheritance on one projection | 同上（CRDB 已自行 ship LocalTimestamp ≈ per-path Eunomia） | `uncovered_monotone_frontier` |
| **CRDB `#23749`** | 更早期的 observed-timestamp race | 同 `#36431` | 同上 | 同上 |
| **YB `#20124`** | master 缓存 tablet leader lease stale | Silence on metadata-answer path | Witness Layer `validateRegionAnswer` 检查 `sync_health == HEALTHY` 与 `descriptor_revision` | `uncovered_descriptor_revision` |
| **YB `#24575`** | RF 变化导致同类 bug class 10 个月后 recur | 同 `#20124` 新 trigger | 同上 | 同上 |
| **MongoDB `SERVER-17975`** | stale primary 继续 serve read | Silence on read path, opt-out default | Gate Layer `eunomiaGate` + Witness Layer verifier（non-opt-in） | `post_seal_accepted_reply` |
| **TiKV `PR #6343`** | follower read ReadIndex term check 漏 | Silence on ReadIndex path | Gate Layer uniform | 同上 |
| **TiKV `PR #9240`** | transfer-leader 后 stale read index | 同 `#6343` 新 trigger | 同上 | 同上 |
| **FoundationDB GRV proxy (lead)** | old GRV proxy 给 stale read version | Silence on transaction version path | Gate Layer uniform + Witness Layer verifier | `illegal_reply_era` |

几点必须和这张表一起明说：

1. **11/14 bug 在当前 Eunomia artifact 中直接可防**；1/14 (KAFKA-15489 分区内部) 因 CAP 物理限制 **partition-local 期间防不了**，但愈合后 Witness Layer + Audit Layer 保证 inadmissibility（见 §2.4 N5）；1/14 (CRDB `#66562`) 需要 **multi-dim frontier 泛化**（不是单整数 frontier，而是 predecessor served summary 与 successor `lease_start` 的 ordered coverage）；当前 artifact 已经在 NoKV 中提供一个 **exact issue-schedule reproduction**：`n1` 在 `[0,10]` 下服务 `read(k@9)`，`n1→n2` cooperative transfer 在 `@6` 带上 summary，`n2` 在 `@8` 提前过期，`n3` brand-new lease `@8` 若不做 coverage 会错误接受 `write(k@8)`；打开 coverage 后，rooted check 机械拒绝 `n3@8`，只接受 `lease_start>9` 的 successor。同一 violation 现在同时被 rooted snapshot、`crdb-lease-start` trace adapter 与 `EunomiaMultiDim / LeaseStartOnly` formal pair 消费。它仍然**不是** live CRDB replay；1/14 (CRDB `#36431`) 已经被 CRDB 自己用 LocalTimestamp ship —— **反而是 Eunomia 思路能 ship 的 reverse evidence**。
2. 每一行的 "eunomia-audit 检测规则" 列名都对应 [`coordinator/audit/report.go`](coordinator/audit/report.go) 的 `SnapshotAnomalies` 结构字段或 [`coordinator/audit/trace.go`](coordinator/audit/trace.go) 的 `ReplyTraceAnomaly.Kind`；reviewer 可直接在 artifact 中 grep 验证。
3. **防御路径不是 per-bug 定制的**——是 3 个 orthogonal layer（`eunomiaGate` / `advanceWitnessEraFloor` / `eunomia-audit`）对所有 bug 的**统一覆盖**。每个新增 bug signature 只需要将其映射到 4 条保证中的一条，不需要新代码路径。

#### 7.5.2 Documented tradeoff / local finality：不该算 bug，但更能说明“工业界在离散地修 finality”

另一组材料同样重要，但不应和 strict bug 混写：

- TiDB APF
  - 文档明确写出 follower stale answer、reject 条件和 TiKV client-go fallback；
  - 这不是 bug 证据，而是一种 **两层 sealing**：PD 层允许 bounded stale，KV 层负责最终 guard；
- Consul
  - `stale/default/consistent` 是公开 API knob，不是系统偷偷越权；
  - `LastContact/KnownLeader` 让调用方自己选择可接受的 staleness；
- Ceph RGW multisite
  - “先追平 metadata sync 再 promote”“关闭旧 master 上的 `radosgw`”是显式 operator runbook；
  - 这不是 live reply bug，而是 **admin-side finality discipline**。

这组材料在本文里的正确角色，不是“正样本证明 bug class 存在”，而是：

> **工业界已经在零散位置上实现了 finality guard；但这些 guard 仍停留在 per-path、per-layer 或 per-procedure 的离散实现，还没有被收成统一 protocol object，也没有被收成可证明、可审计的同一语义。**

#### 7.5.3 Adjacent substrate evidence：可做 motivation，不应混入 positive set

还有一类材料可以保留，但应明确降级为 motivation 辅证。例如 etcd 2025 年关于 zombie members 的升级说明，反映的是 membership truth 在历史双存储结构中的不一致如何在升级时暴露出来；`KAFKA-15911` 更像 follower-progress / audit residual gap，而不是干净的 stale-era reply signature；FoundationDB 当前也更适合作 doc-level supporting lead，而不是 canonical strict issue。它们当然说明 control-plane truth substrate 仍会出错，但它们不是 live authority transition 下的标准 strict-hit，因此不应混入 strict positive evidence。

因此本文的 problem statement 不应再停留在"control plane availability 很重要"，而应更直接：

> **authority-gap anomalies 已经在多个工业系统的公开 bug / issue 中被直接观察到；与此同时，工业界又在 APF、Consul、Ceph 这类系统中用 tradeoff、fallback 或 operator discipline 零散地修 finality。本文要占住的剩余空间，不是再举更多例子，而是把这些离散实践统一成一个可定义、可检查、可审计的 correctness class。**

#### 7.5.4 Eunomia-guided findings: 新观察与可验证的 hunt roadmap

如果 Eunomia 只能解释历史 bug，它仍是 post-hoc 叙事。更强的说法是：**Eunomia 的结构性方法能够预测尚未被社区 file 的 bug class，并且在简短实验里直接触发它们**。本文目前已有两条这类 Eunomia-guided evidence，以及 3 条高置信度的待验证 hypotheses。

**已触发的新 client-visible finding (真 new-bug 级别，artifact 可复现)**

- **F1 — etcd `v3.6.10` `clientv3.KeepAlive` buffered stale-success after `Revoke`**（§5.5.2；**已作为 [`etcd-io/etcd#21638`](https://github.com/etcd-io/etcd/issues/21638) 公开 filed，2026-04-19 提交，当前 OPEN**）
  `Revoke` 已在更高 revision 完成后，调用方仍可从 buffered channel 读到更早的 `TTL>0` keepalive success；`eunomia-audit` 将其判成 `accepted_keepalive_success_after_revoke`，Layer-6 witness floor gate 关闭。这是当前 artifact 在工业系统上的唯一一条 truly new client-visible finding，且已通过 upstream filing 形成 community-visible 可独立复核的证据（issue body 内嵌 ~50 行 Go 复现脚本，observed output `ttl=3 success_revision=2 revoke_revision=3`）。

**结构化预测仍然成立，但本版 artifact 不再把旁支 hunt 写进主线**

> **Eunomia 不只是刻画 authority-gap 的一个 vocabulary；它也是一个可以事前指出哪里必须补 gate 的 structural lens。** 当前代码库把这个 claim 收敛到两条最硬证据：etcd F1 和 CRDB `#66562` exact issue-schedule reproduction。旁支 hypothesis、pedagogical counterexample 与 live hunt 留到下一轮外部系统扩展，不再混入本文当前的主闭环。

### 7.6 Concurrent Partial Reinventions of Eunomia Primitives

如果 reviewer 的最硬攻击是“MongoDB、Kafka、CockroachDB、TiKV 都已经在做这些，你只是换了名字”，那么本文不能回避，反而应该正面承认并利用这个事实。当前更准确的结论不是“别人都没想到”，而是：

> **工业界已经在碎片化地重新发明 Eunomia primitive，但没有任何单一系统把 rooted seal、era-based inadmissibility、successor coverage 与 finality-complete audit 一起做成统一协议。**

把已有系统按 primitive 摊开，这个差异会非常清楚：

| 系统/路径 | era-based reject | rooted seal before reply | successor coverage frontier | finality-complete rooted lifecycle |
| --- | --- | --- | --- | --- |
| MongoDB `readConcern:linearizable + electionId` | 部分 | 强 | 弱 | 无 |
| Kafka KIP-320 / KRaft patch family | 强（per-RPC） | 弱 | 弱 | 无 |
| CockroachDB `read-summary / timestamp frontier` | 弱 | 弱 | 强（per-path） | 无 |
| TiKV leader-transfer fixes | 强（single path） | 弱 | 弱 | 无 |
| YugabyteDB workaround / runbook | 弱 | 弱 | 部分 | 无 |
| NoKV `Eunomia + 四条最小保证` | 强 | 强 | 强 | 第一版 skeleton |

这张对比表指向三个更硬的结论。

第一，**没有任何单一系统同时实现三种 Eunomia primitive**。大多数系统只在一个 path 上补 era check，或只在一个 API 上做 rooted seal，或只在某个 cache / read-summary path 上做 successor coverage。也正因此，它们更像 partial reinvention，而不是 class-level finality。

第二，**工业界 recurrence 的真实模式不是“完全没修”，而是“修了一块、另一块复发”**。Kafka 在 KIP-320 / `KAFKA-6880` 之后，仍然依次暴露 `KAFKA-14154/#15489/#16248`；CockroachDB 在 `#23749/#36431` 之后，`#66562` 仍长期 open；YugabyteDB 则在 `#20124` 缓解后又遇到 `#24575`。这恰好说明：path-specific patch 可以解决局部触发器，却不等于把 bug class 封口。

第三，**Eunomia 的必要性本身也有边界**。对一个单一 epoch tuple 就足以描述 predecessor frontier 的 duty，Eunomia 可能是 overkill；Kafka fetch fencing 或 TiKV 的 term equality check 就可能足够。但对 timestamp cache、read summary、descriptor frontier、tablet leader lease set 这类 multi-dimensional frontier，piecemeal patch 的 recurrence 明显更高，因为 predecessor frontier 不是单个整数，而是集合或 lattice。本文真正要占住的位置 therefore 不是“Eunomia 是万能银弹”，而是：**Eunomia 是 multi-dimensional frontier duty 上第一个统一 rooted seal、inadmissibility、coverage 与 finality 的协议。**

把前面三组近邻放在一起，本文真正剩下的空间因此就不再是“再造一个 handoff protocol 部件”，而是：

1. `authority-gap anomalies` 是一类跨系统 bug class；
2. `Eunomia + 四条最小保证` 给出这类 bug class、尤其是 multi-dimensional frontier variant 的最小 service-level semantics；
3. `eunomia-audit` 让这组语义可以被 trace、checker 与 external pilot 共同消费；
4. NoKV 是第一个把这组语义做成 executable rooted skeleton 的 reference implementation。

只要这个边界站住，本文就不再只是“把已知机制缝得更完整”；反之，只要 formal、checker 与 cross-system evidence 没跟上，它就仍然会被压回 known pieces 的高质量组合。

## 8. 结论

本文不是在提出一个更快的 failover controller，也不是在把若干已知机制堆叠成新的术语。本文真正要提出的是一个更强的问题类：

> **当 root authority 暂时不可达时，singleton-duty control plane 是否还能在显式 handoff object、显式 continuation witness 与显式 handover witness 之下，继续安全地维持有限但有用的服务。**

NoKV 当前给出的，不是这个问题的最终完整答案，而是一套已经运行的最小 rooted substrate：typed rooted truth、lease-fence transition、windowed monotone serving 与第一版 handover skeleton。它已经足以说明，这条路线不是空想；而最值得继续投入的，不是“再做一个更快的 coordinator”，而是把这些分散机制对象化成一个真正的 `auditable authority handoff protocol`。

如果本文只留下一个核心结论，那就是：

> **对承担 authority-bound continuations 的 distributed KV control plane，最关键的不是如何在失败后恢复一个大脑，而是能否把“继续服务这件事本身”变成一个有显式交接、显式边界、显式证据和显式收口的 authority handoff phase。**

如果本文最终只留下一个最强命题，那应该是：

> **我们不是只在 protocolize continuation，我们是在把 finality 提升成 first-class safety condition。**

## 9. 参考文献与公开资料骨架

下面给出当前 paper 必须显式挂出的第一版引用骨架。后续整理成统一参考文献格式之前，至少应保持这些 primary sources 全部可追溯。

### 9.1 学术论文

1. Chubby: [The Chubby lock service for loosely-coupled distributed systems](https://www.usenix.org/conference/osdi-06/presentation/chubby-lock-service-loosely-coupled-distributed-systems)
2. Spanner: [Spanner: Google's Globally-Distributed Database](https://research.google/pubs/pub39966/)
3. Megastore: [Megastore: Providing Scalable, Highly Available Storage for Interactive Services](https://research.google/pubs/pub36971/)
4. Zanzibar: [Zanzibar: Google's Consistent, Global Authorization System](https://www.usenix.org/system/files/atc19-pang.pdf)
5. Delos: [Virtual Consensus in Delos](https://www.usenix.org/system/files/osdi20-balakrishnan.pdf)
6. FoundationDB: [FoundationDB: A Distributed Key-Value Store](https://www.foundationdb.org/files/fdb-paper.pdf)
7. Smart Casual Verification of CCF: [Smart Casual Verification of the Confidential Consortium Framework](https://www.microsoft.com/en-us/research/wp-content/uploads/2024/07/nsdi25spring-final392.pdf)
8. RPRC: [Runtime Protocol Refinement Checking for Distributed Protocol Implementations](https://www.usenix.org/system/files/nsdi25-ding.pdf)
9. Remix: [Multi-Grained Specifications for Distributed System Model Checking and Verification](https://marshtompsxd.github.io/pub/eurosys25_remix.pdf)
10. T2C: [Deriving Semantic Checkers from Tests to Detect Silent Failures in Production Distributed Systems](https://www.usenix.org/system/files/osdi25-lou.pdf)
11. Basilisk: [Basilisk: Using Provenance Invariants to Automate Proofs of Undecidable Protocols](https://www.usenix.org/system/files/osdi25-zhang-tony.pdf)
12. Kondo (OSDI 2024): [Inductive Invariants That Spark Joy: Using Invariant Taxonomies to Streamline Distributed Protocol Proofs](https://www.usenix.org/system/files/osdi24-zhang-nuda.pdf)
13. Bayou session guarantees: [Session Guarantees for Weakly Consistent Replicated Data](https://www.cs.cornell.edu/courses/cs734/2000FA/cached%20papers/SessionGuaranteesPDIS_1.html)
14. Viewstamped Replication Revisited: [Viewstamped Replication Revisited (Liskov & Cowling, 2012)](http://pmg.csail.mit.edu/papers/vr-revisited.pdf)
15. Linearizability (form-of-contribution reference): [Herlihy & Wing, Linearizability: A Correctness Condition for Concurrent Objects (TOPLAS 1990)](https://dl.acm.org/doi/10.1145/78969.78972)

### 9.2 工业系统公开资料

1. TiDB PD Microservices: [PD Microservices](https://docs.pingcap.com/tidb/stable/pd-microservices/)
2. TiDB Active PD Follower GA: [TiDB 8.5.0 Release Notes](https://docs.pingcap.com/tidb/stable/release-8.5.0/)
3. TiDB APF contract detail: [Tune Region Performance](https://docs.pingcap.com/tidb/stable/tune-region-performance/)
4. etcd failure boundary: [Failure modes](https://etcd.io/docs/v3.4/op-guide/failures/)
5. etcd authority-transition bug class: [Issue #15247](https://github.com/etcd-io/etcd/issues/15247)
6. etcd authority-transition variant: [Issue #12528](https://github.com/etcd-io/etcd/issues/12528)
7. etcd stale reads under process pause: [Issue #20418](https://github.com/etcd-io/etcd/issues/20418)
8. etcd zombie members upgrade note: [Avoiding Zombie Cluster Members When Upgrading to etcd v3.6](https://etcd.io/blog/2025/zombie_members_upgrade/)
9. KRaft stale metadata issue: [KAFKA-15489](https://issues.apache.org/jira/browse/KAFKA-15489)
10. Kafka controller soft-failure residual state: [KAFKA-14154](https://issues.apache.org/jira/browse/KAFKA-14154)
11. Kafka zombie replica fencing / KIP-320: [KAFKA-6880](https://issues.apache.org/jira/browse/KAFKA-6880)
12. Kafka client-side workaround for zombie leader: [KAFKA-16248](https://issues.apache.org/jira/browse/KAFKA-16248)
13. CockroachDB lease frontier issue: [Issue #66562](https://github.com/cockroachdb/cockroach/issues/66562)
14. CockroachDB historical stale-read issue: [Issue #36431](https://github.com/cockroachdb/cockroach/issues/36431)
15. CockroachDB earlier successor-coverage issue: [Issue #23749](https://github.com/cockroachdb/cockroach/issues/23749)
16. TiKV follower-read leader transition fix: [PR #6343](https://github.com/tikv/tikv/pull/6343)
17. TiKV stale read index after transfer leader: [PR #9240](https://github.com/tikv/tikv/pull/9240)
18. YugabyteDB stale leader-lease metadata: [Issue #20124](https://github.com/yugabyte/yugabyte-db/issues/20124)
19. YugabyteDB recurrence under RF change: [Issue #24575](https://github.com/yugabyte/yugabyte-db/issues/24575)
20. MongoDB stale reads with majority under failover: [SERVER-17975](https://jira.mongodb.org/browse/SERVER-17975)
21. Kafka KRaft residual seal gap: [KAFKA-15911](https://issues.apache.org/jira/browse/KAFKA-15911)
22. Consul consistency modes: [Consistency Modes](https://developer.hashicorp.com/consul/api-docs/features/consistency)
23. ScyllaDB Raft topology: [Raft in ScyllaDB](https://docs.scylladb.com/manual/stable/architecture/raft.html)
24. CephFS metadata pause boundary: [CephFS health messages](https://docs.ceph.com/en/latest/cephfs/health-messages/)
25. Ceph RGW multisite metadata master discipline: [Multi-Site](https://docs.ceph.com/en/reef/radosgw/multisite/)
26. Kleppmann fencing token reference: [How to do distributed locking (Kleppmann, 2016)](https://martin.kleppmann.com/2016/02/08/how-to-do-distributed-locking.html)
27. **Upstream filing of F1**: [etcd-io/etcd#21638 — clientv3: LeaseKeepAlive channel may yield buffered pre-revoke success after LeaseRevoke returns](https://github.com/etcd-io/etcd/issues/21638) (filed 2026-04-19, OPEN) — community-visible reproduction artifact for paper §5.5.2 and §7.5.4 F1
28. Jepsen Distributed Systems Glossary (2025-07-15): [jepsen.io/blog](https://jepsen.io/blog/2025-07-15-distributed-systems-glossary) — used for §7.5.1a negative evidence on absence of "authority-gap" / "stale leader" class naming
29. Antithesis Distributed Systems Reliability Glossary (2025-10-20): [antithesis.com/resources/reliability_glossary](https://antithesis.com/resources/reliability_glossary/) — cross-referenced with Jepsen glossary for the same negative evidence

### 9.3 写作纪律

- 凡是涉及“当前仍是 experimental / GA / fixed / unresolved / latest docs”之类表述，都必须带版本或日期。
- 凡是表格里对外部系统的判断，都至少要能追到 1 个 primary source。
- 凡是公开 issue，都必须写清：
  - 这是历史 bug class
  - 还是当前未修问题
  - 还是结构性 tradeoff

### 9.4 Primary-Source Receipts 附录计划

- 附录可附一张 `receipts` 表，把正文里最关键的 maintainer acknowledgement、open issue、recurrence 与 fix-grade 判断逐条挂到 issue / PR / post-merge discussion。
- 这张附录表的职责不是“再讲一遍 related work”，而是让 reviewer 可以一眼核对：
  - 哪些是 strict positive evidence；
  - 哪些只是 documented tradeoff / local finality；
  - 哪些 fix 只 seal 了一条 path，哪些 gap 到当前仍 unresolved。

当前最值得保留的 maintainer receipts 如下：

| 系统/issue | maintainer receipt | 角色 |
| --- | --- | --- |
| etcd `#20418` | *“I'm still not convinced myself that the PR fixes the root cause of the issue.”* | 直接承认 merged patch 仍未触及根因 |
| etcd `#15247` | *“radical long term solution might be something like disabling proposal forwarding ... this solution can work for many practical cases”* | 承认当前修复只覆盖 practice，不是 general finality |
| etcd `#12528` | *“we need to spend more time to find a simple and safe way to fix it”* | 承认当前 workaround 之外仍缺结构性解 |
| CockroachDB `#66562` | *“This issue should stay open ... the issue has good discussion.”* | 直接承认 successor-coverage 类问题尚未封口 |

后续整理时应把这几条都补成精确的 comment URL 与日期钉住，避免 reviewer 误以为这里只是作者转述。
