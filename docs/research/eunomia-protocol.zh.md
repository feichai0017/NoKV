# Eunomia：分布式控制面权威切换的一个服务级正确类

> 命名：Eunomia 来自希腊语 εὐνομία，意为 “good order under law”。本文只重命名协议类，`Primacy`、`Inheritance`、`Silence`、`Finality` 四个不变量名保持不变。

> 本文按 Lamport 1978《Time, Clocks, and the Ordering of Events in a Distributed System》与 Herlihy-Wing 1990《Linearizability: A Correctness Condition for Concurrent Objects》的写法组织：先从一个根本问题起，贯穿一个抽象 running example，形式化前铺垫直觉，逐一列举并反驳所有现有方案，最后给出协议与正确性论证。
>
> 关联文档：
> - 主论文草稿：`docs/research/control-plane-paper-draft.zh.md`
> - 实现：`meta/root/state/eunomia.go`、`coordinator/protocol/eunomia/handoff.go`
> - 形式化：`spec/Eunomia.tla`、`spec/EunomiaMultiDim.tla`

---

## 1. 引言

分布式系统的一个基本操作是把对某对象的权威从一个节点交给另一个节点：租约被续签、主被选举、epoch 被更新、range 被迁移。这类操作被研究了四十年，每个现代分布式系统都有一套做法。然而，来自 CockroachDB、etcd、Chubby、Consul、YugabyteDB、Pulsar、Ceph 的 bug 列表指向同一个观察：**一次权威切换完成之后，前任在切换前留下的可观察副作用，对继任者而言仍然是不可见的**。

这类 bug 的共同形式是：前任 A 告诉外部消费者 C 某件事已经发生；A 的权威被终止；继任者 B 在不知道 A 曾经告诉过 C 什么的情况下做了一个合法的新动作，这个动作违反了 C 从 A 那里已经看到的事实。

本文的主张有三点：

(i) 这是一个**类问题**，不是若干偶发 bug 的集合。它的共同结构可以被形式化。

(ii) 现有的正确性工具——fencing token、view change、lease、epoch、state transfer、quorum read——都只解决这个问题的一部分。我们列出十五种主流做法，逐一说明为什么每一种在保留合理假设下都不够。

(iii) 我们定义一个**服务级正确类** Eunomia，由四条最小保证刻画：`Primacy`、`Inheritance`、`Silence`、`Finality`，并给出一个不需要新共识协议的最小实现。

---

## 2. Running Example：单元格交接

考虑最小抽象：一个**单元格** (cell) `x`，在任何时刻只能由一个节点 owner 服务。Owner 接受来自任意 client 的读写。权威可以被撤销并移交给新 owner。

```
           ┌─────────┐
           │   x     │
           └─────────┘
               ▲
               │ serve(read/write)
               │
    ┌──────────┴──────────┐
    │                     │
  owner_A             (future) owner_B
```

**一次 handoff** 由三步组成：

1. `A.own(x) = true`，`A` 接受对 `x` 的请求
2. `A.own(x) := false` —— A 失去权威
3. `B.own(x) := true` —— B 获得权威

两个时间点之间的间隔称为 **handoff interval**。在 handoff interval 中：
- A 可能还有没完成的 reply 在路上（**in-flight reply**）
- A 可能在失去权威之前已经把某件事告诉过某个 C（**published commitment**）
- B 对这两者都没有直接知识

这个抽象足以把后续所有讨论支撑起来。CRDB #66562 是它的一个实例（published commitment = "key k 在 ts=9 被读到值 v"）；etcd #21638 是另一个实例（in-flight reply = "lease keep-alive success")；Chubby sequencer bug 是第三个实例。

**一个具体时间线**（CRDB #66562）：

```
  t=1 │ A owns x
  t=2 │ A serves read(k@ts=9) → client                    ← published commitment
  t=3 │ A loses authority (lease expires / transfer)
  t=4 │ B acquires authority with lease_start=8
  t=5 │ B accepts write(k@ts=8)                           ← violates commitment at t=2
```

到 t=5 这一步，B 的所有本地状态是合法的：它的 lease 区间不与 A 重叠，它的 term 大于 A 的，它的 fencing token 更新。它违反的是一个**不在它视野里的事实** —— A 在 t=2 对外承诺了什么。

### 2.1 最小术语表

为了避免把代码对象名、协议语义名和论文叙事名混在一起，本文正文尽量只保留下面这套最小词汇：

- `Tenure`：当前 active authority record
- `Legacy`：前任 authority 的冻结边界
- `Handover`：一次 handoff 的收尾记录
- `Era`：authority era
- `Witness`：用来判断当前 handoff 是否安全的证明对象

四条保证统一写成：

- `Primacy`
- `Inheritance`
- `Silence`
- `Finality`

正文直接沿用实现层的 `Tenure / Legacy / Handover`、`Era`、`MandateFrontiers` 术语，不再额外保留一套旧别名。

---

## 3. 问题陈述

### 3.1 Events and Observations

沿用 Lamport 的记号。系统由**事件** (event) 构成。事件有两类：
- **Internal event**：节点状态的局部变化
- **Message event**：节点之间的消息发送/接收

我们引入一类特殊事件：

**Definition 1 (Commitment)**. 一个 commitment 是 owner A 发送给 consumer C 的一个消息事件 `m`，满足以下性质：C 接收 `m` 之后，C 的后续行为依赖于 "`m` 在系统任意时刻都是真的"。

例如，CRDB 中 "read(k@ts=9) returns v" 是一个 commitment：client 收到这个答复之后，在 `ts >= 9` 的时间窗里任何写入都必须保留 `v`，否则 client 观察到的历史被重写。

对每个 commitment `m`，记 `m.ts` 为其时间戳（或更一般地，某个值域上的 frontier）。

### 3.2 Authority Handoff Gap

**Definition 2 (Handoff Interval)**. Owner A 被 owner B 取代的 handoff interval 是一个时间窗 `[t_end(A), t_start(B)]`，其中 `t_end(A)` 是 A 失去权威的时刻，`t_start(B)` 是 B 获得权威的时刻。

**Definition 3 (Authority Handoff Gap)**. 对一次 handoff `A → B`，authority handoff gap 是一个四元组 `(A, m, H, B)`，其中 `m` 是 A 在 `t_end(A)` 之前产生的一个 commitment，且 B 在 `t_start(B)` 之后执行的某个动作 `a` 满足 `¬compatible(a, m)`，即 `a` 违反了 `m` 对外承诺的事实。

**Proposition 1**. 如果系统不提供机制使 `m` 的**信息**从 A 跨越 handoff interval 传到 B，那么 gap 是**无法避免的**，只要：
- C 记得 `m`（consumer 不被追踪）
- `m.ts` 落在 B 的合法动作值域内（gap 跨越值域边界）

证明（非正式）：A 在 `t_end(A)` 之后无法与 B 通信（否则 A 仍是权威）；C 在收到 `m` 之后可能随时消费 `m`；B 只能基于自己的输入做决策，而它的输入里不包含 `m`。因此违反 `m` 的动作 `a` 在 B 看来是合法的。

这条命题说明：**修复 gap 的唯一工程路径是把 `m` 的摘要写入一个双方都能读到的持久化位置**。所有不这样做的方案都是不完整的。

### 3.3 为什么这个问题在经典文献里没有被命名

分布式系统文献传统上把一致性问题分成三类：

- **序列一致性** (Lamport 1979, Herlihy-Wing 1990)：读写的全局排序
- **共识** (Lamport 1978 & 1998, Ongaro-Ousterhout 2014)：节点对值的一致决定
- **成员管理** (Liskov-Cowling 2012 VR)：谁在集合里

Authority handoff gap 不属于这三类。它涉及的是**权威本身的生命周期与 commitment 的继承**，是一个**服务级别**而不是协议级别的问题。这是本文要填补的空缺。

---

## 4. 现有方案的完整列举与反驳

我们枚举十五种常见做法。每一种给出核心机制、尝试修复 gap 的方式、以及一个**具体反例**说明它不够。反例统一用 §2 的 running example 表达。

### 4.1 Lease（租约）

**机制**：Owner A 持有一个带绝对或相对时间边界的租约 `[t_begin, t_end]`。只有 `now ∈ [t_begin, t_end]` 时 A 才能服务。新 owner 只能在 `now > A.t_end` 之后获取租约。

**尝试的修复角度**：保证 handoff 两端在时间上不重叠。

**反例**：CRDB #66562 的 `n1 lease [0,10] → n2 lease [6,8] → n3 lease [8, +∞)`。三个租约在时间上没重叠，满足 lease 条件；但 commitment `k@9` 从 n1 泄漏到 n3 接管后的值域 `[8, +∞)`。Lease 只检查**谁能服务**，不检查**服务过什么**。

### 4.2 Fencing Token（Chubby sequencer）

**机制**：每次 handoff 递增一个单调整数 token。Consumer 记住它见过的最大 token，拒绝任何更小 token 的请求。

**尝试的修复角度**：防止 A 的延迟 reply 被 C 接受为新权威的答复。

**反例**：n3 自己发出的写入 `write(k@ts=8)` 不携带任何旧 token —— 它是 B 的新动作，token 是最新的。Consumer 能拒绝 A 的 stale reply，但 consumer 无法告诉 B "k@9 已经被我看过"。Fencing 只管**reply 方向**，不管 **owner 决策方向**。

### 4.3 View Change / View Stamps (VR, Raft term)

**机制**：每次 handoff 伴随 view 或 term 递增；新 view 开始前必须完成 log recovery 或 state transfer。

**尝试的修复角度**：新 owner 继承 log 中的所有决策。

**反例**：`k@9` 这条 read 服务不在 Raft log 中。它是 leaseholder 在 lease 有效期内自行决定的 "follower read" / "leaseholder read"。View change 搬运的是**协议层 log**，commitment 是**服务层状态**，不在搬运范围内。

**推论**：任何"只搬 log"的方案都有同样盲区。Log 只记录被提议并 commit 的命令，不记录"在 lease 内答复过读请求"这类非 commit 动作。

### 4.4 State Transfer（VR、Raft InstallSnapshot）

**机制**：新 owner 在接管前从 quorum 读一份最新快照。

**反例**：同 4.3。快照是状态机快照，不是"服务过的读摘要"。

### 4.5 Quorum Read

**机制**：读请求访问 quorum 个副本，取最新。

**反例**：这修的是"读是否看到最新的写"，不是"新 owner 是否知道旧 owner 服务过什么"。两个问题在数据模型上不同。而且 quorum read 在 lease-based 系统里经常被绕过（leaseholder read 就为了避免 quorum）。

### 4.6 Epoch Number（Kafka epoch, TiKV region epoch）

**机制**：每次 membership 或 authority 变化递增 epoch。所有跨 epoch 的操作必须携带正确 epoch，否则被拒绝。

**反例**：epoch 等价于 token + view 的并集。和 4.2 + 4.3 同样的反例 —— commitment 不在 epoch 携带的信息范围内。

### 4.7 Hybrid Logical Clock (HLC)

**机制**：每个事件带 `(physical, logical)` 双维时间戳，保证因果关系。

**反例**：HLC 解决的是"事件的时间戳是否反映因果"，不是"新 owner 是否知道旧 owner 做过什么"。A 服务 `k@9` 时的 HLC 戳在 A 本地，不会因为 B 接管就自动传到 B。

### 4.8 TrueTime（Spanner）

**机制**：用硬件时钟提供 `TT.now()` 的区间，commit wait 保证全局 ordering。

**反例**：TrueTime 保证的是**时间戳的全序**，不是**服务状态的继承**。Spanner 本身也有 leader lease + Paxos，gap 的修复在 Paxos log 层面做 —— 这是 4.3 的推论。

### 4.9 Read Index (etcd)

**机制**：Linearizable read 之前先向 leader 确认"我仍然是 leader"。

**反例**：这是 `Silence` 的一种实现 —— 拒绝 stale leader 的读答复。不覆盖 `Inheritance`。

### 4.10 Session Guarantees (Bayou)

**机制**：Client session 记住它读过的版本和写过的内容，后续请求携带这些信息。

**反例**：session guarantee 要求 **client 主动携带**读写历史。这是对 Consumer 的假设 —— 在多数系统里不成立（client 通常是 stateless）。Eunomia 目标是在 **consumer 被动**的情况下把 commitment 的继承做到 rooted 层。

此外，session guarantee 只保证**同一 client** 的一致性。CRDB #66562 里违反的是**跨 client** 的线性一致（A 的 client 读到 k@9，B 的 client 写到 ts=8）。

### 4.11 CRDT / Anti-entropy

**机制**：所有副本最终收敛，冲突用 merge 函数解决。

**反例**：Eunomia 目标是**避免冲突发生**，不是**合并冲突**。CRDT 假设应用能容忍最终一致；authority handoff gap 在**强一致系统**里也发生（CRDB 声明 serializable isolation）。

### 4.12 TEE-based Trusted Lease (T-Lease, DSN 2021)

**机制**：用 Intel SGX/TDX 把 lease 时钟做在 TEE 里，对抗 Byzantine 时钟。

**反例**：T-Lease 解决的是**时钟可信**问题，信任模型是 Byzantine；Eunomia 在 crash-recovery 模型下的 authority handoff gap 仍然存在，哪怕时钟完美。

### 4.13 Primary-Backup with Stable Storage

**机制**：primary 每次动作写 stable storage，failover 时 backup 读 stable storage 重建状态。

**反例**：如果 "serve read(k@9)" 不被 primary 写到 stable storage，backup 读不到。这恰好是 4.3 的同构问题：**需要识别什么叫 commitment 并持久化它**。这正是 Eunomia 的核心，只是 Eunomia 把这个动作形式化并最小化（Seal.Frontiers 而不是 full log）。

### 4.14 ZooKeeper session + ephemeral node

**机制**：session 过期时删除 ephemeral node，外部观察者 watch 节点变化。

**反例**：session 机制管的是"owner 是否还在"；它不保证 owner 离开之前产生的 commitment 被继任者继承。

### 4.15 CockroachDB PR #73001 "Write-time Read-summary Gate"

**机制**：在**写**路径上检查历史读摘要，拒绝违反已服务读的写入。

**反例（cascading chain）**：假设连续 3 次 handoff `A → B → C → D`，每次切换都更新 read summary。如果 read summary 只在 in-memory 层维护，B 失败（crash before persist）时其继任者 C 拿不到 A 的 commitment。我们在 `benchmark/eunomia/crdb/` 构造了一个针对 PR #73001 思路的 cascading chain 反例：PR 的 in-memory 做法在链式 handoff 下丢失信息。这也是 PR 停在 3 年没合并的技术原因之一。

Eunomia 与 PR #73001 的差别：Eunomia 把 read summary 刻到 **rooted** 层（replicated metadata），**链式 handoff 中每一环都强制 inherit rooted 状态**；PR #73001 停在 in-memory 层。

### 4.16 汇总：覆盖四条最小保证的能力矩阵

下文 §6 会形式化 `Primacy / Inheritance / Silence / Finality`。这里先给出上述 15 种方案的覆盖矩阵作为 §4 的收尾：

| # | 方案 | Primacy | Inheritance | Silence | Finality |
|---|---|---|---|---|---|
| 4.1 | Lease | ✅ | ❌ | ❌ | ❌ |
| 4.2 | Fencing Token | ❌ | ❌ | ✅ | ❌ |
| 4.3 | View Change | ✅ | 部分 | 部分 | ❌ |
| 4.4 | State Transfer | ✅ | 部分 | ❌ | ❌ |
| 4.5 | Quorum Read | ❌ | ❌ | ❌ | ❌ |
| 4.6 | Epoch | ✅ | ❌ | ✅ | ❌ |
| 4.7 | HLC | ❌ | ❌ | ❌ | ❌ |
| 4.8 | TrueTime | ❌ | ❌ | ❌ | ❌ |
| 4.9 | Read Index | ❌ | ❌ | ✅ | ❌ |
| 4.10 | Session Guarantee | — | 部分 | — | — |
| 4.11 | CRDT | — | — | — | — |
| 4.12 | T-Lease | ✅ | ❌ | ✅ | ❌ |
| 4.13 | Primary-Backup | 部分 | 部分 | ❌ | ❌ |
| 4.14 | ZooKeeper Session | ✅ | ❌ | ❌ | ❌ |
| 4.15 | CRDB PR #73001 | ✅ | 部分 | ❌ | ❌ |
| | **Eunomia (本文)** | ✅ | ✅ | ✅ | ✅ |

(— 表示 "不在该问题的范围内" / "假设不成立时不可比较")

**Theorem 1 (Coverage Necessity)**. 对 §2 定义的 running example，**不存在**一种 §4.1–4.15 中的方案或它们的有限布尔组合，使得 `Primacy ∧ Inheritance ∧ Silence ∧ Finality` 全部成立而不等价于 Eunomia。

**证明（非正式）**：`Inheritance` 要求继任者在接管前持久化地继承前任的 commitment frontier。在 §4.1–4.15 中只有 4.13 (Primary-Backup) 和 4.15 (PR #73001) 触及这一点。4.13 要求写**全部**状态到 stable storage（不最小化），Eunomia 本身可以看作 4.13 的一个最小化实例。4.15 只持久化到 in-memory，cascading chain 反例证明不够。因此任何达到 `Inheritance` 的方案至少等价于"把 commitment frontier 持久化到 rooted storage"，即 Eunomia。`Finality` 同理：它要求每一次 handoff 都以 rooted、committed 的 finality 收口，这也是 Eunomia 的机制。

---

## 5. 系统模型

**Participants**. 系统由三类角色组成：
- `RootStore` R：一份 replicated 的、提供 linearizable append-read 的持久状态机
- `Coordinator` C：由 R 授权的权威实例。任意时刻 R 的状态决定哪个 C 是 active
- `Consumer` X：任何向 C 发请求的实体

**Trust model**. Crash-recovery。节点可崩溃、重启、分区，但不伪造。

**Clock model**. 时钟不被信任为精确同步。正确性不依赖时钟正确性 —— 正确性来自 R 的 linearizability。

**Non-goals**. Byzantine。Membership change（由 VR / Raft 处理）。跨 region 全局事务。

**Assumption R1 (Rooted Linearizability)**. R 提供 linearizable append-read on typed objects.

**Assumption X1 (Witness Capability)**. Consumer 能携带并验证 reply 附带的 witness。Witness 的格式与验证由 Client SDK 提供。

---

## 6. Eunomia 协议

### 6.1 三个 Rooted Object

Eunomia 向 R 引入三个类型化对象，记为 `Tenure`、`Legacy`、`Handover`。它们的 schema 在 `meta/root/state/eunomia.go`：

- `Tenure`: `{holder_id, epoch, expires_at, mandate, lineage_digest}`
- `Legacy`: `{holder_id, epoch, mandate, frontiers}`
- `Handover`: `{holder_id, legacy_era, successor_era, legacy_digest, stage}`

记号约定：对某个 holder `h` 和时刻 `t`，`Tenure(h,t)` 表示 `R.read(Tenure).holder_id == h ∧ R.read(Tenure).expires_at > t`；类似定义 `Legacy(h)`、`Handover(h)`。

### 6.2 四条最小保证

**Primacy（single active authority）**. 对任意时刻 `t`：
```
∀ h1, h2. Lease(h1, t) ∧ Lease(h2, t) ⇒ h1 = h2
```
即 R 的 Lease 对象任意时刻最多标记一个 active holder。

**Inheritance**. 对任意两个先后的 `Tenure T1, T2`，若 `T1.holder ≠ T2.holder` 且存在 `Legacy L`，`L.holder = T1.holder`：
```
∀ mandate m. T2.frontiers[m] ≥ L.frontiers[m]
```
即继任者的 frontier 不得低于前任 Legacy 记录的 frontier。

**Silence**. 对 consumer X 收到的任意 reply `r`，若存在 `Legacy L` 使得 `L.holder = r.source ∧ L.epoch = r.epoch`：
```
X.admit(r) = false
```
即一旦 Legacy 存在，带相同 epoch 的 reply 不得被接受。

**Finality**. 同一 holder `h` 进入新一轮 `Tenure` 之前，上一轮的 `Handover` 必须走到 `stage ∈ {Closed, Reattached}`：
```
∀ h. new_Tenure(h) ⇒ Handover(h).stage ∈ {Closed, Reattached}
```

### 6.3 三个 rooted 命令

每条保证都由 rooted 命令强制：

| Command | 强制 |
|---|---|
| `ApplyTenure.Issue(h, digest, expires)` | 检查 `digest == hash(Legacy_previous)`（Inheritance lineage），原子写 `Lease(h)`（Primacy） |
| `ApplyHandover.Seal()` | 写 `Legacy.frontiers = piggybacked frontier`（为 Inheritance 提供源） |
| `ApplyHandover.{Confirm,Close,Reattach}` | 推进 Finality.stage（Finality） |

Consumer 侧的 `Silence` 由 Witness Verifier 强制（§6.5）。

### 6.4 生命周期状态机

```
                      Issue                  Seal
      (nothing)  ─────────────▶  Active  ─────────────▶  Sealed
                                   │                       │
                                   │                       │ Confirm
                                   │                       │ (Inheritance verified)
                                   │                       ▼
                                Reattach                Covered
                                   ▲                       │
                                   │                       │ Close
                                   │                       │ (Finality required)
                                   │                       ▼
                                   └─────────────── Closed
```

五个阶段对应四个持久化 `Finality.stage`：`Unspecified → Confirmed → Closed → Reattached`。

### 6.5 三层纵深防御

**Gate Layer (Server pre-action gate)**. 在 `coordinator/server/service_admission.go:eunomiaGate`。每个 Alloc / TSO / GetRegionByKey / Seal / Close / Reattach 请求执行前检查四条保证。

**Witness Layer (Client witness verifier)**. 在 `coordinator/client/client.go`。每个 reply 带 `(epoch, frontiers, root_token)`。Client 维护三个单调 floor 验证 reply 不回退。

**Audit Layer (Offline audit)**. `coordinator/audit/`。对 rooted snapshot + reply trace 做事后扫描，按 `Primacy / Inheritance / Silence / Finality` 分类异常。

每一层失效时另两层能独立拦截。

---

## 7. 正确性论证

我们不追求全 TLAPS 证明。给出四条保证各自的关键引理和组合性论证。

### 7.1 Primacy

**Lemma 1 (Uniqueness)**. 在 R 的 linearizability 假设下，`Tenure` 对象是 single-writer single-value 类型。`Issue` 命令原子替换它。因此任一时刻最多一个 holder。

### 7.2 Inheritance

**Lemma 2 (Coverage via Lineage)**. 设 `T2` 是 `T1` 的继任。`T2.digest = hash(Legacy_{T1.holder})`，而 `T1.holder` 完成 `Seal` 时必须把 `frontiers` 写入 `Legacy`。命令 `Issue(T2)` 在 R 侧执行 `verify T2.frontiers ≥ Legacy.frontiers`（代码：`ValidateLeaseStartInheritance`）。

### 7.3 Silence

**Lemma 3 (Post-Seal Reply Admission)**. Client 维护 `floor[holder] = max epoch seen`。收到 Seal 事件后 floor 不会回退。任何 `reply.epoch ≤ floor` 被拒。

**注意**：`Silence` 在实现里是**两层**保护：(i) Server 在 Seal 后不再产生带旧 era 的 reply；(ii) Client 即使收到 stale reply 也拒绝。(i) 依赖 server 正确实现；(ii) 是防御性层。

### 7.4 Finality

**Lemma 4 (Finality before Next Issue)**. `ApplyTenure.Issue` 命令的前置条件包含 `Handover(h).stage ∈ {Closed, Reattached}`（见 `coordinator/protocol/eunomia/handoff.go:ValidateHandoverFinality`）。`Handover.stage` 的推进只能通过 `Confirm → Close → Reattach`，每一步都要求 successor coverage 验证通过。因此下一轮 `Issue` 之前上一轮 `Handover` 必须闭合。

### 7.5 组合性

**Theorem 2 (Eunomia Strict Separation)**. `Primacy ∧ Inheritance ∧ Silence ∧ Finality` 的合取严格强于 §4 中任一方案或方案组合覆盖的性质。

**证明要点**：由 §4.16 的覆盖矩阵与 Theorem 1 组合得出。

### 7.6 形式化对照

`spec/` 下有六个 TLA+ 模型：`Eunomia`、`EunomiaMultiDim`（正模型）；`LeaseOnly`、`LeaseStartOnly`、`TokenOnly`、`ChubbyFencedLease`（反模型）。TLC 和 Apalache 都检查过，输出在 `spec/artifacts/`：

- `Eunomia.tla` 满足 `G1_Eunomia ∧ G2_PrimacyInductive ∧ G3_Silence ∧ G4_Finality`
- `LeaseOnly.tla` 违反 `NoOldReplyAfterSuccessor`
- `TokenOnly.tla` 违反 `NoOldReplyAfterSuccessor`
- `ChubbyFencedLease.tla` 违反 `SuccessorCoversHistoricalFrontiers`
- `LeaseStartOnly.tla` 违反 `NoWriteBehindServedRead`
- `EunomiaMultiDim.tla` 满足 `NoWriteBehindServedRead`

这些反例是 §4.16 矩阵里 ❌ 的机器化见证。

---

## 8. 案例研究

### 8.1 CRDB #66562（Inheritance 违反，5 年悬案）

时间线见 §2。Eunomia 的拦截点：n1 服务 `k@9` 时把 ts=9 通过 piggyback 写入 `Legacy.frontiers[lease_start]`；n3 的 `Issue` 命令在 R 侧检查 `n3.lease_start ≥ Legacy.frontiers[lease_start]`，由于 `8 < 9`，请求被拒绝（返回 `ErrInheritance`）。n3 退让，client 看到 NotLeader 并重试，最终以 lease_start = 10 成功。

实现：`benchmark/eunomia/crdb/crdb_66562_issue_test.go`。

### 8.2 etcd #21638（Silence 违反，我们上报）

时间线：
```
t=1  KeepAlive stream opened
t=2  server 在 revision=2 产生 KeepAliveResponse(ttl>0) 入 send buffer
t=3  server 在 revision=3 执行 Revoke
t=4  client 收到 buffered revision=2 的 success reply，误认为 lease 仍健康
```

Eunomia 的拦截点：Witness Layer client 维护 `revokeObserved=true` 之后的 epoch floor；revision=2 的 reply 被拒。

实现：`benchmark/eunomia/etcd/etcd_delayed_lease_test.go`。已向 etcd 上游上报为 etcd#21638。

### 8.3 Cascading Chain（Finality 违反，设计反例）

连续 handoff `A → B → C → D`，每次 `Seal` 的 frontier 需要被下一任继承。若只有 `Primacy + Inheritance + Silence` 而没有 `Finality`，存在 race：C 在 B 的 `Handover` 未 `Close` 时 issue D，D 可能只继承 B 的 `Legacy` 而错过 A 的 `Legacy`（因为 A 的 `Handover` 还停在 `Confirmed` 状态）。

实现：`spec/Eunomia.tla` 在关闭 `Finality` 验证时可产生反例。

---

## 9. 讨论

### 9.1 Eunomia 不是什么

- **不是共识协议**。Eunomia 构造在任何 linearizable RootStore 之上。
- **不是通用一致性模型**。它是**生命周期正确类**，描述 authority 如何合法切换，不描述读写如何排序。
- **不解决 Byzantine**。
- **不保证性能**。Gate Layer pre-action gate 实测有 20–26% overhead（`benchmark/eunomia/results/`）。

### 9.2 Eunomia 与经典正确类的关系

- **Linearizability** (Herlihy-Wing 1990)：读写的全序。在 Eunomia 四条保证成立的前提下，服务级可见行为是 linearizable 的；Eunomia 是其**前置条件**之一，不是其替代。
- **Sequential Consistency** (Lamport 1979)：比 linearizability 弱，不要求实时序。与 Eunomia 正交。
- **Session Guarantees** (Bayou 1995)：client session 级。Eunomia 是 authority 级。层级不同。
- **View-stamped Replication** (Liskov-Oki 1988, Liskov-Cowling 2012)：VR 提供 `Primacy` 的一种实现；不保证 `Inheritance` 或 `Finality`。
- **Chubby Sequencer** (Burrows 2006)：sequencer 是 `Silence` 的一种实现；不保证 `Inheritance` 或 `Finality`。

### 9.3 实现代价

对一个已有 rooted metadata store 的系统：
- RootStore schema 增加 3 个类型化对象，约 200 行 Go
- Service 层 eunomiaGate 约 150 行
- Client witness verifier 约 300 行
- 审计工具 eunomia-audit（可选）约 500 行

热路径 overhead 20–26%。对比 CRDB #66562 开放 5 年的历史代价，这是合算的。

### 9.4 局限

- `Inheritance` 的 `frontiers` 格式需要 per-duty 定义。已有 lease-start 一个实例（`MandateLeaseStart`）；每加一个 duty 需要在 `rootproto.MandateFrontiers` 里定义 projection。
- 跨系统 audit 目前是 synthetic fixture 驱动；live trace adapter 需要为每个被审计系统写 collector。
- 不处理 authority 切换以外的过渡（membership change、region split）。

---

## 10. 相关工作（按主题而非时间）

**时间与排序**. Lamport 1978 (happens-before / logical clocks) 提供了事件排序的基础。HLC (Kulkarni et al. 2014) 把物理与逻辑时钟合一。TrueTime (Corbett et al. 2012) 用硬件 bound 支撑 Spanner。这些工作解决**事件排序**，不解决 authority 下的**状态继承**。

**共识**. Paxos (Lamport 1998), VR (Liskov-Oki 1988, Liskov-Cowling 2012), Raft (Ongaro-Ousterhout 2014) 解决节点对值的一致决定。Eunomia 使用它们作为 RootStore 的实现，不替代它们。

**一致性模型**. Linearizability (Herlihy-Wing 1990), Sequential Consistency (Lamport 1979), Session Guarantees (Terry et al. 1995 Bayou), Snapshot Isolation. Eunomia 是**authority 生命周期**级的正确类，与这些**数据访问**级的正确类正交但互补。

**分布式锁与 fencing**. Chubby (Burrows 2006), ZooKeeper (Hunt et al. 2010). Sequencer / ephemeral node 实现 `Primacy` 和 `Silence` 的部分。

**元数据存储架构**. Delos (Balakrishnan et al. 2020) 的 truth/service 分离思想启发了 Eunomia 的 RootStore 层。FoundationDB (Zhou et al. 2021) 的 coordinator/resolver 分层是另一种实现。

**形式化工具**. TLA+ (Lamport 2002). Kondo (Zhang et al. OSDI 2024) 和 Basilisk (Zhang et al. OSDI 2025 Best Paper) 提供分布式协议证明自动化。Smart Casual Verification of CCF (Howard et al. NSDI 2025) 把 TLA+ 绑到实现。Eunomia 目前到 TLA+ bounded check，未做 refinement。

**可信 Lease**. T-Lease (Kaplan et al. DSN 2021) 对抗 Byzantine 时钟。Eunomia 是 crash-recovery 模型下的正交工作。

**现有实战修复尝试**. CockroachDB PR #73001 尝试 write-time read-summary gate，停留在 in-memory 层，被 cascading chain 反例证明不够；Eunomia 通过 rooted `Legacy.frontiers` 解决同一问题。Chubby sequencer 是 `Silence` 的生产实现；TiKV region epoch 是 `Primacy + Silence` 的组合。

**负证据**. Jepsen/Antithesis 2025 的权威 glossary 经全文检索不包含 authority handoff gap 相关的 correctness class 条目。这是 Eunomia 能独立命名这个问题的外部依据。

---

## 附录 A. 核心 API

```go
// meta/root/state/eunomia.go

// Primacy + lineage
func ValidateTenureClaim(
    current Tenure, seal Legacy,
    holderID, lineageDigest string,
    expiresUnixNano, nowUnixNano int64,
) error

// Inheritance (lease-start mandate)
func ValidateLeaseStartInheritance(
    seal Legacy,
    successorLeaseStart uint64,
) error

// Inheritance (general)
func EvaluateInheritance(
    current Tenure, seal Legacy,
    frontiers MandateFrontiers,
) InheritanceStatus

// coordinator/protocol/eunomia/handoff.go
func BuildHandoverWitness(...) HandoverWitness              // Witness Layer
func ValidateHandoverConfirmation(...) (HandoverWitness, error)   // Inheritance + Finality
```

## 附录 B. TLA+ 对照家族

`spec/` 下六份模型，配置在 `spec/*.cfg`，golden 输出在 `spec/artifacts/`：

| 模型 | 正/反 | 关键不变式 |
|---|---|---|
| Eunomia.tla | 正 | G1_Eunomia, G2_PrimacyInductive, G3_Silence, G4_Finality |
| EunomiaMultiDim.tla | 正 | NoWriteBehindServedRead |
| LeaseOnly.tla | 反 | 违反 NoOldReplyAfterSuccessor |
| LeaseStartOnly.tla | 反 | 违反 NoWriteBehindServedRead |
| TokenOnly.tla | 反 | 违反 NoOldReplyAfterSuccessor |
| ChubbyFencedLease.tla | 反 | 违反 SuccessorCoversHistoricalFrontiers |

运行：
```
make tlc-eunomia
make tlc-leaseonly-counterexample
make apalache-check-eunomia
```

---

**End**
