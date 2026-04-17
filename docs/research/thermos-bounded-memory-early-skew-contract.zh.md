# Thermos：面向 Persistent KV 的 Bounded-Memory Early-Skew Degradation Contract

> 状态：研究设计草稿。
> 定位：storage / systems workshop 到中强会的机制型提案。
> 主张边界：不声称统一 read hotspot、compaction、region scheduling；不声称发明新的 heavy-hitter 数据结构；不把现有 throttle / scheduler / hotspot detector 的拼接包装成研究贡献。本文聚焦一个更窄但更硬的系统性质：**在 persistent KV 中，把“全局 backlog 已形成之后才触发的粗粒度减速”前移为一套 bounded-memory、local-first 的 early-skew degradation contract。**

## 摘要

现有 LSM-based KV 往往在 compaction debt、L0 backlog 或 pending bytes 已经明显累积后，才通过全局 slowdown、write stall、I/O scheduler 或 compaction redesign 进行控制。这类机制当然有效，但它们通常在三个方面显得过粗：第一，触发时机偏晚，很多局部偏斜已经演化成全局 debt；第二，作用对象偏大，经常是 column family 乃至整个 DB 被一并减速；第三，控制语义偏混杂，foreground write path 与 background maintenance path 往往共享一套 backlog-driven 触发器。本文提出 `Thermos`：一套面向 persistent KV 的 **bounded-memory early-skew degradation contract**。它的目标不是统一所有热点，而是更早地暴露两类局部对象上的压力，并只对它们施加语义对齐的局部动作。

`Thermos` 只跟踪两类对象。第一类是 `key pressure`：短时间内对单 key 或小 hotset 的重复写入压力。第二类是 `range maintenance pressure`：某些 key-range 因为反复 overlap、反复进入 compaction、反复制造 rewrite debt 而表现出来的后台维护压力。前者只触发 write-path admission 动作，后者只触发 compaction-path hint 动作。系统不允许把“热点”当成一个大而全的抽象去驱动任意子系统。更具体地说，`Thermos` 的核心 contract 是：**当局部 skew 已足以伤害写路径稳定性、但全局 backlog 还没爆表时，系统必须先对局部对象执行显式动作；只有局部动作无效，系统才允许升级到现有的全局 slowdown / stop。**

`Thermos` 由三部分组成。其一，bounded-memory 观测面：系统用近似吸收层筛出 candidate key 和 candidate maintenance band，再用小规模精确候选层维护对象状态、窗口衰减与动作预算。其二，object-action alignment：`key pressure -> accept/pace/reject`，`range maintenance pressure -> priority hint / worker-share bias / non-urgent deferral`。其三，local-first escalation ladder：局部 admission 与局部 compaction hint 若无法抑制 debt 增长，系统才升级到既有的全局 backlog throttle。本文不把 sketch、top-k、count-min 或 rotating ring 当成创新点主体；这些只是 bounded-memory substrate 的实现手段。真正的贡献点在于：把“何时退化、对谁退化、允许做什么动作、何时升级到全局控制”定义为显式 contract。

我们计划在 NoKV 的真实宿主路径上实现该设计，而不是在玩具模型上伪造结果。NoKV 当前已经具备两个关键接线点：一是 per-key write reject 的热键钩子，二是 picker + scheduler-policy 分离的 compaction 执行面。`Thermos` 因此不需要重写 compaction correctness，也不需要把所有热点逻辑塞进默认读路径。最合理的 v1 形态是：保留 NoKV 现有 global backlog throttle 作为 fallback，在其之前增加一层 local-first contract。我们要证明的 strongest claim 不是“系统更聪明地检测热点”，而是：**bounded-memory、object-aligned、local-first 的 early-skew contract 能在 backlog-driven global throttling 之前，以更细粒度、更低 collateral damage 的方式稳定 persistent KV 的写路径。**

## 1. 引言

### 1.1 问题背景

LSM-based KV 已经有大量关于 write stall、tail latency、compaction interference 与 I/O admission 的研究。工业系统和学术系统都清楚：当 foreground writes、flushes、compactions 叠在一起时，系统会经历剧烈抖动，严重时进入明显的全局 stall。NoKV 当前也不例外：写路径已经具备 per-key hot-write reject 与全局 compaction-backlog-driven slowdown / stop 两套机制，但它们之间仍存在一个空隙。这个空隙就是本文要研究的对象。

这个空隙可以概括为：**很多局部 skew 在“值得全局减速”之前，已经足以伤害局部写路径稳定性，但系统还没有一套明确的局部退化契约。** 结果是，系统常常在两种极端之间摇摆：要么完全不作为，等 backlog 积累起来后再全局减速；要么在某个 subsystem 内埋入 heuristic，把局部现象偷偷转化为广域副作用。两种路径都不理想。前者反应太晚，后者边界不清。

### 1.2 核心观察

本文的核心观察有三条。

第一，persistent KV 中真正值得前移的不是“热点检测”，而是“退化契约”。单纯证明某个 detector 能在 bounded memory 下抓住 hot key，并不足以形成强论文边界。更难也更值得研究的问题是：局部 skew 何时可以被视为需要控制的对象，系统允许对它做什么动作，这些动作何时失效，以及何时才应该升级到现有的全局 throttle。

第二，前台写压力与后台 compaction 压力虽然都叫“热点”，但它们不是同一种对象。对一个单 key 的重复更新，适合的动作是 `admission`；对一个反复产生 overlap 和 rewrite debt 的 key-range，适合的动作是 `maintenance prioritization`。如果系统把两者混成一个“大一统热点层”，最终往往会退化成 heuristic pile。本文因此强调 `object-action alignment`，而不是 semantic unification。

第三，persistent KV 中更值得跟踪的 range 对象不是“访问热门的 range”，而是“制造维护压力的 range”。本文不把第二类对象定义成普通 `range hotness`，而定义成 `range maintenance pressure`。这是一条很重要的边界：我们关心的不是哪个区间“被访问得多”，而是哪个区间在 compaction、overlap、rewrite recurrence 的意义上“正在制造 debt”。

### 1.3 论文主张

本文的主张可以压缩成一句话：

> 在 persistent KV 中，可以用 bounded-memory 的 local-skew contract，在 backlog-driven global throttling 之前，显式地对 key-scoped write pressure 与 range-scoped maintenance pressure 进行局部退化控制。

更展开地说，本文主张四件事：

- 系统应显式地区分 `key pressure` 与 `range maintenance pressure`；
- 两类对象只能触发与自身语义一致的动作；
- 系统必须遵循 local-first、global-fallback 的升级梯度；
- bounded-memory substrate 的价值不在 detector 本身，而在于支撑这套可执行 contract。

### 1.4 贡献

本文计划贡献以下四点：

1. 提出 `early-skew degradation contract`，把 persistent KV 从“等 backlog 出来再全局减速”的被动路径，推进到“先对局部对象执行显式控制、再升级到全局 throttle”的双层退化模型。
2. 提出 `object-action alignment`：`key pressure -> admission`，`range maintenance pressure -> compaction hint`，避免把所有热点信号混成一个大而全的控制层。
3. 提出 `maintenance-pressure range abstraction`：第二类对象不是普通热访问 range，而是 compaction/overlap/rewrite recurrence 驱动的维护压力带。
4. 在真实 NoKV 宿主路径上实现 local-first contract，保留既有 global backlog throttle 作为 fallback，而不修改 compaction correctness 或 flush/recovery 语义。

## 2. 背景与动机

### 2.1 现有 persistent KV 的常见路径

今天的 persistent KV 面对写路径失稳时，常见的办法主要有四类：

- backlog-driven slowdown / stop；
- I/O scheduler 与 foreground/background bandwidth control；
- dataflow harmonization、thread/batch tuning；
- compaction redesign 或硬件协同。

这些路径都解决真实问题，但本文要指出的是一个更窄的 gap：**现有机制大多在“局部 skew 已经外溢成全局 debt”之后才发力。**

### 2.2 为什么“再做一个热点 detector”不够

如果 `Thermos` 的贡献只是：

- bounded-memory hot key 检测；
- 再加一点 compaction hint；

那么它很容易被质疑为：

- detector engineering；
- familiar mechanism remix；
- 或者“把 HotRing、I/O admission、compaction priority 摆在同一页”。

因此本文不把 detector 当论文主体。detector 只是 substrate。真正需要清晰定义的是：

- 哪个对象进入 contract；
- contract 允许的动作是什么；
- 动作失效时系统如何升级；
- 如何证明这种升级梯度比纯全局 backlog throttle 更好。

### 2.3 为什么 range 对象必须重新定义

若第二类对象被定义成普通 `range hotness`，它立刻会与热记录提升、hotness-aware compaction、tiered-storage retention 等工作产生重叠。本文故意把它改写成 `range maintenance pressure`，并要求它至少满足以下特征之一：

- 在短时间内反复成为 compaction overlap 的中心；
- 在多个 round 中反复被 planner/picker 选中；
- 对应区间重写字节数显著大于周边区间；
- 对应区间 debt recurrence 明显高于平均水平。

这使该对象从“访问统计对象”变成“维护压力对象”。这一转向是本文最值得强调的概念创新之一。

## 3. 设计目标与非目标

### 3.1 设计目标

本文的 v1 目标是：

- 在固定内存预算下跟踪足够有用的局部 skew 信号；
- 为 key 写入提供可渐进的 local admission；
- 为 compaction 执行面提供局部 hint，而非重写 correctness；
- 将局部动作纳入显式升级梯度，与现有 global throttle 形成清晰边界；
- 在 NoKV 真实引擎路径上做到可实现、可测、可解释。

### 3.2 非目标

本文明确不做：

- read hotspot、cache、prefetch、layout；
- region split、leader transfer、hot-region balancing；
- 新的 compaction correctness protocol；
- 通用 interval index 或 arbitrary range oracle；
- detector-first 的“谁最热”论文。

## 4. Thermos Contract

### 4.1 核心合同

`Thermos` 的核心合同是：

1. 系统必须在 global backlog throttle 之前，先尝试局部动作。
2. 局部动作只允许作用于对象自己的责任边界。
3. 局部动作失败后，系统才升级到现有 global slowdown / stop。

这三条规则把 `Thermos` 从“一个热点层”变成“一个退化协议层”。

### 4.2 三层合同结构

从协议视角看，`Thermos` 由三层组成。

#### 4.2.1 观测合同

系统必须持续产生两类局部信号：

- `KeyPressure(key)`
- `RangePressure(band)`

这两类信号都必须：

- bounded-memory
- time-windowed
- decayed
- 可审计

#### 4.2.2 动作合同

不同对象只能触发不同动作：

- `KeyPressure -> accept | pace | reject`
- `RangePressure -> priority hint | worker-share bias | non-urgent deferral`

这里不允许跨对象滥用动作。例如，key pressure 不应直接改写 compaction correctness；range pressure 也不应直接把所有写都拒掉。

#### 4.2.3 升级合同

若局部动作仍不足以阻止 debt 增长，系统才允许升级到现有全局控制：

- local pace / reject 无法抑制局部 queue growth；
- compaction hint 后 range debt recurrence 仍持续上升；
- 才进入 global slowdown / stop。

换句话说，`Thermos` 不是替代 global throttle，而是把它从“唯一动作”降级成“fallback 动作”。

## 5. 对象模型

### 5.1 Key Pressure

第一类对象是 `key pressure`。它是一个前台写路径对象，按如下 identity 定义：

```text
KeyObject := (ColumnFamily, UserKey)
```

它不是长期 popularity，而是局部、短窗口、可衰减的重复写入压力。一个 key 是否进入 contract，不只取决于裸计数，还取决于压力是否已经表现出系统意义上的外部性，例如：

- 短窗口写频率；
- burstiness；
- 该 key 对应的 enqueue wait 增长；
- 最近一次局部动作是否有效；
- key 是否在连续窗口内稳定活跃。

### 5.2 Range Maintenance Pressure

第二类对象是 `range maintenance pressure`。它不是任意抽象 range，而是与 compaction maintenance path 直接对齐的对象。最稳妥的定义不是 arbitrary interval，而是 **maintenance band**：

- planner split range；
- ingest shard；
- L0→L1 overlap band；
- 一次 compaction candidate 所对应的 canonical range id。

这样定义有三个好处：

- 内存边界天然存在，不需要维护无界 interval universe；
- 动作语义明确，可以直接映射到 compaction hint；
- 更容易在真实系统中审计 recurrence 与 rewritten bytes。

该对象的典型压力来源包括：

- 同一 band 在连续 rounds 中反复进入 compaction；
- 同一 band 对应的 overlap width 持续偏高；
- 同一 band 的 rewritten bytes / useful progress 比例偏差大；
- 某 band 的 debt 在 hint 后仍持续累积。

## 6. 状态机

### 6.1 Key 状态机

每个 `KeyObject` 在精确候选层中处于以下状态之一：

- `Cold`
- `Watched`
- `LocallySlowed`
- `LocallyRejected`
- `Released`

状态转移原则如下：

- `Cold -> Watched`：近似层认为该 key 在当前窗口达到 candidate 条件；
- `Watched -> LocallySlowed`：压力达到 pace 阈值；
- `LocallySlowed -> LocallyRejected`：pace 无法抑制局部 pressure；
- `LocallySlowed/LocallyRejected -> Released`：冷却窗口内压力显著下降；
- `Released -> Cold`：对象被逐出精确候选层。

这使 write admission 从单阈值 reject 升级为渐进控制，而不是硬断崖。

### 6.2 Range 状态机

每个 `MaintenanceBand` 在精确候选层中处于以下状态之一：

- `Idle`
- `Tracked`
- `Hinted`
- `Escalated`
- `Released`

其典型迁移为：

- `Idle -> Tracked`：近似层发现 recurring debt；
- `Tracked -> Hinted`：对 picker 或 scheduler 发出 priority bias；
- `Hinted -> Escalated`：hint 后 debt recurrence 仍显著上升；
- `Escalated -> Released`：band debt 回落，退出候选层。

注意：`Escalated` 也不直接改变 correctness。它只是推动更强的 hint 或触发系统更早升级到 global fallback。

## 7. 数据结构

### 7.1 两级结构

`Thermos` 使用两级结构。

#### 第一级：近似吸收层

目标是：

- 吸收所有事件；
- 低内存；
- 低路径开销；
- 快速筛 candidate。

可用实现包括：

- count-min sketch
- sampled bucket counters
- space-saving summary

key 与 range 各自维护一套近似层。

#### 第二级：精确候选层

只保存晋升出来的热点对象。每个对象至少记录：

- identity
- 最近窗口压力
- decay / rotation 元数据
- 当前状态
- 上次动作与动作效果
- eviction/cooldown 元数据

对 key，还需要：

- pace budget
- reject cooldown

对 range，还需要：

- recent rewritten bytes
- recurrence count
- hint strength

### 7.2 有界性原则

bounded-memory 的关键不是 sketch 本身，而是：

- 谁能晋升；
- 精确候选层最多容纳多少对象；
- 对象在何时被降级/逐出；
- 突发热点 shift 时如何快速腾挪。

因此本文建议把 memory budget 明确切成：

- `ThermosKeyBudget`
- `ThermosRangeBudget`

并让系统把 budget exhaustion 视为一等运行状态，而不是隐藏细节。

## 8. 动作设计

### 8.1 Key Admission

对 key pressure，系统支持三种动作：

- `accept`
- `pace`
- `reject`

其中：

- `accept`：仅记录观测；
- `pace`：以 key-local budget 或额外 delay 进行局部减速；
- `reject`：返回显式 rejection。

这比单纯 `ErrHotKeyWriteThrottle` 更强，因为它提供了渐进式退化，而不是“超过阈值就立刻拒绝”。

### 8.2 Range Hint

对 range maintenance pressure，v1 只做 hint，不改 correctness path。可选动作包括：

- 为 candidate priority 增加 `ThermosBias`；
- 为特定 queue 保留 worker share；
- 延后低收益、非紧急的 compaction work；
- 在 global debt 尚未高企时优先收敛 recurring band。

重点是：**hint 只改变执行顺序或局部 bias，不改变 candidate generation 的安全边界。**

### 8.3 Local-First Escalation

系统采用如下升级梯度：

1. 仅记录压力；
2. key-local pace / range-local hint；
3. key-local reject / stronger range bias；
4. 若 debt 仍继续增长，升级到现有 global slowdown / stop。

这套梯度是本文最重要的 contract 之一。

## 9. 与 NoKV 的接线点

### 9.1 Write Path

NoKV 当前已具备 per-key reject 钩子：

- `db.maybeThrottleWrite()` 会调用 `ShouldThrottleHotWrite()`；
- 当前动作只有“允许 / 拒绝”。  

这意味着 `Thermos` 最自然的接入方式是把现有单阈值判定升级成三态 admission：

- `accept`
- `pace`
- `reject`

而不是另起一套平行的 write guard。

### 9.2 Global Throttle

NoKV 当前已具备 global compaction-backlog-driven throttle：

- `applyThrottle()` 接收 `WriteThrottleState`
- `sendToWriteCh()` 在 slowdown 时按 rate 休眠，在 stop 时阻塞。

这正好适合作为 `Thermos` 的 fallback 层。换句话说，v1 不应移除这套机制，而应在其之前增加 local-first contract。

### 9.3 Compaction Picker / Scheduler

NoKV 当前已经把 compaction 分成两层：

- picker 负责候选生成；
- scheduler policy 负责执行顺序安排。

这非常适合 `Thermos`：

- range hint 优先注入 scheduler/policy；
- 必要时再轻量影响 priority score；
- 避免直接侵入 compaction correctness。

本文建议 v1 优先走 scheduler/policy 注入，而不是重写 picker 规则。

## 10. 实现草图

### 10.1 关键接口

建议在运行时引入一个显式对象：

```text
Thermos
  ObserveWrite(key, size, enqueueWait, now) -> AdmissionDecision
  ObserveCompactionBand(band, overlap, rewrittenBytes, now)
  HintPriorities(priorities) -> adjustedPriorities
  Snapshot() -> ThermosStats
```

其中：

- 写路径只调用 `ObserveWrite`；
- compaction 完成后调用 `ObserveCompactionBand`；
- scheduler/picker 在每轮开始前调用 `HintPriorities`。

### 10.2 AdmissionDecision

```text
AdmissionDecision
  Action: Accept | Pace | Reject
  Delay:  optional
  Reason: key_pressure | global_fallback | none
```

让 admission 输出成为显式对象，而不是散落在条件分支里的隐式副作用，这是本文推荐的工程风格。

### 10.3 Compaction Hint

```text
ThermosHint
  BandID
  BiasScore
  Recurrence
  Escalated
```

该 hint 只应被 compaction policy 解释为排序/配额 bias，而不是 correctness signal。

## 11. 评估计划

### 11.1 核心假设

本文要验证三条假设：

1. bounded-memory local-skew contract 能早于 global backlog throttle 发现并控制有害 skew。
2. key admission 与 range hint 的组合，比单纯全局 throttle 具有更低 collateral damage。
3. range maintenance pressure 比普通访问热度更适合作为 compaction-side 对象。

### 11.2 对照组

最少需要以下对照：

- NoKV current：per-key reject + global backlog throttle
- global throttle only
- key admission only
- range hint only
- full Thermos

### 11.3 Workloads

最少需要以下 workload：

- uniform
- zipfian hot key
- bursty hot key
- hotspot shift
- recurrent hot-range overlap

### 11.4 指标

不能只看 tail latency。必须至少覆盖：

- P99 / P999 write latency
- cumulative slowdown time
- cumulative stop time
- cold-key throughput loss
- fairness
- queue growth
- compaction debt growth
- rewritten bytes / useful progress
- memory budget sensitivity

## 12. 创新点与边界判断

本文最值得强调的创新点不是 detector，而是以下四条：

1. **Early-skew degradation contract**  
   把局部 skew 的响应时机前移到全局 backlog throttle 之前。

2. **Object-action alignment**  
   key 只对应 admission，maintenance band 只对应 compaction hint。

3. **Maintenance-pressure range abstraction**  
   第二类对象不是普通热访问 range，而是制造后台维护压力的 range。

4. **Local-first, global-fallback ladder**  
   系统先执行局部动作，失败后才升级到全局 throttle。

本文不主张以下内容为创新：

- rotating ring / sketch / heavy hitter 本身；
- “统一所有热点层”；
- “第一次把热点用到 compaction”；
- read hotspot 与 region scheduling 的整合。

## 13. 风险与下一步

### 13.1 最大风险

最大的研究风险不是实现难度，而是 claim 塌缩：

- 若 key admission 只剩单阈值 reject，创新度会迅速下降；
- 若 range pressure 被实现成普通热访问 range，相关工作重叠会明显上升；
- 若 compaction hint 侵入 correctness，系统复杂度会暴涨；
- 若评估只展示 Zipf happy path，审稿人会认为这是 heuristic tuning。

### 13.2 下一步

最合理的下一步是：

1. 先在 NoKV 中实现 `accept / pace / reject` 的 key-local admission；
2. 再实现 maintenance-band tracking 与 scheduler bias；
3. 最后用 real workloads 验证 local-first escalation 是否真的减少 global slowdown/stop。

## 14. 一句话总结

`Thermos` 最合理的研究边界不是“做一个更聪明的热点层”，而是：**在 persistent KV 中，把局部 skew 的响应从隐式 heuristic 与全局 backlog fallback，提升为一套 bounded-memory、object-aligned、local-first 的 early-skew degradation contract。**
