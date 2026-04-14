# 面向层级命名空间的 Listing Layer：建立在 LSM 基线之上的最小研究提案

> 状态：research proposal
> 定位：systems/storage workshop 方向
> 目标：不是替换 NoKV 的 LSM 主引擎，而是在其上研究一个 namespace-aware listing component，用于高效支撑分布式文件系统的 `list(dir)` 与对象存储的 `List(prefix)`。

## 1. 问题定义

NoKV 当前是一个以 LSM-tree 为单机基线、再向上扩展到 WAL / MVCC / raftstore / control plane 的分布式 KV 存储系统。这个基线对通用 KV 工作负载是合理的，但对层级命名空间元数据存在一个长期的结构性错配：

- key 虽然共享长前缀，但底层布局仍将它们视为扁平字节串
- `list(dir)` / `List(prefix)` 是一等操作，但 flat range scan 只是通用 KV 上的退化表达
- 同一目录或同一前缀下的大量并发创建容易聚集到单一逻辑热点
- namespace 层面的局部性、分页和恢复语义，没有在主引擎边界被显式表达

本文拟研究的问题不是“能否重新发明一个新的 metadata engine”，而是更窄的一类问题：

> 在不替换现有 LSM 主引擎的前提下，能否引入一个最小的 namespace-aware listing layer，使分布式 KV 系统同时高效支撑 DFS 的 `list(dir)` 和 object store 的 `List(prefix)`，并把一致性、恢复、写放大、分页与热点前缀行为控制在可接受范围内。

## 2. 为什么这是一个真实问题

该问题并非凭空创造，而是对象存储、分布式文件系统与数据湖场景中的常见元数据访问模式：

- 对象存储：`ListObjects(prefix)`、continuation token、delimiter 语义
- 文件系统：`list(dir)`、目录枚举、热点目录下海量 create
- 数据湖：共享长前缀的分区目录遍历
- AI / checkpoint：同一前缀下高频写入与枚举

工业界和学术界已经存在两类相关路径：

- 通用 KV/LSM 路线：
  - RocksDB prefix extractor / prefix seek
  - Ozone OM + RocksDB
  - JuiceFS / SeaweedFS 使用通用外部 KV 或 SQL 作为 metadata backend
- namespace-aware 路线：
  - CephFS dynamic subtree partitioning
  - IndexFS
  - DeltaFS
  - FractalBits 的 Fractal ART

这说明问题是真实的，但也说明论文必须把边界写清楚：本文不声称“首次发现 namespace metadata 与 flat KV 的错配”，而是研究在 **LSM-based distributed KV substrate** 上引入一个最小 listing 组件是否值得、以及该组件应该如何设计。

## 3. 为什么不是“直接在 RocksDB 上做”

reviewer 一定会问：RocksDB 已经有 prefix extractor、prefix seek、Bloom/filter/index 优化，为什么不直接在 RocksDB 上做？

本文必须正面承认：

- RocksDB-style prefix tuning 是强 baseline
- 它能缓解一部分 prefix-heavy workload 的 seek 和 SST 过滤成本

但本文要研究的是更窄、也更难替代的问题：

> 即使保留 flat-LSM baseline 并认真做 prefix-aware tuning，hierarchical metadata workload 仍然缺少一种 namespace-aware persistent organization，用来直接服务 `list(dir)` / `List(prefix)`、分页和热点前缀并发。

也就是说，本文不是否定 RocksDB，也不是要替代通用 LSM 主引擎；本文要研究的是：

- flat truth keyspace 是否应该继续保留
- 在此基础上，是否还需要一个专门的 listing layer
- 这个 listing layer 的最小协议边界是什么

## 4. 为什么在 NoKV 上做，而不是在 RocksDB 上单独做

NoKV 在这里扮演的不是“替代 RocksDB 的竞争性引擎”，而是一个 **distributed KV research substrate**。

选择 NoKV 作为研究平台的理由是：

- NoKV 已经具备完整的 LSM / WAL / SST / MVCC / raftstore / control plane 基线
- 可以在不改主引擎根部语义的前提下，比较 baseline 与新设计
- 可以把 listing layer 放到 end-to-end 的 distributed setting 里观察一致性、恢复、复制与热点行为
- 可以用统一 benchmark、integration test 和 recovery test 验证设计，而不是只做单机 RocksDB patch

换言之，本文要研究的是 **LSM-based distributed KV 上的 metadata-structural boundary**，不是单机 RocksDB tuning 本身。

## 5. 核心设计主张

本文不拟提出新的主引擎，而是提出一个更小的系统组件：

> 一个建立在 flat metadata truth 之上的 namespace-aware listing layer。

其核心原则如下：

- authoritative truth 仍然保留在现有 full-path metadata keyspace 中
- listing layer 只优化 namespace enumeration、分页和热点前缀并发
- listing layer 不抢 authoritative truth 的角色
- listing layer 的更新必须与 metadata truth 更新处于同一事务或同一持久化边界

### 5.x 与现有 ART memtable 的跨层结合

本文不把 ART 视为创新点本体。ART 在 NoKV 中已经是现有 memtable 的可选/默认有序内存索引；本文利用的是这个既有能力，而不是重新提出 ART。

更准确的结合方式是一个三层结构：

- `M|full_path`：authoritative metadata truth
- `LD|parent|page|child`：mutable listing deltas，作为最近 namespace 更新的细粒度前端
- `L|parent|page`：materialized listing pages，作为持久化 companion listing

其中：

- mutable tier 利用现有 ART-backed memtable 承接最近的 namespace 更新，因为这些 delta key 天然共享长前缀，适合 ART 的有序前缀聚集能力
- persistent tier 仍然是 paged listing index，而不是把 ART 直接当成新的主持久化结构
- authority 始终留在 full-path truth 中，不会被 listing layer 或 memtable state 抢走

这意味着本文的系统主张不是“用 ART 解决 namespace”，而是：

> 在不替换 NoKV 现有 LSM 主引擎的前提下，利用其 ART-backed mutable tier 作为 namespace delta 的 staging area，再将其物化为 paged listing companion structure。

### 5.0 correctness contract

本文把 listing layer 定义为 **truth-authoritative + companion listing**，并坚持以下三个 contract：

- `Lookup(path)` 只认 authoritative truth；listing layer 不能独立决定对象存在性。
- metadata truth 与 listing state 必须在同一提交边界内更新；本文不依赖异步修复或后台补偿来维持基本正确性。
- `List(prefix)` 返回某个 committed snapshot 下的 membership 视图；它不是 cache，也不是 best-effort enumerate。

这三个 contract 是本文与“给 KV 再加一个索引”之间的本质区别。本文不是要把 listing 当成可丢弃的性能缓存，而是要定义一个伴生但受约束的 namespace 结构。

### 5.1 双 keyspace 模型

保留现有 truth keyspace：

```text
M|/bucket/a/b/file1 -> metadata
M|/bucket/a/b/file2 -> metadata
```

新增 listing keyspace：

```text
L|/bucket/a/b|page-0 -> [file1, file2, ...]
L|/bucket/a/b|page-1 -> [...]
```

其中：

- `M|...` 是 authoritative metadata truth
- `L|...` 是 listing layer 的 namespace-aware 伴生索引

### 5.2 ListingPage

listing layer 的基本单位暂定为 `ListingPage`：

- 表示某个 prefix 下的一部分 children
- 支持分页读取
- 支持 page split / shard
- 允许热点前缀横向拆散

每个 page 最小包含：

- child name
- child kind
- 指向 truth entry 的引用或直接 metadata summary
- page metadata（cursor、version、stats）

### 5.3 热点前缀拆分

同一 prefix 下允许存在多个 page：

```text
L|/checkpoint/run-1|a-m
L|/checkpoint/run-1|n-z
L|/checkpoint/run-1|0-9
```

或按 hash shard：

```text
L|/checkpoint/run-1|00
L|/checkpoint/run-1|01
...
```

这不是为了把 listing 变成新的主索引，而是为了避免：

- 大目录被一个大 value 吞掉
- 热点目录下所有 create 都争用同一逻辑页

## 6. 语义边界

这是该方向最容易被 reviewer 攻击的部分，必须先定义清楚。

### 6.1 authoritative truth

`Lookup(path)` 的 authoritative source 始终是 `M|path`。  
listing layer 不负责判定对象是否存在，只负责高效枚举 prefix/dir 下的成员。

### 6.2 一致性

最低要求是：

- `Create/Delete` 对 truth 和 listing page 的更新必须原子提交
- `List(prefix)` 返回的是某个一致快照下的成员视图，而不是尽力而为的异步缓存

如果第一版实现成本过高，论文可以先明确定位为：

- snapshot-consistent listing
- 非独立线性化的 side structure

但不能模糊不写。

### 6.3 rename / mkdir / delete

第一篇 workshop 不建议把 rename 做成主问题。  
更合理的 scope 是：

- `mkdir`
- `create`
- `delete`
- `list`

rename 可以作为讨论项或 future work，除非后续发现它能通过很小的 indirection 做成独立亮点。

### 6.4 pagination / continuation token

对象存储和文件系统都需要分页语义，因此 `ListingPage` 必须天然支持：

- ordered scan within page
- page-to-page continuation
- continuation token 的稳定编码

这是该方向区别于“普通 secondary index”的一个重要点。

### 6.5 ordering boundary

第一版实现有意选择较弱但实用的排序语义：

- page 内按 `Name` 保持确定性顺序
- continuation token 在 page-local 边界内稳定
- 跨 shard 不保证完整全局字典序

这一定义对 DFS 风格的 `list(dir)` 是自然的，对 object store 的 `List(prefix)` 则只覆盖一类实用的 prefix-enumeration workload，而不直接声称兼容所有 S3 general-purpose bucket 的全局 lexicographical ordering 语义。换言之，本文统一的是一类最小 listing contract，而不是完整对象存储 namespace 规范。

## 7. 与现有系统的本质区别

本文需要明确区分四类方案：

### Baseline A：flat full-path range scan

- 当前 NoKV baseline
- 所有 listing 都退化为 flat range scan

### Baseline B：flat LSM + tuned prefix path

- 相当于“认真在 RocksDB 上做 prefix-aware tuning”
- 可以模拟 prefix grouping、scan hint、cache hint 等

### Baseline C：naive secondary index

- 仍然是普通伴生索引
- 但没有 page、pagination、hot-prefix split 的显式设计

### Our Design：namespace-aware listing layer

- 不是取代主引擎
- 不是一般 query secondary index
- 而是针对 `list(dir)` / `List(prefix)` 的一等结构

## 8. 最小实现范围

为了保持 workshop 可交付，第一阶段范围必须收紧：

- 不改 NoKV 的主 LSM / WAL / SST / raftstore 根设计
- 不实现新的主持久化引擎
- 不把 rename 作为第一版核心目标
- 先只做：
  - `create`
  - `delete`
  - `list`
  - 热点前缀 page split

第一版实现再进一步收紧为：

- 先在 `namespace/` 内实现单机 prototype
- 先把 `truth + companion listing` 的提交边界写清楚
- 先把 `LD|...` delta key model 作为 mutable-tier 骨架收进原型
- 暂不修改 `lsm/` 的 memtable 内核，也暂不引入新的 column family

这样做的目的，是先验证数据路径和语义 contract，再决定是否需要把 listing delta 更深地接入现有 ART memtable。

第一版最小接口可以是：

```go
type ListingIndex interface {
    AddChild(parent []byte, child Entry) error
    RemoveChild(parent []byte, name []byte) error
    List(parent []byte, cursor []byte, limit int) ([]Entry, []byte, error)
}
```

## 9. 评测计划

本文的评测 story 不能停留在“list 更快了”，而必须覆盖代价与边界。

### 9.1 workload

- 大量小文件 / 小对象的 create + list
- 热点目录 / 热点 prefix
- 深层 namespace 与扁平 namespace 对比
- mixed workload：put/get/list
- delete-heavy
- recovery / rebuild

### 9.2 指标

- `List(prefix)` latency / throughput
- p50 / p95 / p99
- concurrent create under same prefix
- write amplification
- space amplification
- compaction 开销
- page split 频率与效果
- recovery / rebuild 时间

### 9.3 基线

- Baseline A：flat full-path scan
- Baseline B：flat + tuned prefix
- Baseline C：naive secondary index
- New Design：listing layer

## 10. 预期贡献

如果该方向能做成，最稳的贡献不是“发明了一个新的 metadata engine”，而是下面三点：

1. **问题刻画**  
   系统性量化 flat-LSM metadata truth 在 hierarchical listing workload 上的结构性代价。

2. **最小设计**  
   在不替换主引擎的前提下，引入一个 namespace-aware listing layer，使 `list(dir)` / `List(prefix)` 成为高效一等操作。

3. **边界清晰**  
   明确 listing layer 的一致性、恢复和热点前缀语义，并说明它与 authoritative metadata truth 的关系。

## 11. 风险

该方向的主要风险有三类：

- 如果设计太轻，容易被看成 secondary index patch
- 如果设计太重，容易膨胀成重新造一个 metadata engine
- 如果语义边界写不清，一致性和恢复问题会直接成为 reviewer 的攻击点

因此，第一版必须坚持两个纪律：

- 不替换主引擎
- 不回避一致性与恢复语义

## 12. 当前结论

这是一个适合 NoKV 当前阶段的正式研究方向：

- 它解决的是业界真实问题
- 它不要求推翻 NoKV 现有 LSM-based 架构
- 它允许以最小组件形式逐步验证
- 它有明显的 workshop 潜力

最准确的一句话定位是：

> 在一个 LSM-based distributed KV substrate 上，研究一个 namespace-aware listing layer，使对象存储与分布式文件系统元数据中的 `List(prefix)` / `list(dir)` 不再完全退化为 flat range scan。

## 13. Implementation Plan

为了把该方向控制在 workshop 规模内，建议按三个阶段推进。

### Phase 1：钉死接口与 workload

这一阶段不改存储引擎根部语义，只做三件事：

- 新增 `namespace/` 包，定义 `ListingIndex`、`ListingPage`、`Entry` 等最小抽象
- 在 `benchmark/` 里建立 metadata workload generator
- 用 synthetic baseline 跑通三类基线：
  - flat full-path scan
  - naive secondary index
  - namespace-aware listing pages

这一阶段的目标不是证明生产可用，而是验证：

- benchmark story 是可跑的
- 关键 workload 足够区分设计差异
- 语义边界可以被清晰表达

### Phase 2：单机原型

这一阶段把 listing layer 接到 NoKV embedded 形态上，但仍然不改 LSM 内核：

- authoritative truth 继续保留在现有 metadata keyspace
- listing layer 通过同一 write batch / WAL batch 原子更新
- 支持：
  - `create`
  - `delete`
  - `list`
  - page sharding
  - continuation token

这一步的重点不是“快到什么程度”，而是：

- 一致性语义能否站住
- 写放大是否可接受
- page split 是否真的能缓解热点前缀

### Phase 3：分布式接入

只有在单机原型边界清楚之后，再把设计接到 distributed path：

- 通过现有 raft apply path 更新 metadata truth + listing pages
- 在 region/range 层验证热点前缀行为
- 增加 recovery / snapshot / rebuild 评测

这一步依然不要求重写主引擎，只要求证明：

- 该 listing layer 能在 distributed KV substrate 上工作
- 它的收益不只是单机 cache 偶然改善

## 14. 第一版代码边界

第一版建议严格控制改动范围：

- 新增：
  - `namespace/`
  - `benchmark/namespace/`
- 不改：
  - `lsm/`
  - `wal/`
  - `raftstore/`
  - `coordinator/`

只有当 benchmark 和单机 prototype 已经证明问题值得做，再决定是否进入更深的集成层。
