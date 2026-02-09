# 2026-02-09 MemTable + Arena + Index（Skiplist / ART）设计心得

这份笔记整理 NoKV 在 **MemTable 层** 的核心设计：生命周期、WAL 绑定、Arena 内存分配策略，以及两种索引实现（Skiplist / ART）的工程取舍。目标是回答三个问题：

1. 为什么这样设计。  
2. 两种实现到底差在哪。  
3. 线上如何选型和调参。  

---

## 一页摘要（TL;DR）

**核心思路**：MemTable 不是“纯内存结构”，而是与 WAL segment 绑定的写入阶段单元。写入先追加 WAL，再更新内存索引；达到阈值后轮转为 immutable，后台 flush 为 SST，再通过 manifest checkpoint 释放旧 WAL。

| 设计点 | NoKV 实现 | 直接收益 | 代价/约束 |
| :-- | :-- | :-- | :-- |
| MemTable 抽象 | `memIndex` 接口（`Add/Search/Iterator/MemSize/Ref`） | 索引可插拔 | 统一抽象会屏蔽部分实现细节优化 |
| WAL 绑定 | `memTable.segmentID` + `walSize` | 恢复与回收路径清晰 | 轮转主要看 WAL 字节而非纯内存占用 |
| Arena 分配 | chunk 化、无锁 bump 分配、对齐保障 | 低分配开销、低 GC 压力 | 内部碎片不可避免，释放粒度粗 |
| Skiplist | 概率层高 + CAS 链接 | 实现成熟、语义直观 | 写放大与指针开销更高 |
| ART | 前缀压缩 + Node4/16/48/256 + COW/CAS | 热点读写性能强、顺序扫描好 | 实现复杂，调试门槛高 |

---

## 1. 设计目标（工程视角）

1) **写入路径可恢复**：任何时点 crash 后都能按 WAL + manifest 重建状态。  
2) **索引实现可替换**：同一 LSM 流程下允许 skiplist/art 切换。  
3) **内存分配可控**：减少小对象分配和 GC 抖动。  
4) **flush 回收可证明**：SST 安装成功后才能推进 WAL checkpoint 并删除旧段。  

---

## 2. MemTable 生命周期

### 2.1 结构与抽象

`memTable` 结构（`lsm/memtable.go`）：

```go
type memTable struct {
    lsm        *LSM
    segmentID  uint32
    index      memIndex
    maxVersion uint64
    walSize    int64
}
```

索引抽象：

```go
type memIndex interface {
    Add(*kv.Entry)
    Search([]byte) kv.ValueStruct
    NewIterator(*utils.Options) utils.Iterator
    MemSize() int64
    IncrRef()
    DecrRef()
}
```

代码锚点（便于和实现逐行对照）：

| 主题 | 代码位置 |
| :-- | :-- |
| MemTable 抽象与写入 | `lsm/memtable.go`（`memTable` / `setBatch` / `recovery`） |
| 轮转与 flush worker | `lsm/lsm.go`（`Set` / `SetBatch` / `rotateLocked` / `submitFlush`） |
| Flush 安装与 log pointer | `lsm/levels.go`（`flush` / `canRemoveWalSegment`） |
| memtable engine 选择 | `lsm/memtable.go`（`newMemIndex`） |
| ART 主实现 | `utils/art.go`（`tryInsert` / `lowerBound` / `artIterator`） |
| Arena 分配器 | `utils/arena.go`（`allocate` / `allocAligned` / `ensureChunk`） |

---

### 2.2 写入路径（单条/批量）

```mermaid
sequenceDiagram
  participant DB as commitWorker
  participant LSM as lsm.Set/SetBatch
  participant MT as memTable.setBatch
  participant WAL as wal.Manager
  participant IDX as memIndex

  DB->>LSM: SetBatch(entries)
  LSM->>MT: setBatch(entries)
  MT->>WAL: Append(payloads...)
  MT->>IDX: Add(entry...)
  IDX-->>LSM: in-memory visible
```

关键点：

* `setBatch` 先编码并 `wal.Append`，再 `index.Add`（`lsm/memtable.go`）。  
* `walSize` 按 WAL 追加长度累加，用于轮转和 checkpoint。  
* 批量路径在同一 memtable segment 内尽量合并写入，减少锁与系统调用开销。  

线性化点与并发语义（实现视角）：

* 线性化点在 `wal.Append` 成功后；索引更新发生在其后，因此“可见即可恢复”。  
* `Set/SetBatch` 常态走 `RLock`；仅在容量触发时升级到 `Lock` 执行 `rotateLocked`。  
* `rotateLocked` 将旧表放入 `immutables`，新表切换到新 WAL segment，旧表再异步 flush。  

```mermaid
flowchart LR
  A["RLock: write to active memtable"] --> B{"walSize + estimate > limit"}
  B -->|No| C["setBatch: Append WAL then index.Add"]
  B -->|Yes| D["Unlock RLock and take Lock"]
  D --> E["rotateLocked: active->immutable and NewMemtable"]
  E --> F["submitFlush(old) async"]
  F --> A
```

---

### 2.3 轮转、flush、恢复

```mermaid
flowchart TD
  W["Write to active memtable"] --> C{"walSize > MemTableSize ?"}
  C -->|No| W
  C -->|Yes| R["rotate: active -> immutable + new segment"]
  R --> F["flush immutable -> SST(L0)"]
  F --> M["manifest: AddFile + EditLogPointer"]
  M --> G["remove eligible WAL segment"]
  Crash["Crash/Restart"] --> Replay["replay WAL segments > log pointer"]
  Replay --> Rebuild["rebuild memtables + restore active"]
```

关键点：

* 新建 memtable 时会 `SwitchSegment(newFid)`，将内存与 WAL 段一一对应。  
* `flush` 成功后，manifest 同时记录 `AddFile` 与 `EditLogPointer`，再尝试删除可删 WAL 段。  
* `recovery` 根据 manifest 的 log pointer 删除/跳过旧段，仅重放必要 WAL。  

状态机视角：

```mermaid
stateDiagram-v2
  [*] --> Active
  Active --> Rotating: "walSize threshold"
  Rotating --> ImmutableQueued
  ImmutableQueued --> Flushing
  Flushing --> Installed: "SST + manifest edits"
  Installed --> WalGC: "eligible segment remove"
  WalGC --> [*]
```

---

### 2.4 一个重要设计取舍

当前轮转阈值主要由 `walSize` 判断（`lsm/lsm.go`），而不是严格由 `index.MemSize()` 驱动。  

含义：

* 优点：和恢复/checkpoint 语义一致，逻辑简单。  
* 代价：对 ART 这类结构，索引真实内存占用与 WAL 字节不总是线性一致，可能出现“内存先紧张但未轮转”或“较早轮转”的偏差。  

---

## 3. Arena 设计（两种索引共用）

### 3.1 为什么要 Arena

MemTable 高频写入会产生大量短生命周期对象。若完全走 Go heap，小对象分配/回收会明显放大 GC 成本。  

Arena 目标：

* 将 key/value/node 以“偏移量”组织在连续 chunk 中。  
* 用原子 bump 指针分配，减少锁争用。  
* 通过 refcount 在结构层统一释放整块资源。  

---

### 3.2 关键机制

* `Arena.allocate`：CAS 推进 `n`（偏移），按 chunk 边界切分。  
* `ensureChunk`：按需创建 chunk，避免一次性大分配。  
* `allocAligned`：保证对齐，满足原子读写需求。  

---

### 3.3 对齐是硬约束

`skiplist` 的 value 走 `atomic.LoadUint64`，因此 node/value 对齐必须保证（`utils/arena.go` 中 `nodeAlign` 相关注释）。这属于“正确性前提”，不是性能微调。

---

### 3.4 Arena 的工程权衡

* 优点：低 GC、低碎片元数据开销、并发分配快。  
* 缺点：对象不能细粒度释放，生命周期跟随整棵索引；长期运行下需要依赖 memtable 轮转做阶段性回收。  

---

## 4. Skiplist 设计

### 4.1 结构

* 概率层高（`randomHeight`）  
* 每层前向指针 CAS 链接  
* 节点和 value 都在 Arena，节点里保存 offset

---

### 4.2 写入语义

`Add` 路径先做 splice，若 key 已存在直接覆盖 value；否则分层插入新节点。  

优势：

* 逻辑直观、可维护性高。  
* 在 MVCC key（内部 key 已含 ts）场景天然有序。  

代价：

* 指针塔结构 + 随机层高在高写入下有更多元数据开销。  

---

### 4.3 迭代器与引用计数

`NewIterator` 会 `IncrRef`，`Close` 时 `DecrRef`。这保证 flush/读并发时索引不会提前释放。

---

## 5. ART 设计

### 5.1 结构

* Node4/16/48/256 自适应节点。  
* 前缀压缩减少重复 key 前缀开销。  
* leaf 保存完整 key + value，支持 lower-bound 查找。  

节点升级路径：

```mermaid
flowchart LR
  N4["Node4"] -->|count >= 4| N16["Node16"]
  N16 -->|count >= 16| N48["Node48"]
  N48 -->|count >= 48| N256["Node256"]
```

---

### 5.2 并发模型（关键）

代码注释明确采用 **COW + CAS**：

* 读路径 lock-free，读取不可变节点快照。  
* 写路径复制 payload/节点后 CAS 安装。  
* 插入冲突时重试（`tryInsert` 循环）。  

这套模型让 ART 在高并发读写下保持较好吞吐与扫描顺序稳定性。

插入流程（对应 `tryInsert`）：

```mermaid
flowchart TD
  A["Set(key, value)"] --> B["tryInsert"]
  B --> C{"root == nil"}
  C -->|Yes| D["new leaf and CAS root"]
  C -->|No| E["walk by prefix + child byte"]
  E --> F{"node is leaf"}
  F -->|same key| G["storeValue (atomic)"]
  F -->|different key| H["splitLeaf then replaceChild CAS"]
  F -->|No| I{"prefix mismatch"}
  I -->|Yes| J["splitPrefix then replaceChild CAS"]
  I -->|No| K{"child exists"}
  K -->|Yes| E
  K -->|No| L["clone payload insert child and CAS"]
```

写路径关键细节：

* 非 root 替换通过 `parent.payloadOffset.CompareAndSwap` 完成，避免全局锁。  
* `insertChild` 会在必要时进行 `Node4->16->48->256` 升级。  
* CAS 失败后外层重试，保证并发写下的一致安装。  

---

### 5.3 查找语义（Search/Seek）

`Search` 的语义不是“精确路径命中即返回”，而是：

1. 先用 `lowerBound(target)` 找到第一条 `>= target` 的 leaf。  
2. 再用 `kv.SameKey` 校验是否同一 user key。  
3. 不同 key 则返回 miss。  

```mermaid
flowchart LR
  A["Search(target)"] --> B["lowerBound(target)"]
  B --> C{"leaf exists?"}
  C -->|No| D["miss"]
  C -->|Yes| E{"SameKey(target, leaf.key) ?"}
  E -->|Yes| F["return leaf value"]
  E -->|No| D
```

这种实现能直接复用范围查找逻辑，也让 `Seek` 与 `Search` 在行为上统一到 lower-bound。

---

### 5.4 迭代器

ART 迭代器维护栈帧（`iterFrame`），通过 `descendToMin/advance` 做中序遍历。Seek 使用 lower-bound，再回填路径栈，适合范围查询与 MVCC merge。

```mermaid
sequenceDiagram
  participant It as artIterator
  participant T as artTree
  participant S as stack

  It->>T: Seek(key)
  T-->>It: lowerBound leaf
  It->>S: buildStackToLeaf(leaf)
  loop Next
    It->>S: advance to next child/subtree
    It->>It: descendToMin(child)
  end
```

---

## 6. Skiplist vs ART：当前代码下的实测与解读

本地微基准（`go test ./utils -bench ... -benchmem -benchtime=1s`）：

| Benchmark | 结果（Apple M3 Pro / darwin arm64） |
| :-- | :-- |
| `BenchmarkARTInsert` | `~255 ns/op` |
| `BenchmarkARTGet` | `~73 ns/op` |
| `BenchmarkSkiplistInsert` | `~810 ns/op` |
| `BenchmarkSkiplistGet` | `~316 ns/op` |

解读：

* 在当前实现与测试模型下，ART 插入/读取都明显更快。  
* 但这不是“绝对结论”：两组 benchmark 负载模型并不完全对齐（key 空间与重置策略不同），更适合当趋势信号。  

---

## 7. 选型与调参建议

1. 默认稳态优先：`mem_table_engine = "skiplist"`  
适合先追求可维护性和行为可预期。  

2. 读密集/热点明显：`mem_table_engine = "art"`  
适合低延迟、范围遍历较多的场景。  

3. 配合 `MemTableSize` 一起调  
增大 memtable 可降低 flush 频率，但会增加恢复重放成本。  

4. 观察 flush/WAL 指标联动  
如果 flush backlog 长期增长，先检查 compaction 和磁盘，而不是只调 memtable engine。  

---

## 8. 小结

NoKV 的 MemTable 设计本质是：**WAL 语义优先 + 索引可插拔 + Arena 降 GC**。  

* Skiplist 提供稳定、清晰、低认知成本的默认路径。  
* ART 提供更激进的性能上限，尤其在读与有序遍历上。  
* 生命周期（rotate/flush/recovery）和 manifest checkpoint 的一致性，是整个设计最关键的工程价值。  
