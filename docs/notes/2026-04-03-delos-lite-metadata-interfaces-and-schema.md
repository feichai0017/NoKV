# NoKV Delos-lite Metadata HA 接口与 Schema 草案

> 状态：接口与 schema 草案。本文档在《NoKV Delos-lite Metadata HA 设计草案》基础上继续细化，目标是给出第一版 Delos-lite 的 Go 接口草案、事件 schema 草案、checkpoint 结构草案，以及当前 `meta/root/types.go` 和 `pb/meta/root.proto` 的演进建议。

## 1. 文档目标

前一篇设计文档已经明确了 Delos-lite 的总体形状：

- `event`
- `log`
- `state`
- `checkpoint`
- `pd/view`

这一篇继续回答更具体的问题：

1. `event` 的 Go 接口应该长什么样
2. `log` 应该暴露什么最小接口
3. `state` 的输入输出结构应该是什么
4. checkpoint 里到底存什么
5. `pb/meta/root.proto` 未来应该怎么收敛
6. 当前 `meta/root/types.go` 中哪些结构可以保留，哪些需要迁移

本文档仍然只讨论第一版 Delos-lite，不讨论：

- CURP 快路径
- Bizur-like 分桶共识
- 大规模 metadata sharding

---

## 2. 当前类型现状

当前主要类型集中在：

- `meta/root/types.go`
- `pb/meta/root.proto`
- `meta/codec/root.go`

### 2.1 当前已经有的核心类型

当前已经有：

- `Cursor`
- `State`
- `Snapshot`
- `CommitInfo`
- `EventKind`
- `Event`
- 各种 payload struct

这些结构对 Delos-lite 第一版很有价值，因为它们已经表达了：

- allocator fence
- store membership
- descriptor publish / tombstone
- split / merge
- peer change
- placement policy

### 2.2 当前存在的问题

当前问题不是“完全没有 schema”，而是：

1. `Root` 接口、event schema、checkpoint schema 混在一个包里
2. event 命名里混有 committed truth 和 intent
3. `pb/meta/root.proto` 还带着一些当前 local rooted backend 的历史形状
4. checkpoint 结构当前偏“local backend 实现细节”，还不是明确的 rooted state schema

所以未来不是推翻现有 schema，而是做一次收敛。

---

## 3. 建议的 Go 包与接口布局

## 3.1 `meta/root/event`

建议建立独立包：

- `meta/root/event`

职责：

- 定义 metadata truth event
- 提供事件构造 helper
- 提供事件校验逻辑

建议接口草案：

```go
package event

type Kind uint16

type Event struct {
    Kind Kind
    Body any
}

func (e Event) Validate() error
func (e Event) Clone() Event
```

如果不想用 `any`，可以继续保留当前 union-like 结构，但最好包级职责独立，不和 `Root` 接口放在同一个文件里。

## 3.2 `meta/root/log`

建议建立独立包：

- `meta/root/log`

职责：

- append ordered metadata truth events
- 提供 committed event stream
- 提供 snapshot install / compaction 边界

建议接口草案：

```go
package rootlog

type Cursor struct {
    Term  uint64
    Index uint64
}

type CommittedEvent struct {
    Cursor Cursor
    Event  event.Event
}

type Log interface {
    Append(events ...event.Event) (Cursor, error)
    ReadCommitted(from Cursor) ([]CommittedEvent, Cursor, error)
    Current() (Cursor, error)
    InstallSnapshot(snapshot checkpoint.RootedCheckpoint) error
    Compact(upto Cursor) error
    Close() error
}
```

第一版只做一个实现：

- `raftOrderedLog`

## 3.3 `meta/root/state`

建议建立独立包：

- `meta/root/state`

职责：

- 定义 rooted truth state
- apply committed event
- 导出 rooted snapshot

建议接口草案：

```go
package rootstate

type Machine interface {
    Current() State
    Snapshot() Snapshot
    Apply(cursor rootlog.Cursor, evt event.Event) error
}
```

## 3.4 `meta/root/checkpoint`

建议建立独立包：

- `meta/root/checkpoint`

职责：

- 定义 rooted checkpoint 结构
- 编码/解码 checkpoint
- 边界化恢复流程

建议接口草案：

```go
package checkpoint

type Store interface {
    Load() (RootedCheckpoint, error)
    Save(cp RootedCheckpoint) error
}
```

---

## 4. 事件 schema 草案

## 4.1 事件设计原则

第一版 schema 建议遵守这四条：

1. event 必须显式表达 truth，不依赖推断
2. event payload 必须自描述
3. event 设计优先面向 committed truth，而不是 intent
4. event 应服务于 rooted state materialization，而不是临时控制面逻辑

## 4.2 建议的事件集合

### A. allocator truth

#### `AllocatorFenced`

语义：

- 将某个 allocator domain 的下界推进到一个单调值

建议字段：

- `kind`
- `min`

### B. store membership truth

#### `StoreJoined`

建议字段：

- `store_id`
- `address`
- `labels` 或未来 metadata extension（可选）

#### `StoreLeft`

建议字段：

- `store_id`
- `address`

#### `StoreMarkedDraining`

建议字段：

- `store_id`
- `address`

### C. topology truth

#### `DescriptorPublished`

建议字段：

- `descriptor`

说明：

- 用于 descriptor 的一般性更新或 bootstrap publish

#### `DescriptorTombstoned`

建议字段：

- `region_id`

#### `SplitCommitted`

建议字段：

- `parent_region_id`
- `split_key`
- `left_descriptor`
- `right_descriptor`

说明：

- 这是 committed truth event
- 不建议第一版保留 `SplitRequested` 进入 rooted truth 主日志

#### `MergeCommitted`

建议字段：

- `left_region_id`
- `right_region_id`
- `merged_descriptor`

#### `PeerChangeCommitted`

建议字段：

- `region_id`
- `change_kind`
- `store_id`
- `peer_id`
- `result_descriptor`

其中 `change_kind` 建议明确定义枚举，例如：

- `ADD`
- `REMOVE`

### D. policy truth

#### `PlacementPolicyChanged`

建议字段：

- `version`
- `name`

### E. 未来可选但第一版不建议进入主日志的 event

- `SplitRequested`
- `LeaderTransferIntent`

原因：

- 这类 event 更像 control-plane intent
- 第一版 Delos-lite 主日志更应该只承载 committed truth

---

## 5. Rooted state schema 草案

建议明确把 rooted state 分成下面四块。

## 5.1 `RootControlState`

这是顶层 compact rooted state 的骨架。

建议结构：

```go
type RootControlState struct {
    ClusterEpoch    uint64
    MembershipEpoch uint64
    PolicyVersion   uint64
    LastCommitted   rootlog.Cursor
}
```

这部分当前基本已经存在于：

- `meta/root/types.go:State`

## 5.2 `AllocatorState`

建议结构：

```go
type AllocatorState struct {
    IDFence  uint64
    TSOFence uint64
}
```

第一版不建议把 allocator runtime cache 放进 rooted state。

## 5.3 `StoreMembershipState`

建议结构：

```go
type StoreMembershipEntry struct {
    StoreID  uint64
    Address  string
    State    StoreState
}

type StoreMembershipState struct {
    Stores map[uint64]StoreMembershipEntry
}
```

这里的 `State` 是 truth state，不是 heartbeat-derived health。

## 5.4 `DescriptorCatalogState`

建议结构：

```go
type DescriptorCatalogState struct {
    Descriptors map[uint64]descriptor.Descriptor
}
```

第一版 range index 可以继续作为内存 materialization，不一定要放进 checkpoint 的持久化 schema。持久化 schema 先保守。

## 5.5 顶层 rooted snapshot

建议结构：

```go
type Snapshot struct {
    Control    RootControlState
    Allocator  AllocatorState
    Membership StoreMembershipState
    Catalog    DescriptorCatalogState
}
```

对比当前 `meta/root/types.go:Snapshot`：

- 当前只有 `State + Descriptors`
- 未来可以进一步把 membership 也纳入 rooted snapshot

---

## 6. Checkpoint schema 草案

第一版 checkpoint 我建议叫：

- `RootedCheckpoint`

建议结构：

```go
type RootedCheckpoint struct {
    Snapshot      Snapshot
    LogOffset     uint64
    LastCommitted rootlog.Cursor
}
```

### 6.1 为什么要带 `LogOffset`

这是当前 `meta/root/backend/local/store.go` 已经证明有价值的字段：

- checkpoint 之后只 replay tail
- 恢复成本 bounded
- log compaction 可以知道安全截断点

### 6.2 为什么要带 `LastCommitted`

虽然 `Snapshot.Control.LastCommitted` 已经有，但 checkpoint 顶层直接保留一次也有价值，因为：

- install snapshot
- log truncate
- restore guard

这些地方经常会直接使用它。

---

## 7. Proto schema 草案

当前 proto 入口：

- `pb/meta/root.proto`

未来不需要立刻完全重写，但我建议按 Delos-lite 的目标做一次收敛。

## 7.1 第一版 proto 应保留的结构

建议保留：

- `RootCursor`
- `RootState`
- `RootCheckpoint`
- `RootEvent`
- event payload messages

## 7.2 proto 层建议新增或收敛的点

### A. 拆出更清晰的 checkpoint shape

当前 `RootCheckpoint` 只有：

- `state`
- `descriptors`
- `log_offset`

未来建议演进成：

```proto
message RootCheckpoint {
  RootState state = 1;
  repeated nokv.meta.v1.RegionDescriptor descriptors = 2;
  repeated RootStoreMembership stores = 3;
  uint64 log_offset = 4;
}
```

这样 rooted membership 也能在 checkpoint 中恢复。

### B. event kind 收敛

当前：

- `REGION_BOOTSTRAP`
- `REGION_SPLIT_REQUESTED`
- `LEADER_TRANSFER_INTENT`

这些更像 intent 或 transitional event。

第一版 Delos-lite rooted truth 主日志建议优先保留 committed truth kinds：

- `STORE_JOINED`
- `STORE_LEFT`
- `STORE_MARKED_DRAINING`
- `REGION_DESCRIPTOR_PUBLISHED`
- `REGION_TOMBSTONED`
- `REGION_SPLIT_COMMITTED`
- `REGION_MERGED`
- `PEER_CHANGE_COMMITTED`
- `PLACEMENT_POLICY_CHANGED`
- `ALLOCATOR_FENCED`

### C. allocator fence payload

当前 proto 里没有单独的 `AllocatorFenced` payload message。

建议未来补：

```proto
message RootAllocatorFence {
  uint32 kind = 1;
  uint64 min = 2;
}
```

然后把它加进 `RootEvent.oneof payload`。

### D. peer change payload 收敛

当前 proto 里是：

- `RootPeerChange`
  - `region_id`
  - `store_id`
  - `peer_id`
  - `descriptor`

未来建议再加一层：

```proto
message RootPeerChange {
  uint64 region_id = 1;
  uint64 store_id = 2;
  uint64 peer_id = 3;
  RootPeerChangeKind kind = 4;
  nokv.meta.v1.RegionDescriptor descriptor = 5;
}
```

这样事件语义更完整，不必再依赖 event kind 拆两种 add/remove event。

---

## 8. `meta/root/types.go` 的演进建议

当前 `meta/root/types.go` 混合了：

- control state
- event schema
- Root interface
- helper constructors

这对当前 local rooted backend 是够用的，但对 Delos-lite 不够清楚。

建议未来拆成：

- `meta/root/event/types.go`
- `meta/root/state/types.go`
- `meta/root/log/types.go`
- `meta/root/root.go` 或 `meta/root/api.go`

### 8.1 可以直接保留的

- `AllocatorKind`
- `Cursor`
- `State` 的大部分字段
- `Snapshot` 的核心思想
- 现有大多数 event payload struct

### 8.2 需要收敛的

- `EventKindRegionBootstrap`
- `EventKindRegionSplitRequested`
- `EventKindLeaderTransferIntent`
- `EventKindPeerAdded`
- `EventKindPeerRemoved`

未来应区分：

- committed truth events
- runtime/control intent events

### 8.3 `Root` 接口未来应降到更薄

当前 `Root` 接口同时承担：

- append truth event
- fence allocator
- current state
- snapshot
- read since

未来 Delos-lite 下，更合理的是：

- `rootlog.Log`
- `rootstate.Machine`
- `checkpoint.Store`

分别承担职责，最外层再组合成一个 root service。

---

## 9. 第一版最小接口组合

如果按工程最小集来落，我建议第一版直接暴露这三个接口。

### 9.1 `rootlog.Log`

```go
type Log interface {
    Append(events ...event.Event) (rootlog.Cursor, error)
    ReadCommitted(from rootlog.Cursor) ([]rootlog.CommittedEvent, rootlog.Cursor, error)
    Current() (rootlog.Cursor, error)
    InstallSnapshot(checkpoint.RootedCheckpoint) error
    Compact(upto rootlog.Cursor) error
    Close() error
}
```

### 9.2 `rootstate.Machine`

```go
type Machine interface {
    Current() state.RootControlState
    Snapshot() state.Snapshot
    Apply(cursor rootlog.Cursor, evt event.Event) error
}
```

### 9.3 `materializer.Materializer`

```go
type Materializer interface {
    LoadSnapshot(snapshot state.Snapshot) error
    Apply(cursor rootlog.Cursor, evt event.Event) error
}
```

这样接口边界非常清楚：

- `Log` 只管顺序
- `Machine` 只管 truth
- `Materializer` 只管 view

---

## 10. 推荐的 proto / Go 演进顺序

### 第一步

继续在当前代码里把 topology truth 生产侧显式化：

- split committed
- merge committed
- peer change committed

### 第二步

把 `meta/root/types.go` 中的 event schema 和 root API 拆开

### 第三步

先在文档和代码里把 committed truth event 集合收死

### 第四步

再去定义真正的 Delos-lite replicated log 接口

这个顺序不能反，否则 schema 还不稳就会把 log 层做死。

---

## 11. 总结

NoKV Delos-lite 的第一版接口和 schema，不需要从零发明，而应该从当前：

- `meta/root/types.go`
- `pb/meta/root.proto`
- `meta/codec/root.go`

这三处往下收敛。

最重要的工作不是“造更多结构”，而是：

1. 把 event 变成显式 committed truth event
2. 把 rooted state 收敛成 compact truth state
3. 把 log/state/checkpoint/view 四层接口切开
4. 让 proto 只表达未来真正需要的 rooted truth schema

一句话：

> Delos-lite 第一版最值钱的不是协议创新，而是 schema 与接口边界收敛。
