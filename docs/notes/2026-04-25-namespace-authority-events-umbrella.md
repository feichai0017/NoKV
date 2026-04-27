# 2026-04-25 Namespace Authority Events Umbrella

## 导读

- 🧭 主题：统一 namespace / subtree / snapshot / quota primitive 的 rooted event 边界
- 🧱 核心对象：Mount、Subtree、SnapshotEpoch、QuotaFence、StoreMembership
- 🔁 调用链：`fsmeta/server -> coordinator/meta-root command -> rooted event -> coordinator/runtime view -> raftstore/fsmeta primitive`
- 📚 参考对象：NoKV Eunomia、`meta/root` rooted truth、DaisyNFS / FSCQ 的 verified metadata 边界、TiKV PD 的 membership / runtime view 分层

## 1. 结论

namespace primitive 不应该各自发明 RootEvent。当前代码已经按这份 umbrella 落了主要 domain：

| Domain | Rooted truth | Runtime view | Stage |
|---|---|---|---|
| Store membership | store 是否属于集群、是否 retired | address、heartbeat、capacity、load | done |
| Mount lifecycle | mount 是否存在、root inode、schema version | mount admission cache、watch subscription close | done |
| Subtree authority | 某个 subtree 的 authority era / handoff frontier | watcher fan-out、pending handoff repair | done |
| Snapshot epoch | snapshot ID、read timestamp、covered subtree | snapshot-version reads | done |
| Quota fence | quota limit、fence era、frontier | quota fence cache + data-plane usage counter | done |

原则：

> `meta/root` 只保存必须持久、可审计、影响 authority legality 的事实。地址、负载、cache、watcher 和高频 usage counter 不进 root；usage counter 是 data-plane key，并入 fsmeta metadata transaction。

## 2. 为什么要先做 umbrella

如果不先统一 schema，后面会出现五种互相不兼容的 event 风格：

- store membership 一套；
- mount registry 一套；
- snapshot 一套；
- quota 一套；
- subtree handoff 又一套。

结果是 `meta/root/state` 变成事件堆场，`coordinator/rootview` 每个 primitive 写一份 materialization，测试矩阵也会碎掉。

这份 umbrella 的目的不是一次实现所有 event，而是先固定三件事：

1. 哪些事实可以进入 rooted truth；
2. event 命名和 payload 风格；
3. compact state 怎么按 domain 分层。

## 3. Event 命名规则

用 lifecycle 动词，不用模糊状态词。

| 推荐 | 避免 | 理由 |
|---|---|---|
| `StoreJoined` / `StoreRetired` | 暂时离线类命名 | `retired` 明确表示 membership 终态 |
| `MountRegistered` / `MountRetired` | `MountAdded` / `MountDeleted` | mount 是 authority object，不只是 map entry |
| `SnapshotEpochPublished` / `SnapshotEpochRetired` | `SnapshotCreated` | snapshot 关键是 read epoch，不是文件对象 |
| `QuotaFenceUpdated` | `QuotaChanged` | fence 表示 authority boundary |
| `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | `SubtreeMoved` | handoff 是协议，不是单次赋值 |

所有 event 都应满足：

- payload 只包含 truth 字段；
- 不包含 runtime address / load / cache；
- 有明确 idempotent materialization 规则；
- 能从 compact state 反推出当前 truth；
- 能被 audit 工具独立解释。

## 4. Compact state 分层

`rootstate.Snapshot` 不应该只有 flat fields。当前方向是按 domain 分层：

```go
type Snapshot struct {
    State State

    Stores   map[uint64]StoreMembership
    Mounts   map[string]MountRecord
    Subtrees map[SubtreeID]SubtreeAuthority
    Snapshots map[SnapshotID]SnapshotEpoch
    Quotas   map[QuotaSubject]QuotaFence

    Descriptors map[uint64]descriptor.Descriptor
    PendingPeerChanges map[uint64]PendingPeerChange
    PendingRangeChanges map[uint64]PendingRangeChange
}
```

这不是要求一次改完。当前已按这个方向加入 `Stores`、`Mounts`、`Subtrees`、`SnapshotEpochs`、`Quotas` 等 compact state；后续新增 domain 也应继续沿用这个分层，不要把所有东西塞进 `State`。

## 5. Store membership events

Store membership 的最小形状：

```go
type StoreMembership struct {
    StoreID uint64
    State   StoreMembershipState
    JoinedAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}
```

Events：

```text
StoreJoined(store_id)
StoreRetired(store_id)
```

不进 rooted truth 的字段：

- `client_addr`
- `raft_addr`
- `last_heartbeat`
- `capacity`
- `available`
- `leader_count`

这些字段由 store heartbeat 刷新到 coordinator runtime view。

## 6. Mount lifecycle events

Mount 是 fsmeta 的 namespace 根。fsmeta 不需要完整 POSIX mount，但需要一个 durable mount registry，避免每个 caller 自己约定 mount string。

当前代码已实现这条 registry：mount membership 进入 `meta/root` rooted truth，`nokv-fsmeta` 的写路径通过 coordinator mount view 做 admission，未注册或 retired mount 会被拒绝。

`docs/spec/MountLifecycle.tla` 覆盖这条 lifecycle：

- `MountRegistered` 只能把未出现过的 mount 变成 active；
- `MountRetired` 是终态，retired mount 不能再次 active；
- mount 的 root inode 和 schema version 属于 rooted identity，不是 runtime cache。

建议 events：

```text
MountRegistered(mount_id, root_inode, schema_version)
MountRetired(mount_id, retired_at)
```

Compact record：

```go
type MountRecord struct {
    MountID string
    RootInode uint64
    SchemaVersion uint32
    RegisteredAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}
```

进入 rooted truth：

- mount 是否存在；
- root inode；
- fsmeta schema version；
- retired 状态。

不进入 rooted truth：

- active client sessions；
- local mount cache；
- mount endpoint / frontend address。

## 7. Subtree authority events

`WatchSubtree` 本身不需要把每个 watch subscription 写进 root。subscription 是 runtime view。

但 subtree 的 authority boundary 需要 rooted event，否则后续 `RenameSubtree` / `SnapshotSubtree` / `QuotaFence` 会各自发明边界。

当前 event 形状：

```text
SubtreeAuthorityDeclared(mount_id, subtree_root, authority_id, era)
SubtreeHandoffStarted(mount_id, subtree_root, from_authority, to_authority, legacy_frontier)
SubtreeHandoffCompleted(mount_id, subtree_root, authority_id, era, inherited_frontier)
```

这套命名直接对应 Eunomia：

- `authority_id + era` 类似 Tenure；
- `legacy_frontier` 类似 Legacy；
- `handoff completed` 对应 Finality。

`WatchSubtree` 使用 subtree prefix 过滤 data-plane apply events，不把每个 watch 事件写进 root。`RenameSubtree` 使用这组 rooted handoff event 推进 subtree authority frontier。

`docs/spec/SubtreeAuthority.tla` 建模这组 handoff 语义。这个 spec 不建模 dentry 写入，只建模 authority 记录本身：

- `Primacy`：每个 subtree 至多一个 active authority；
- `Inheritance`：successor frontier 必须覆盖 predecessor frontier；
- `Silence`：sealed authority 的 reply 不再 admissible；
- `Finality`：sealed predecessor 必须处于 pending handoff 或 closed。

当前 `RenameSubtree` 按这个 spec 方向实现 rooted handoff event，而不是另起一套 rename-local 状态机。

## 8. Snapshot epoch events

`SnapshotSubtree` 的核心不是复制一份数据，而是发布一个 stable read epoch。

建议 events：

```text
SnapshotEpochPublished(snapshot_id, mount_id, subtree_root, read_ts, frontier)
SnapshotEpochRetired(snapshot_id)
```

Compact record：

```go
type SnapshotEpoch struct {
    SnapshotID string
    MountID string
    SubtreeRoot uint64
    ReadTS uint64
    Frontier uint64
    PublishedAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}
```

`read_ts` 来自 coordinator TSO。`frontier` 表示 snapshot 覆盖的 metadata frontier。真正的数据读取仍然走 Percolator MVCC，不把文件列表写进 root。

## 9. Quota fence events

Quota fence 已实现。event 是：

```text
QuotaFenceUpdated(subject, limit_bytes, limit_inodes, era, frontier)
```

当前语义：

- rooted truth 保存 quota limit 和 fence era；
- data plane 保存 usage counter key；
- 写路径把 usage counter mutation 和 dentry/inode mutation 放进同一个 Percolator transaction；
- gateway 重启不丢 usage，多 gateway 通过同一个 usage key 上的 Percolator conflict 串行化。

不要把每次 usage 增减写进 root。那会把高频数据面计数污染 authority truth。

## 10. WatchSubtree 与 rooted event 的边界

`WatchSubtree` 是 fsmeta 的 headline primitive，但它自己的 event stream 不等于 RootEvent。

两条流要分开：

| 流 | 来源 | 内容 | 持久性 |
|---|---|---|---|
| RootEvent | `meta/root` | authority / lifecycle truth | 持久、审计 |
| WatchEvent | raftstore apply hook | file/dentry mutation notification | 可恢复 cursor，但不进 root |

`WatchSubtree` 可以借鉴 `meta/root` 的 TailSubscription 模式，但不能把每个 file mutation 塞进 `meta/root`。

## 11. 实施状态

1. `StoreJoined` / `StoreRetired`：最小 rooted membership，已实现。
2. `MountRegistered` / `MountRetired`：fsmeta namespace registry，已实现。
3. `WatchSubtree`：runtime watch stream，不新增 high-frequency root events，已实现 ready / ack / replay。
4. `SnapshotEpochPublished` / `SnapshotEpochRetired`：MVCC snapshot epoch，已实现。
5. `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted`：RenameSubtree authority frontier，已实现。
6. `QuotaFenceUpdated`：rooted fence + data-plane usage counter，已实现。

这个顺序的好处已经体现在代码里：先验证 root schema 扩展，再做 runtime watch，再做 read-only snapshot，最后接入 handoff 和 quota。

## 12. 测试规则

每新增一种 RootEvent 必须有四类测试：

| 测试 | 位置 | 目的 |
|---|---|---|
| event constructor / clone | `meta/root/event` | payload 不 alias |
| wire roundtrip | `meta/wire` | proto 编码不丢字段 |
| state materialization | `meta/root/state` | compact state 正确 |
| coordinator bootstrap | `coordinator/integration` | runtime view 从 rooted snapshot 恢复 |

如果 event 会影响 data-plane admission，还要加 `raftstore/integration` 测试。

## 13. 不做什么

- 不把 WatchEvent 当 RootEvent。
- 不把 runtime address、watcher、session、cache、load 写进 root。
- 不把 mount registry 放进 fsmeta 本地内存当 truth。
- 不让 snapshot 记录具体 dentry 列表。
- 不把 `RenameSubtree` 的每个 dentry mutation 展开成 rooted event；root 只记录 authority handoff frontier。

## 14. 完成信号

这份 umbrella 完成后，任何新增 namespace primitive 都必须先回答：

1. 它有没有 rooted truth？
2. 如果有，event 名是什么，payload 是否只包含 truth？
3. 它对应的 runtime view 是什么？
4. 它和 Eunomia / authority handoff 有没有关系？
5. 它的四类测试在哪里？

答不上来，就不写代码。
