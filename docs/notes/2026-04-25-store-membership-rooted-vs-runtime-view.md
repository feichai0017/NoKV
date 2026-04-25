# 2026-04-25 Store Membership：rooted truth 与 runtime view 的边界

## 导读

- 🧭 主题：把 Stage 1 的 coordinator store registry 推到 Stage 2 的 rooted membership 设计
- 🧱 核心对象：`StoreJoined` / `StoreRetired`、store heartbeat、`GetStore` / `ListStores`
- 🔁 调用链：`raftstore store -> coordinator heartbeat -> runtime view`，以及 `meta/root event -> coordinator bootstrap -> membership truth`
- 📚 参考对象：TiKV PD 的 store membership / heartbeat 分层；FoundationDB 的 coordinator bootstrap 思路；NoKV 现有 `meta/root` rooted truth 分层

## 1. 结论

Store 相关状态要拆成两类，不能混在一个 registry 里：

| 类别 | 例子 | 性质 | 归属 |
|---|---|---|---|
| **Membership truth** | `store_id=7` 是否属于集群、是否 retired | 持久、审计、必须经由 authority 决策 | `meta/root` rooted event |
| **Runtime view** | `store_id=7` 当前地址、最后心跳、容量、leader 数、是否超时 | 可重建、会抖动、由 heartbeat 驱动 | `coordinator` 内存 view |

Stage 1 已经把 client bootstrap 从静态 store list 收敛到 coordinator endpoint，这是对的。但 Stage 1 的 store registry 仍然由 heartbeat 隐式创建：一个 store 只要发心跳，coordinator 就会把它放进 runtime view。Stage 2 要把这个边界收紧：

> heartbeat 只能刷新已注册 store 的 runtime view，不能让一个未知 store 自动成为集群成员。

这条是 Stage 2.1 的第一刀。

## 2. 当前状态

Stage 2.1 已经把 rooted membership 从雏形推进到执行路径：

- `meta/root/event/types.go` 里有 `KindStoreJoined` / `KindStoreRetired`。
- `rootstate.Snapshot` 持有 `Stores map[uint64]StoreMembership`。
- `ApplyEventToSnapshot` 遇到 store join / retire 会推进 `MembershipEpoch` 并 materialize compact membership state。
- `meta/wire/root.go` 能编码 root event payload 和 checkpoint 里的 store membership compact state。
- `coordinator/rootview` bootstrap 会把 rooted store membership materialize 到 `coordinator/catalog.Cluster`。
- `coordinator/catalog.Cluster.UpsertStoreHeartbeat` 只接受 rooted active store；unknown / retired store 会被拒绝。
- `coordinator/server.GetStore/ListStores` 返回 membership + heartbeat 组合出来的 store state。
- `StoreState_TOMBSTONE` 由 rooted retired membership 驱动。

因此 Stage 2.1 的边界已经收紧：store registry 不再由 heartbeat 隐式创建；heartbeat 只是刷新已注册 store 的 runtime view。

## 3. 为什么不能把地址也写进 `meta/root`

地址和 membership 不是同一种事实。

`store_id=7` 属于集群，这是 membership truth。它需要持久化、审计和显式下线流程。

`store_id=7` 当前可拨地址是 `nokv-store-1:20160`，这是 runtime view。它可能因为 Docker、Kubernetes、host 网络、rolling restart、port remap 变化。把这个地址写进 `meta/root` 会造成两个问题：

1. 每次 runtime address 变化都会污染 rooted log。
2. coordinator restart 时会过度相信 last-known address，而不是等待 heartbeat 重新确认。

因此 Stage 2 的边界是：

```text
meta/root:
  store_id membership, lifecycle, tombstone

coordinator runtime view:
  client_addr, raft_addr, last_heartbeat, capacity, load, state derived from TTL
```

可以有一个本地 last-known endpoint cache，但它只能是加速启动的 cache，不能是 truth。

## 4. Rooted event 设计

Stage 2.1 使用两个 lifecycle event：

| Event | 语义 |
|---|---|
| `StoreJoined(store_id)` | 该 store ID 成为集群成员。 |
| `StoreRetired(store_id)` | 该 store ID 被显式下线，不再接受 heartbeat 注册。 |

rooted membership payload 必须保持精简：

- `StoreJoined(storeID)` 只声明成员资格。
- `StoreRetired(storeID)` 只声明成员生命周期结束。
- `address` 不进入 root event；client / raft 地址只来自 heartbeat runtime view。

目标 compact state：

```go
type StoreMembershipState uint8

const (
    StoreMembershipUnknown StoreMembershipState = iota
    StoreMembershipActive
    StoreMembershipRetired
)

type StoreMembership struct {
    StoreID uint64
    State   StoreMembershipState
    JoinedAt rootstate.Cursor
    RetiredAt rootstate.Cursor
}

type Snapshot struct {
    State  State
    Stores map[uint64]StoreMembership
    // existing descriptors / pending transitions...
}
```

`MembershipEpoch` 在 join / retire 时推进。heartbeat 不推进 `MembershipEpoch`。

## 5. Coordinator 行为

Coordinator 启动时应该从 rooted snapshot 恢复 membership，再等待 heartbeat 填 runtime fields。

启动后有三层状态：

| 状态 | 条件 | `GetStore` 行为 |
|---|---|---|
| `not_found` | store 不在 rooted membership 里 | `not_found=true` |
| `UNKNOWN` | rooted active，但还没有 heartbeat | 返回 store，state=`UNKNOWN`，client 不 dial |
| `UP` | rooted active + heartbeat 未过 TTL | 返回 `client_addr` |
| `DOWN` | rooted active + heartbeat 过 TTL | 返回 store，state=`DOWN`，client 不 dial |
| `TOMBSTONE` | rooted retired | 返回 store，state=`TOMBSTONE`，client 不 retry |

这个设计把 "never joined" 和 "known but currently down" 分开。Stage 1 只能靠 heartbeat view 推断，Stage 2 可以给出明确语义。

## 6. Heartbeat admission

当前：

```text
StoreHeartbeat(store_id=7, addr=...)
    -> Upsert runtime view
```

Stage 2：

```text
StoreHeartbeat(store_id=7, addr=...)
    -> check rooted membership
       - active: refresh runtime view
       - retired: reject FailedPrecondition
       - unknown: reject NotFound / FailedPrecondition
```

这会带来一个部署要求：store 第一次启动前必须先有 `StoreJoined` rooted event。

Docker Compose 和 dev cluster 可以在 bootstrap 阶段从 `raft_config.stores` 生成初始 `StoreJoined` events。生产环境应走显式 join 命令。

## 7. CLI / bootstrap 入口

Stage 2.1 需要两个入口：

| 命令 | 用途 |
|---|---|
| `nokv store join --store-id N` | 写入 `StoreJoined` rooted event。 |
| `nokv store retire --store-id N` | 写入 `StoreRetired` rooted event。 |

Bootstrap 期间可以批量 join：

```bash
nokv store bootstrap-membership --config raft_config.example.json
```

这条命令只写 membership，不写 runtime address。store address 仍然由 heartbeat 上报。

## 8. 与 TiKV PD / FDB 的关系

TiKV PD 的关键边界是：store 是否属于集群由 PD 持久化状态决定；heartbeat 刷新 liveness 和 stats。NoKV Stage 2 应该采用同一条边界，但 truth 后端不是 etcd，而是 `meta/root`。

FoundationDB 的 cluster file / coordinator 思路解决的是 client 找到控制面入口。NoKV Stage 1 已经做到 client 只需要 coordinator endpoint。Stage 2 继续补的是 control plane 自己重启后能从 rooted membership 恢复，而不是等 heartbeat 重新发明 membership。

## 9. 测试矩阵

Stage 2.1 必须覆盖：

| 测试 | 断言 |
|---|---|
| root state apply join / retire | `Stores` map 更新，`MembershipEpoch` 单调推进 |
| root wire roundtrip | `StoreJoined` / `StoreRetired` 不丢字段 |
| coordinator bootstrap from root | restart 后 active store membership 存在，runtime address 为空 |
| heartbeat active store | 刷新 runtime address，`GetStore` 返回 `UP` |
| heartbeat unknown store | 拒绝，runtime view 不隐式创建 |
| heartbeat retired store | 拒绝，`ListStores` 显示 `TOMBSTONE` |
| stale heartbeat | active store 从 `UP` 变 `DOWN`，client 返回 store unavailable |
| Docker restart | compose 重启后 coordinator 能在心跳前知道 membership，心跳后恢复 address |

集成测试应该放在 `coordinator/integration` 或 `raftstore/integration`，不要塞进 `fsmeta`。这是 control-plane membership，不是 fsmeta 业务逻辑。

## 10. 实施顺序

1. 改 `meta/root/event`：使用 `StoreJoined(storeID)` / `StoreRetired(storeID)` 精简 payload。
2. 改 `rootstate.Snapshot`：增加 `Stores map[uint64]StoreMembership`。
3. 改 materialization / wire / tests：join / retire 进入 compact snapshot。
4. 改 `coordinator/rootview`：bootstrap 时把 rooted membership 装入 `catalog.Cluster`。
5. 改 `coordinator/catalog`：分离 membership view 与 heartbeat view。
6. 改 `StoreHeartbeat` admission：unknown / retired store 不允许 upsert。
7. 改 `GetStore/ListStores`：输出 `UNKNOWN` / `UP` / `DOWN` / `TOMBSTONE`。
8. 改 Docker/dev bootstrap：初始 stores 写入 rooted membership。
9. 补 integration：coordinator restart + heartbeat admission + retired store。

每一步都应该保持无兼容层中间态。Stage 2.1 完成后，heartbeat 隐式注册 store 的路径应该彻底消失。

## 11. 不做什么

- 不把 `client_addr` / `raft_addr` 写进 rooted truth。
- 不让 `raft_config.stores` 在 runtime 阶段覆盖 coordinator store registry。
- 不给 unknown store 做自动 join。
- 不把 store membership 逻辑放到 fsmeta。
- 不在 Stage 2.1 做 scheduler rebalancing。membership truth 先闭合，调度后置。

## 12. 完成信号

Stage 2.1 完成时，系统应该满足：

1. coordinator 重启后，不需要等 heartbeat 才知道哪些 store 是集群成员。
2. 未 join 的 store 发 heartbeat 会被拒绝。
3. retired store 发 heartbeat 会被拒绝，并在 `ListStores` 中显示为 tombstone。
4. fsmeta / redis / raftstore client 仍然只依赖 coordinator endpoint。
5. `meta/root` 仍然只保存 membership truth，不保存 runtime endpoint。
