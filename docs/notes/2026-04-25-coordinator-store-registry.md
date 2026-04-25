# 2026-04-25 Coordinator 驱动的 Store Registry

> 状态：已落地。NoKV 客户端 bootstrap 已从“静态 `raft_config` 携带全部 store 地址”收敛到“只依赖 coordinator endpoint”。

## 1. 结论

NoKV 现在参考 TiKV PD 和 FoundationDB 的 bootstrap 思路，把 store 地址发现交给 `coordinator`。

`coordinator` 同时提供三类运行时入口：

- `GetRegionByKey(key)`：返回 region descriptor 和 peer `store_id`。
- `GetStore(store_id)`：返回该 store 当前 client gRPC 地址。
- `Tso` / `AllocID`：提供事务时间戳和 ID 分配。

数据面请求仍然直连 raftstore。coordinator 只负责 discovery / route / TSO，不代理 KV 读写。

```text
client / fsmeta / redis
    |
    | only coordinator endpoint
    v
coordinator
    | GetRegionByKey(key) -> region descriptor
    | GetStore(store_id)  -> current store address
    v
raftstore store
```

## 2. 参考边界

TiKV 把 store liveness、store state、region state 上报给 PD，PD 根据 store heartbeat 和 region heartbeat 做调度与路由决策。参考：<https://tikv.org/docs/dev/reference/architecture/scheduling/>

FoundationDB 的客户端和服务端从 cluster file 找到 coordinators，再进入实际集群。参考：<https://apple.github.io/foundationdb/architecture.html>

NoKV 的实现更接近 TiKV PD：coordinator 维护 runtime store registry，raftstore 继续执行读写。

## 3. Wire Contract

Store heartbeat 上报 data-plane 地址：

```proto
message StoreHeartbeatRequest {
  uint64 store_id = 1;
  uint64 region_num = 2;
  uint64 leader_num = 3;
  uint64 capacity = 4;
  uint64 available = 5;
  uint64 dropped_operations = 6;
  repeated uint64 leader_region_ids = 7;
  string client_addr = 8;
  string raft_addr = 9;
}
```

Coordinator 暴露 store discovery：

```proto
enum StoreState {
  STORE_STATE_UNKNOWN = 0;
  STORE_STATE_UP = 1;
  STORE_STATE_DOWN = 2;
}

message StoreInfo {
  uint64 store_id = 1;
  string client_addr = 2;
  string raft_addr = 3;
  StoreState state = 4;
  uint64 region_num = 5;
  uint64 leader_num = 6;
  uint64 capacity = 7;
  uint64 available = 8;
  uint64 dropped_operations = 9;
  uint64 last_heartbeat_unix_nano = 10;
}

message GetStoreRequest {
  uint64 store_id = 1;
}

message GetStoreResponse {
  StoreInfo store = 1;
  bool not_found = 2;
}

message ListStoresRequest {}

message ListStoresResponse {
  repeated StoreInfo stores = 1;
}
```

`client_addr` 是 KV RPC dial 地址。`raft_addr` 目前通常等于 `client_addr`，保留给 transport-aware scheduling 和诊断。

`STORE_STATE_DOWN` 不是占位：coordinator 会根据最后一次 heartbeat 时间和 `storeHeartbeatTTL` 判断 store 是否过期。过期 store 仍然出现在 `ListStores` 中用于诊断，但 client 看到 `DOWN` 后不会继续 dial 该地址。

## 4. Coordinator 内部职责

`coordinator/view.StoreStats` 保存 runtime address 和 heartbeat stats：

```go
type StoreStats struct {
    StoreID           uint64
    ClientAddr        string
    RaftAddr          string
    RegionNum         uint64
    LeaderNum         uint64
    Capacity          uint64
    Available         uint64
    DroppedOperations uint64
    UpdatedAt         time.Time
}
```

`coordinator/catalog.Cluster` 提供：

- `StoreByID(storeID)`：单 store lookup。
- `StoreSnapshot()`：diagnostics / `ListStores`。

这仍然是 rebuildable runtime view。coordinator 重启后，store 下一轮 heartbeat 会恢复 registry。registry 空窗口期应该返回 `NotFound` / `Unavailable`，不从旧配置绕过控制面。

## 5. Client 侧职责

`raftstore/client.Config` 现在要求两个 resolver：

```go
type RegionResolver interface {
    GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error)
    Close() error
}

type StoreResolver interface {
    GetStore(ctx context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error)
}

type Config struct {
    RegionResolver RegionResolver
    StoreResolver  StoreResolver
}
```

正常部署中，两者由同一个 `coordinator/client.GRPCClient` 提供。

执行流程：

1. `GetRegionByKey` 得到 region descriptor。
2. client 选择 leader store。
3. store cache miss 时调用 `GetStore(store_id)`。
4. 如果 `StoreInfo.state == DOWN`，直接按 store unavailable 处理。
5. dial `StoreInfo.client_addr` 并缓存连接。
6. 连接失败时失效 store cache，重新向 coordinator resolve。

缓存命中不是永久信任。`raftstore/client` 默认每 30 秒 revalidate 一次 cached store endpoint：

- revalidation 看到 `DOWN`：失效本地 store cache，返回 `ErrStoreUnavailable`。
- revalidation 看到同地址 `UP`：刷新 validated timestamp，不重连。
- revalidation 看到新地址 `UP`：替换本地 store connection。
- revalidation 期间 coordinator 短暂不可达：继续使用已有连接，避免控制面抖动放大成数据面中断。

生产 client 不再接受静态 `Stores []StoreEndpoint`。测试里可以注入静态 resolver，但那只是 fake coordinator，不是运行时 fallback。

## 6. Gateway / Service 启动方式

`cmd/nokv-fsmeta`：

```bash
nokv-fsmeta \
  -coordinator-addr 127.0.0.1:2379 \
  -addr 127.0.0.1:8090
```

`cmd/nokv-redis` 分布式模式：

```bash
nokv-redis \
  -coordinator-addr 127.0.0.1:2379 \
  -addr 127.0.0.1:6380
```

`cmd/nokv-redis --raft-config` 只保留为开发便利入口：从 config 解析 coordinator addr，而不是解析 stores。`cmd/nokv-fsmeta` 不支持 `--raft-config`，避免把新服务继续绑定到静态 topology 文件。

## 7. 设计边界

这条改动不会削弱 `meta/root` 和 `coordinator` 的分离：

- durable truth：仍然在 `meta/root`。
- region descriptor truth：仍然通过 rooted events 发布。
- store address / liveness：runtime view，放 coordinator。
- data-plane execution：仍然在 raftstore。
- client bootstrap：只依赖 coordinator endpoint。

coordinator 不是 data-plane proxy，也不是重新变成“大脑数据库”。它只是补齐 PD-style runtime discovery 职责。

## 8. 验收

当前验收点：

- `cmd/nokv-fsmeta` 不需要 `--raft-config`。
- `cmd/nokv-redis` 可以只用 `--coordinator-addr` 进入 raft 模式。
- `raftstore/client` 在 store cache miss 时自动 `GetStore`。
- coordinator 会把超过 heartbeat TTL 的 store 标记为 `DOWN`，client 看到 `DOWN` 会拒绝 dial。
- `fsmeta/integration` 使用 coordinator-only discovery 跑完整 gRPC -> fsmeta server -> executor -> raftstore 链路。
- `raftstore/integration` 覆盖 coordinator unavailable 后的已有连接行为和 cold client 失败路径。
