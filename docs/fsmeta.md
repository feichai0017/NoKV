# FSMetadata

## 导读

- 主题：NoKV 的 namespace metadata substrate。
- 核心对象：Mount、Inode、Dentry、SubtreeAuthority、SnapshotEpoch、QuotaFence、UsageCounter。
- 调用链：`fsmeta/client -> fsmeta/server -> fsmeta/exec -> TxnRunner -> raftstore/percolator/coordinator`。
- 代码合同：wire 在 `pb/fsmeta/fsmeta.proto`，执行器在 `fsmeta/exec`，默认 NoKV runtime 在 `fsmeta/exec.OpenWithRaftstore`。

## 1. 结论

`fsmeta` 是 NoKV 的原生元数据服务。它不是 FUSE frontend，不负责对象 body I/O，也不承诺完整 POSIX。它提供的是分布式文件系统、对象存储 namespace、AI dataset metadata 都能复用的元数据底座。

这层的价值不在于把 inode/dentry 编成几种 key。真正的边界是：常见 namespace 操作被做成服务端原语，而不是让上层应用自己用 `Get` / `Put` / `Scan` 拼协议。

## 2. 当前 API

当前 v1 API 由 `pb/fsmeta/fsmeta.proto` 定义，`fsmeta/server` 暴露 gRPC，`fsmeta/client` 提供 Go typed client。

| RPC | 当前语义 |
|---|---|
| `Create` | 原子创建一个 dentry 和 inode；服务端 `AssertionNotExist` 拒绝重复创建。 |
| `Lookup` | 按 `(mount, parent_inode, name)` 读取一个 dentry。 |
| `ReadDir` | 按 dentry prefix 扫一个目录页。 |
| `ReadDirPlus` | 在同一个 snapshot version 下扫 dentry 并批量读取 inode attr。 |
| `WatchSubtree` | prefix-scoped change feed；支持 ready、ack、back-pressure 和 cursor replay。 |
| `SnapshotSubtree` | 发布一个稳定 MVCC read version，后续 `ReadDir` / `ReadDirPlus` 可用它读取快照。 |
| `RetireSnapshotSubtree` | 主动 retire 一个 snapshot epoch。 |
| `GetQuotaUsage` | 读取 mount/scope 的持久化 quota usage counter。 |
| `RenameSubtree` | 原子移动一个 subtree root dentry；descendants 通过 inode 引用自然跟随。 |
| `Link` | 给已有非目录 inode 创建第二个 dentry，并在同一事务内增加 link count。 |
| `Unlink` | 删除一个 dentry；递减 link count，最后一个 link 被删时删除 inode record。 |

## 3. 数据模型

`fsmeta` 的 key schema 在 `fsmeta/keys.go`，value schema 在 `fsmeta/value.go`。

| 对象 | 存储位置 | 说明 |
|---|---|---|
| Mount metadata key | `EncodeMountKey` | 预留的 mount-level 数据 key；mount lifecycle truth 不存在这里。 |
| Inode | `EncodeInodeKey(mount, inode)` | 文件/目录属性，含 `size`、`mode`、`link_count`。 |
| Dentry | `EncodeDentryKey(mount, parent, name)` | parent/name 到 inode 的映射。 |
| Chunk | `EncodeChunkKey(mount, inode, chunk)` | schema 已有，当前 fsmeta API 不暴露对象 body/chunk I/O。 |
| Session | `EncodeSessionKey(mount, session)` | schema 预留给后续 session/lease。 |
| Usage | `EncodeUsageKey(mount, scope)` | quota usage counter；scope=0 表示 mount-wide，非 0 表示直接 accounting scope。 |

Key/value 都有 magic + schema version。value 是手写二进制布局，不走 JSON。

## 4. 执行边界

`fsmeta/exec.Executor` 只依赖一个窄接口：

```go
type TxnRunner interface {
    ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
    Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
    BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
    Scan(ctx context.Context, startKey []byte, limit uint32, version uint64) ([]KV, error)
    Mutate(ctx context.Context, primary []byte, mutations []*kvrpcpb.Mutation, startVersion, commitVersion, lockTTL uint64) error
}
```

默认 runtime 用 `OpenWithRaftstore` 把 coordinator、raftstore client、TSO、watch source、mount/quota cache、snapshot publisher、subtree handoff publisher 接起来。嵌入式用户可以直接用这个入口；测试和自定义部署可以继续传自己的 `TxnRunner`。

分层约束是：

- `Executor` 不直接知道 raft region / store routing。
- `OpenWithRaftstore` 是 NoKV 默认适配器，负责 raftstore wiring。
- `meta/root` 不保存 inode/dentry 高频数据，只保存 lifecycle / authority truth。
- `raftstore` 和 `percolator` 不理解 fsmeta 语义，只提供事务和 apply observation。

## 5. 原生 primitive

### ReadDirPlus

`ReadDirPlus` 是当前最直接的 shape advantage：一次 dentry scan，加一次 `BatchGet` inode attrs，读同一个 snapshot version。generic-KV baseline 需要 scan 后对每个 dentry 做点查，形成 N+1。

严格语义：任意 inode 缺失或解码失败，整页返回错误。fsmeta 不返回半真半假的目录页。

### WatchSubtree

`WatchSubtree` 订阅 fsmeta key prefix，对外暴露的是 `(region_id, term, index)` cursor 和 `commit_version`。事件来源包括：

- `CMD_COMMIT` 成功；
- `CMD_RESOLVE_LOCK` 且 `commit_version != 0`。

`CMD_PREWRITE`、rollback、diagnostic commands 不产生可见事件。

v1 已支持：

- ready signal；
- back-pressure window；
- ack；
- per-region recent ring；
- resume cursor replay；
- cursor 过期时返回 `ErrWatchCursorExpired`。

### SnapshotSubtree

`SnapshotSubtree` 只发布 read epoch，不复制目录树。token 形状是 `(mount, root_inode, read_version)`。后续 `ReadDir` / `ReadDirPlus` 使用 `snapshot_version` 读取同一个 MVCC 视图。

当前已经有 `SnapshotEpochPublished` / `SnapshotEpochRetired` rooted events。数据面 MVCC GC 还没有把这些 epoch 作为 retention lower bound 使用，这是后续 GC 层的工作。

### RenameSubtree

当前 dentry schema 用 `parent_inode_id` 引用父目录，所以 subtree rename 的物理写入量和普通 rename 一样：删除旧 root dentry，写入新 root dentry。descendants 不需要逐条改 key。

额外语义在 authority 层：

- mutation 前发布 `SubtreeHandoffStarted`；
- dentry mutation 走 Percolator 2PC；
- mutation 后发布 `SubtreeHandoffCompleted`；
- runtime monitor 通过 `WatchRootEvents` 发现 pending handoff 并补 complete。

这个设计优先保证 rooted authority 不会永久卡在未知状态。极端情况下可能推进一个空 era，但不会留下无人修复的 pending handoff。

### Link / Unlink

`Link` 只允许非目录 inode。它会创建新 dentry，并在同一事务内增加 `InodeRecord.LinkCount`。

`Unlink` 删除一个 dentry，并根据 link count 更新 inode：

- link count 大于 1：递减并写回 inode；
- link count 小于等于 1：删除 inode record。

目录 hard link 仍然非法。

### Quota Fence

Quota fence 是 rooted truth；usage counter 是 data-plane key。写路径会把 usage counter mutation 和 dentry/inode mutation 放进同一个 Percolator transaction。

这解决两个问题：

- 多个 `nokv-fsmeta` gateway 不会各自维护本地计数导致突破 limit；
- gateway 重启不会丢 usage。

fence 变化通过 coordinator root-event stream 推给 fsmeta runtime，cache miss 时也会回源查询 coordinator。

## 6. Rooted Truth vs Runtime View

| Domain | Rooted truth | Runtime view |
|---|---|---|
| Mount | `MountRegistered` / `MountRetired` | fsmeta mount admission cache，retired mount 会关闭相关 watch subscription。 |
| Subtree authority | `SubtreeAuthorityDeclared` / `SubtreeHandoffStarted` / `SubtreeHandoffCompleted` | RenameSubtree frontier、pending handoff repair。 |
| Snapshot epoch | `SnapshotEpochPublished` / `SnapshotEpochRetired` | snapshot-version reads。 |
| Quota fence | `QuotaFenceUpdated` | quota fence cache + persisted usage counter keys。 |
| WatchSubtree | 不进 `meta/root` | raftstore apply observer + fsmeta router。 |

`nokv-fsmeta` 启动时先拉一次 `ListMounts` / `ListQuotaFences` / `ListSubtreeAuthorities` 做 bootstrap，然后通过 `WatchRootEvents` 跟随后续变化。`MonitorInterval` 是 root-event stream 断开后的重连 backoff；它不是 steady-state polling interval。

## 7. 部署

Docker Compose 会启动 meta-root、coordinator、raftstore、fsmeta gateway、Redis gateway，并通过 `mount-init` 注册默认 mount：

```bash
docker compose up -d
```

直接启动 fsmeta gateway：

```bash
go run ./cmd/nokv-fsmeta \
  --addr 127.0.0.1:8090 \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --metrics-addr 127.0.0.1:9400
```

注册 mount：

```bash
nokv mount register \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --mount default \
  --root-inode 1 \
  --schema-version 1
```

设置 quota：

```bash
nokv quota set \
  --coordinator-addr 127.0.0.1:2390,127.0.0.1:2391,127.0.0.1:2392 \
  --mount default \
  --limit-bytes 10737418240 \
  --limit-inodes 10000000
```

## 8. Metrics

`nokv-fsmeta --metrics-addr` 暴露四组 expvar：

| Namespace | 含义 |
|---|---|
| `nokv_fsmeta_executor` | transaction retry / retry exhausted。 |
| `nokv_fsmeta_watch` | subscribers、events、delivered、dropped、overflow、remote source state。 |
| `nokv_fsmeta_mount` | mount cache hit/miss、admission rejects。 |
| `nokv_fsmeta_quota` | fence check/reject、cache hit/miss、fence updates、usage mutations。 |

## 9. Benchmarks

fsmeta benchmark 在 `benchmark/fsmeta`。核心对照是同一个 NoKV 集群上的两条路径：

| Driver | 行为 |
|---|---|
| `native-fsmeta` | 调 fsmeta typed API。 |
| `generic-kv` | 用同样的 raftstore/percolator substrate，在客户端拼 metadata schema。 |

Stage 1 headline：`ReadDirPlus` 平均延迟 12.0 ms vs 510.3 ms，约 42.5x。结果 CSV 在 `benchmark/fsmeta/results/`。

WatchSubtree evidence workload 也在同一 benchmark 包里，`watch_notify` 在 Docker Compose 3-node 集群下达到 sub-second p95。

## 10. 非目标

- 不提供 FUSE / NFS / SMB frontend。
- 不提供 S3 HTTP gateway 或 object body I/O。
- 不把每个 inode/dentry mutation 写进 `meta/root`。
- 不做 recursive materialized snapshot；`SnapshotSubtree` 是 MVCC read epoch。
- 不承诺 data-plane MVCC GC 已经按 snapshot epoch 做 retention。
- 不把 `nokv-redis` 接到 fsmeta；Redis gateway 是底层 KV 的第二产品面。
