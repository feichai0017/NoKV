# 2026-04-25 WatchSubtree —— rooted changefeed for filesystem & object namespace

> 状态：Stage 2.2 design note v0。`fsmeta/` Stage 1 (native API) + Stage 2.1
> (rooted store membership) 已闭合。本文是 Stage 2.2 第一份 design note，
> 落 `WatchSubtree` 的合同、数据流、与失败语义。**遵守 §10.4 design-note-first
> 纪律：本文未 merge 前不写实现代码。**

## 导读

- 🧭 主题：让 `fsmeta` 提供 prefix-scoped change feed —— 一个订阅者声明一个 subtree（`/checkpoints` / bucket prefix `s3://my-bucket/v1/`），系统将该 prefix 之下的 dentry / object 变更流式推送给它。
- 🧱 核心对象：raftstore apply-loop post-apply hook、prefix dispatcher、`fsmeta.WatchEvent`、resume cursor、back-pressure 协议。
- 🔁 调用链：客户端 `WatchSubtree(mount, root, after_cursor)` → `fsmeta/server` → `fsmeta/exec.WatchRouter` → `raftstore` apply hook → prefix filter → fan-out → gRPC stream。
- 📚 锚点：
  - 工业 — etcd watch（key 粒度，不够）；TiKV CDC（region 粒度，错位）；CockroachDB rangefeed（rangefeed 粒度，应用要拼）；S3 event notifications（最终一致，弱）。
  - 学术 — `meta/root` `TailSubscription` （NoKV 已有的内部成功模式，可借语义形式）。
  - **没有任何系统在 KV 层提供 prefix-scoped subtree change feed**——这是 Stage 2 的 headline novel primitive（v3 positioning §10.4 已标）。

## 1. 结论

`WatchSubtree` 是一个 **服务端原生** 的 prefix-scoped change feed：

> 客户端订阅一个 subtree（`mount + root_inode` 在 FS 视角，或 `mount + key prefix` 在对象存储视角），服务端在每次 raft apply 提交后，把命中该 prefix 的 mutation 推到订阅者，**无需客户端重复 readdir 拼接**，且**通知顺序与 raft commit 顺序一致**。

为什么是 NoKV 独有：
- 通用 KV 的 watch 只能按 key 或按 region；都不感知 namespace 层级。
- 上层应用（DFS / 对象存储 gateway）拼前缀过滤的代价是：要么拉更多 region 的 cdc 并自己过滤，要么在客户端轮询 readdir。**前者吞吐浪费、后者延迟差**。
- NoKV 的 fsmeta key schema 已经把目录前缀作为 first-class（`D{mount}{parent}{name}`），server-side 直接做前缀过滤是结构性免费。

## 2. 双消费者视角

| 维度 | 文件系统 | 对象存储 |
|---|---|---|
| 订阅 root | `mount + root_inode`（subtree 根） | `mount + key prefix`（bucket + prefix） |
| 事件粒度 | dentry create / unlink / rename，inode attr 变更 | object create / delete / metadata 更新 |
| 典型用法 | inotify / FUSE invalidate；ETL pipeline trigger | bucket prefix 变更 → 下游 ETL；S3 event notifications 强一致版 |
| 顺序保证 | 单 root 内全局 commit-ts 顺序 | 同 |
| novel 价值 | inotify 不可分布式；CephFS notify 不稳 | **S3 event 是最终一致的，没有 strong-consistency 替代方案** |

**核心纪律**：proto 不为 FS 或 OS 特化字段；`WatchEvent` 用 `mount + key prefix + change_kind + record_payload` 通用形态。FS frontend 把 `key prefix` 解释为 dentry prefix，OS gateway 解释为 object key prefix。

## 3. Wire contract（v0）

新增到 `pb/fsmeta/fsmeta.proto`（v1 service 直接扩展）：

```proto
message WatchSubtreeRequest {
  string mount = 1;
  // root_inode + descend 描述 FS subtree；
  // key_prefix 直接给 bytes prefix（OS gateway / 不依赖 inode 树的客户端用）。
  // 二选一：root_inode != 0 表示按 subtree 订阅；key_prefix 非空表示按
  // bytes prefix 订阅。同时给视为 InvalidArgument。
  uint64 root_inode = 2;
  bytes key_prefix = 3;
  // descend_recursively: 是否包含子目录（FS subtree 通常 true，OS prefix 总是 true）。
  bool descend_recursively = 4;
  // resume_cursor 为 0 表示从当前 apply cursor 之后开始；非零表示从该 cursor 之后续传。
  WatchCursor resume_cursor = 5;
  // back_pressure_window 客户端能接受的最大 outstanding 事件数；服务端达到此
  // 数量未 ack 时停止推送，等 client ack 后续传。0 表示 server default（256）。
  uint32 back_pressure_window = 6;
}

message WatchCursor {
  // raft cursor is scoped to one region. Two regions can have the same
  // (term,index), so region_id is part of the cursor identity.
  uint64 region_id = 1;
  uint64 term = 2;
  uint64 index = 3;
}

enum WatchEventSource {
  WATCH_EVENT_SOURCE_UNSPECIFIED = 0;
  // Emitted from CMD_COMMIT after CommitResponse.error == nil.
  WATCH_EVENT_SOURCE_COMMIT = 1;
  // Emitted from CMD_RESOLVE_LOCK after commit_version != 0 and
  // ResolveLockResponse.error == nil.
  WATCH_EVENT_SOURCE_RESOLVE_LOCK = 2;
}

message WatchEvent {
  WatchCursor raft_cursor = 1;    // per-region monotone resume cursor
  uint64 commit_version = 2;      // MVCC commit ts; not globally monotone
  WatchEventSource source = 3;
  // key 是 fsmeta key（D{...} / I{...}）的原始 bytes；客户端解码自行拿到
  // mount + parent + name 等结构化字段。
  bytes key = 4;
  // v0 不携带 value / prev_value。订阅者需要 value 时，按 commit_version
  // 做 snapshot read；dispatcher 可以在 fsmeta 层做 batch materialization。
}

message WatchSubtreeResponse {
  oneof payload {
    WatchEvent event = 1;
    // catchup_complete 表示 server 已经把订阅 catch-up 到流式实时位置，
    // 后面的事件全部是 live。客户端可以借此切换到"实时模式"。
    WatchCatchupComplete catchup_complete = 2;
    // server-side 节流信号：客户端继续慢，server 会发 throttle，再不 ack
    // 就关流。
    WatchThrottle throttle = 3;
  }
}

message WatchCatchupComplete {
  WatchCursor cursor = 1;
}

message WatchThrottle {
  uint32 outstanding = 1;
  uint32 limit = 2;
  string reason = 3;
}

// AckCursor 通过 stream 的请求方向发送。client → server：客户端已经处理到
// 这个 cursor，server 可以释放对应的 back-pressure 计数。
message WatchAck {
  WatchCursor cursor = 1;
}

service FSMetadata {
  // ... 已有 RPC ...
  // WatchSubtree 是 bidirectional stream：服务端推 WatchSubtreeResponse，
  // 客户端通过 stream 反向发 WatchAck。
  rpc WatchSubtree(stream WatchAckOrSubscribe) returns (stream WatchSubtreeResponse);
}

message WatchAckOrSubscribe {
  oneof body {
    WatchSubtreeRequest subscribe = 1;
    WatchAck ack = 2;
  }
}
```

**关键决策**：
- 用 **bidirectional stream**，不用 server-stream + side-channel ack RPC——避免 ack 与事件流之间的乱序窗口。
- `raft_cursor` 由服务端的 raft apply `(region_id, term, index)` 给出。它是 resume 主键，按 region 单调。
- `commit_version` 是 Percolator MVCC commit ts。它用于 snapshot read 物化 value，但跨事件不保证单调，不能替代 `raft_cursor`。
- v0 不携带 `value / prev_value`，也不直接给 `CREATE / DELETE / UPDATE` 分类。没有读取 value，就不能可靠区分 create/delete/update。将来如果要 with-value watch，作为订阅 flag 单独加路径。
- v0 不内置 server-side filter beyond prefix；将来要加 `WatchEventFilter`（按事件源 / 按 key family）作为附加 oneof 字段，proto 演进兼容。

## 4. 数据流

```mermaid
flowchart TB
  client["fsmeta gRPC client (subscribe + ack)"]
  service["fsmeta/server: WatchSubtree handler"]
  router["fsmeta/exec: WatchRouter (per-cluster singleton)"]
  hook["raftstore/store: command post-apply observer"]
  apply["raftstore command pipeline: Commit / ResolveLock / ..."]

  apply -->|visible commit command| hook
  hook -->|emit (raft_cursor, commit_version, key, source)| router
  router -->|prefix filter + per-subscriber buffer| service
  service -->|stream events| client
  client -->|ack(cursor)| service
  service -->|release back-pressure budget| router
```

### 4.1 raftstore post-apply hook

raftstore 当前的 peer apply loop 先在 `raftstore/peer/peer.go` 收到 raft entries，
再把 normal command entries 交给 `raftstore/store/command_pipeline.go` 解码并调用
KV applier。Stage 2.2 的 hook 点放在 **store command pipeline**，不是 peer raw
entry 层。理由：

- peer 层只看到 raft entry bytes；observer 要重新解码 `RaftCmdRequest`，重复 pipeline 已完成的工作。
- command pipeline 同时有 decoded `RaftCmdRequest` 和 raft `entry.Term/Index`，能构造准确 cursor。
- 这仍然是 store-level observer：一个 store observer 接收该 store 上所有 region 的 apply 事件，dispatcher 自己做 prefix/region filter。

```go
// raftstore/store/observer.go (new)
type ApplyObserver interface {
    OnApply(evt ApplyEvent)
}

type ApplyEvent struct {
    RegionID  uint64
    Term      uint64
    Index     uint64
    Source    ApplyEventSource
    CommitVersion uint64
    Keys      [][]byte
}

type ApplyEventSource uint8

const (
    ApplyEventSourceCommit ApplyEventSource = iota + 1
    ApplyEventSourceResolveLock
)
```

observer 事件只表示 "这些 key 在 `commit_version` 上变成可见状态"。它不读取
default CF，不携带 value，也不在 apply 热路径推断 create/delete/update。

#### Apply event sources

| Apply command | v0 行为 | 理由 |
|---|---|---|
| `CMD_COMMIT` 且 `CommitResponse.Error == nil` | emit | 显式提交，把 lock 升级为 write record |
| `CMD_RESOLVE_LOCK` 且 `commit_version != 0` 且 `ResolveLockResponse.Error == nil` | emit | primary 已提交后，secondary 可能由 resolve-lock 路径最终提交；不观察会漏事件 |
| `CMD_PREWRITE` | skip | prewrite 不是可见提交 |
| `CMD_BATCH_ROLLBACK` | skip | 回滚不可见 |
| `CMD_RESOLVE_LOCK` 且 `commit_version == 0` | skip | rollback resolve，不产生可见 namespace 变化 |
| `CMD_CHECK_TXN_STATUS` | skip | 诊断 / 状态推进，不是 namespace mutation |

#### Cursor vs commit_version

`raft_cursor = (region_id, term, index)` 是事件流 resume 的主键。它在单 region 内
随 raft apply 顺序单调，跨 region 需要由订阅者 / dispatcher 合并。

`commit_version` 是 Percolator 的 MVCC commit ts。它用于后续 snapshot read：

```go
value := Get(key, commit_version)
```

两者不能合并：

- 同一事务的 primary `CMD_COMMIT` 和 secondary `CMD_RESOLVE_LOCK` 可能共享同一个
  `commit_version`，但 raft index 不同。
- 不同 region 的 raft cursor 不可直接比较。
- 多个事务交错时，事件按 raft apply 到达，`commit_version` 不承诺在全局事件流中单调。

command pipeline 在成功 apply 后调用已注册 observer。**observer 调用必须
non-blocking**（observer 内部 goroutine 化或丢弃，详见 §6 back-pressure）。raftstore
apply 路径不能因为 watch subscriber 慢而阻塞。

### 4.2 prefix filter + dispatcher（fsmeta/exec/watch）

```
WatchRouter
  ├── 注册一组 subscriber（id → state{prefix, cursor, channel, budget}）
  ├── 接收 ApplyEvent → 按所有 subscriber 的 prefix 多路分发
  └── 维护每个 subscriber 的 outstanding budget（back-pressure）
```

dispatcher 在 fsmeta/exec 层、不在 raftstore——**保持 raftstore 不感知 fsmeta schema**（key prefix 解读由 fsmeta 决定）。

### 4.3 service 层与 client

`fsmeta/server` 的 `WatchSubtree` handler：
- 接收 `subscribe` 消息 → 在 WatchRouter 注册 subscriber（按 mount+prefix 计算 fsmeta key prefix）
- catch-up 阶段：从 resume_cursor 开始重放历史（v0 用 raft log + apply observer 二选一，详见 §5）
- live 阶段：逐事件 send，遵守 back-pressure
- 接收 `ack` 消息 → 转发给 router 释放 budget
- 客户端断连 → 注销 subscriber，丢弃 buffered 事件

`fsmeta/client` 提供 typed `Subscribe(ctx, req) → chan WatchEvent + Ack func`，把双向 stream 包装成 channel。

## 5. Catch-up 语义

**问题**：客户端用 `resume_cursor` 重连，要求服务端把"从该 cursor 之后到 live 现在"的事件流给它。raftstore apply observer 是流式的，不保留历史。

两条策略：
- **A（v0 选）**：raft log 内置已 retained。`raftstore/raftlog` 里已有 raft log + truncation；`fsmeta/exec/watch` 在 catch-up 时直接读 raft entries（按 region 收到的 cursor 范围），decode 后 emit。**前提**：log 还没被 truncate。如果 cursor < truncation point → 返回 `FailedPrecondition` 让客户端 reset 状态。
- **B（v1）**：单独的 cdc-style log，独立 retention。学术上更干净但工程量大。**Stage 2.2 不做。**

`WatchCatchupComplete` 表示 server 已 catch up 到 `live cursor`（最新 raft commit 时点），后面纯流式。

**FS / 对象存储 视角差异**：FS 客户端会用 catch-up 把 metadata cache 同步到当前；OS gateway 用 catch-up 重放 ETL trigger。两侧对 catch-up 缺失处理策略不同：FS 要 fallback 到 readdir 重新拉；OS 要重新 LIST 重 ingest。**这两个策略都在 client 侧实现**，server 只负责诚实告诉它"cursor 已截断"。

## 6. Back-pressure 协议

**目标**：slow subscriber 不能拖死 raftstore apply 路径，也不能把 fsmeta server 内存撑爆。

机制：
1. 每个 subscriber 在订阅时声明 `back_pressure_window`（默认 256）。
2. dispatcher 维护 `outstanding = sent - acked`。
3. 当 `outstanding >= window`：
   - server 停止推送给该 subscriber，事件继续在 dispatcher 内部 ring buffer 里等。
   - 如果 ring buffer 也满了 → 发 `WatchThrottle{reason="backlog overflow"}` → 5s 内仍未 ack → 关 stream，客户端必须 reconnect 并接受 cursor 可能截断。
4. raftstore apply observer **永远不阻塞**。observer 把 `ApplyEvent` 推给 dispatcher 用 `select { case ch <- evt: default: drop+metric }`。**raftstore 优先于任何订阅者**。

`fsmeta/server` 暴露 metrics：
- `fsmeta_watch_subscribers` (gauge)
- `fsmeta_watch_events_total` (counter, by mount+root)
- `fsmeta_watch_dropped_total` (counter)
- `fsmeta_watch_throttle_total` (counter)
- `fsmeta_watch_outstanding` (histogram)

## 7. 与 `meta/root` 的关系

**WatchSubtree 是数据面 watch**，不进 `meta/root`。理由：
- 数据面变更频率（每秒数万 dentry 变更）远超 `meta/root` 应承载的频率。
- `meta/root` 已有 `TailSubscription` 用于 namespace authority handoff（mount / quota / snapshot epoch）—— **不能把数据面 watch 也压进 rooted log**。

将来的对应：
- `WatchSubtree` 是 **subtree 上的 dentry / object 数据流**（数据面）
- `meta/root` `TailSubscription` 是 **namespace authority 变更流**（控制面）
- 客户端可能同时订阅两者——例如 FUSE 同时听 dentry 变化（cache invalidate）+ snapshot epoch 切换（重置缓存）

## 8. 失败语义

| 失败 | 客户端看到 | server 行为 |
|---|---|---|
| raftstore peer 切主 | `Unavailable` | router 把订阅迁到新 leader region；客户端 reconnect 透明（cursor 不变） |
| region 分裂 / 合并 | `Unavailable` 或 `FailedPrecondition` | v0：关流让客户端 reconnect；v1：路由到新 region |
| log truncated 超过 resume_cursor | `FailedPrecondition: cursor truncated` | 客户端必须重新 LIST / readdir |
| dispatcher backlog overflow | stream 关闭 | metrics 计数 + log warning |
| coordinator unavailable（store registry） | 不直接影响 watch | watch 路径只依赖 raftstore，不依赖 coordinator |
| fsmeta server 重启 | stream 关闭 | 客户端按 cursor 重连，新 server 实例继续 |

## 9. v0 不做的事（明确）

- **filter beyond prefix**：v1 加 `op_kinds`、`since_attr_change` 等。
- **broker fan-out**：单进程 fsmeta server。客户端多了就靠 fsmeta service 横向扩。
- **保证 catch-up**：log 截断时直接告诉客户端 fail。Stage 3 看是否需要 cdc-log 化。
- **跨 mount 订阅**：v0 一个 subscribe 只覆盖一个 mount + 一个 prefix。
- **strict per-key serialization**：保证全局 commit 顺序，但单 key 内的 multi-mutation 一次 raft entry 内的顺序未跨 entry 保证（v0 acceptable; 一般 metadata 不用单 entry 多 mutation）。

## 10. 实现拆 PR

| PR | scope | 测试 |
|---|---|---|
| **Stage 2.2-A** | raftstore store command post-apply observer interface + register/deregister + non-blocking dispatch + dropped metric | unit + raftstore/integration 加一个 observer 测试，断言 commit / resolve-lock 事件按 raft apply 顺序透传到 observer |
| **Stage 2.2-B** | `fsmeta/exec/watch` package：`WatchRouter`，prefix filter，per-subscriber back-pressure；纯逻辑可单元测 | 单元测试：100 万事件、N 个 subscriber、prefix 命中率、ack 释放 budget、overflow 关流 |
| **Stage 2.2-C** | `pb/fsmeta` 加 `WatchSubtree` bidi stream + `WatchEvent` 等 message；`fsmeta/server` 实装 handler；`fsmeta/client` 提供 typed Subscribe | bufconn integration（两端 typed） |
| **Stage 2.2-D** | 真集群 e2e（`fsmeta/integration`）+ `cmd/fsmeta-demo` workload "subtree subscribe under checkpoint storm"；catch-up 截断分支测试 | sub-second notification latency 断言 |
| **Stage 2.2-E**（可选 / 后置） | catch-up via raft log replay 优化 / metrics 完善 | benchmark 上 watch overhead < 5% |

每个 PR 通过后 push 到 `feature/fsmeta-stage2-foundation`，**不开 PR**直到 Stage 2 全部段位完成。

## 11. 完工判据

Stage 2.2 闭合 = 下面这条能讲：

> "在同一 NoKV docker-compose 集群里，客户端 `Subscribe(/checkpoints)`，独立另一个客户端在该目录下并发 1000 文件 create，第一个客户端能 sub-second 顺序收到 1000 个 `WatchEvent`，且能用 `Ack(cursor)` 控制 backlog。
> 重复同样的实验，对一个 bucket prefix（`s3-meta:/v1/`）做并发 PUT，效果一致——证明 watch 是 namespace-shape-agnostic、对 FS / OS 双消费者通用。"

数字进 `benchmark/fsmeta/results/`，design note + 数字一起进 Stage 2 PR。

## 12. 引用 / 锚点

| 类别 | 引用 | 用途 |
|---|---|---|
| 工业先例 | etcd `clientv3.Watch`（[etcd v3 watch](https://etcd.io/docs/v3.5/learning/api/#watch-api)） | key 粒度，反面示例 |
| | TiKV CDC（[TiKV CDC docs](https://tikv.org/docs/dev/reference/cdc/architecture/)） | region 粒度，反面示例 |
| | CockroachDB rangefeed（[CRDB changefeed core](https://www.cockroachlabs.com/docs/stable/create-changefeed.html)） | rangefeed 粒度，反面示例 |
| | S3 event notifications（[AWS S3 events](https://docs.aws.amazon.com/AmazonS3/latest/userguide/EventNotifications.html)） | 最终一致 baseline，对照 NoKV strong consistency |
| 内部模式 | `meta/root` `TailSubscription`（`meta/root/storage/virtual_log.go`） | NoKV 已成功的 cursor + ack 模式，directly inherits 形式 |
| 学术 | （空白）—— 未发现 prefix-scoped subtree watch 的 KV 层论文。**这是 Stage 2 paper 主攻点之一**。 | novelty claim |

## 13. 决策记录

- **2026-04-25 v0**：选 bidirectional stream，不选 server-stream + side-channel ack；catch-up 用现有 raft log 不引入独立 cdc log；back-pressure 默认 window=256；明确 raft log truncation 是客户端可见的硬错误。
- 后续 v1+ 修订入此文，每条带日期。
