# 2026-04-25 fsmeta Stage 2.2 WatchSubtree benchmark 结果

## 导读

- 🧭 主题：验证 `WatchSubtree` 在 checkpoint-style 元数据写入下的端到端通知延迟。
- 🧱 核心对象：`WatchSubtree`、`WatchEvent`、`watch_notify`、`fsmeta/exec/watch.Router`。
- 🔁 调用链：`raftstore apply observer -> KvWatchApply -> fsmeta RemoteSource -> WatchRouter -> fsmeta/server -> fsmeta/client`。
- 📚 结果文件：`benchmark/fsmeta/results/fsmeta_watchsubtree_20260425T083316Z.csv`。

## 结论

在同一个 NoKV Docker Compose 集群上，`watch-subtree` workload 创建 512 个文件，watch client 收到 512 个事件：

| operation | count | errors | avg | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|---:|
| `watch_notify` | 512 | 0 | 222.810 ms | 178.084 ms | 472.285 ms | 1235.457 ms |
| `watch_create` | 512 | 0 | 312.869 ms | 241.212 ms | 686.459 ms | 1737.813 ms |

主结论：`WatchSubtree` 的 p95 通知延迟在 512-event checkpoint storm 下低于 500ms，已经满足 Stage 2.2 "sub-second notification" 的基本证据。p99 超过 1s，说明当前 v0 还有 tail-latency 空间，主要应该继续看 fsmeta service 层和 Docker bridge 环境下的排队。

## 运行环境

| 项 | 值 |
|---|---|
| 日期 | 2026-04-25 |
| 运行方式 | Docker Compose，本机单机多容器 |
| 集群 | 3 meta-root + 3 coordinator + 3 raftstore + fsmeta service |
| benchmark 容器 | `golang:1.26.2-bookworm` |
| 网络 | Docker bridge network `nokv_default` |
| fsmeta endpoint | `nokv-fsmeta:8090` |
| workload | `watch-subtree` |
| clients | 8 |
| files | 512 |
| watch window | 1024 |

这组数字仍然是同机 Docker Compose 结果，只能作为 Stage 2.2 功能和相对延迟证据，不能当作裸金属或多机生产性能。

## 命令

```bash
RUN_TS=20260425T083316Z
docker run --rm --network nokv_default \
  -v "$PWD":/workspace \
  -w /workspace/benchmark \
  -e NOKV_FSMETA_BENCH=1 \
  golang:1.26.2-bookworm \
  go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -timeout 20m -v -args \
    -fsmeta_drivers native-fsmeta \
    -fsmeta_mount "fsmeta-watch-${RUN_TS}" \
    -fsmeta_addr nokv-fsmeta:8090 \
    -fsmeta_coordinator_addr nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379 \
    -fsmeta_workloads watch-subtree \
    -fsmeta_clients 8 \
    -fsmeta_files 512 \
    -fsmeta_watch_window 1024 \
    -fsmeta_timeout 15m \
    -fsmeta_output "results/fsmeta_watchsubtree_${RUN_TS}.csv"
```

注意：Go test 的工作目录是 package 目录 `benchmark/fsmeta`，所以 curated result 应写到 `results/...`，不是 `fsmeta/results/...`。

## Workload 形状

1. 创建一个 watched directory。
2. 打开 `WatchSubtree(KeyPrefix=EncodeDentryPrefix(mount, watched_dir))`。
3. 写入一个 warm-up dentry，确认 watch stream 已经真正接上。
4. 8 个 worker 并发创建 512 个文件。
5. watch client 对每个收到的 event 立即 `Ack(cursor)`。
6. `watch_notify` 记录从对应 create 发起前到 watch event 到达 client 的端到端延迟。

这不是纯 "commit 后网络推送延迟"，而是用户可感知的 create-to-notification 延迟。这个口径更适合作为 Stage 2.2 对外证据。

## Caveats

1. v0 是 live-only watch，没有历史 catch-up。断线后的修复策略仍然是客户端 `ReadDirPlus` / LIST 后重新订阅。
2. 本次运行前，当前 Docker volumes 来自 Stage 2.1 rooted membership 之前的旧 compose 集群；为了避免删除 volumes，手动向 coordinator 发布了 `StoreJoined(1..3)` 修复 membership。fresh `docker compose down -v && docker compose up -d --build` 不需要这一步。
3. p99 超过 1s，后续要补 watch metrics 后再定位 tail：可能来自 Docker bridge、fsmeta service 排队、store apply event fan-out 或 client-side scheduling。
4. 这组结果没有对比 generic-KV，因为通用 KV API 没有等价的 prefix-scoped live metadata watch 原语。

## 后续

- 补 `fsmeta_watch_*` metrics：subscriber 数、events total、dropped total、overflow total、notify latency。
- 加 `cmd/fsmeta-demo --mode=subscribe`，让这个 workload 可以作为 demo 而不是只在 benchmark test 里跑。
- Stage 2.3 进入 `SnapshotSubtree` 前，保留当前 live-only contract，不把 catch-up 混入 snapshot 语义。
