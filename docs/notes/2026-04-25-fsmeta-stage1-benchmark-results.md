# 2026-04-25 fsmeta Stage 1 benchmark 结果

## 导读

- 🧭 主题：验证 `fsmeta` native API 相比 generic KV schema 的 Stage 1 价值
- 🧱 核心对象：`ReadDirPlus`、`AssertionNotExist`、`native-fsmeta` driver、`generic-kv` driver
- 🔁 调用链：`fsmeta/client -> fsmeta/server -> fsmeta/exec -> raftstore/client -> stores`
- 📚 结果文件：`benchmark/fsmeta/results/fsmeta_formal_native_vs_generic_20260425T051640Z.csv`

## 结论

这轮结果里，`ReadDirPlus` 是 Stage 1 的主数字：

> 在同一个 NoKV Docker Compose 集群上，`native-fsmeta` 的 `ReadDirPlus`
> 平均延迟为 **12.0 ms**，`generic-kv` 为 **510.3 ms**。generic/native
> 平均延迟比约 **42.5x**，p50 比约 **44.8x**。

这不是底层存储引擎对比，而是 API shape 对比。两个 driver 使用同一个 NoKV 集群、同一个 raftstore、同一个 Percolator 事务层。差异只在元数据操作怎么表达：

- `native-fsmeta`：服务端原生 `ReadDirPlus`，一次 dentry scan + 一次 batch inode fetch。
- `generic-kv`：把 fsmeta 当 raw KV，用一次 dentry scan + N 次 point Get 组合出 `ReadDirPlus`。

## 运行环境

| 项 | 值 |
|---|---|
| 日期 | 2026-04-25 |
| 运行方式 | Docker Compose，本机单机多容器 |
| 集群 | 3 meta-root + 3 coordinator + 3 raftstore + fsmeta service |
| benchmark 容器 | `golang:1.26.2-bookworm` |
| 网络 | Docker bridge network `nokv_default` |
| fsmeta endpoint | `nokv-fsmeta:8090` |
| coordinator endpoints | `nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379` |

这组数字只能说明同一集群、同一机器、同一 workload 下的 native-vs-generic 比例。不能拿来声称 NoKV 绝对性能优于 TiKV、FoundationDB 或其他系统。

## Workload

命令：

```bash
docker run --rm --network nokv_default \
  -v "$PWD":/workspace \
  -w /workspace/benchmark \
  -e NOKV_FSMETA_BENCH=1 \
  golang:1.26.2-bookworm \
  go test ./fsmeta -run TestBenchmarkFSMeta -count=1 -timeout 30m -v -args \
    -fsmeta_drivers native-fsmeta,generic-kv \
    -fsmeta_mount fsmeta-formal-20260425T051640Z \
    -fsmeta_addr nokv-fsmeta:8090 \
    -fsmeta_coordinator_addr nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379 \
    -fsmeta_workloads checkpoint-storm,hotspot-fanin \
    -fsmeta_clients 8 \
    -fsmeta_dirs 16 \
    -fsmeta_files_per_dir 32 \
    -fsmeta_files 512 \
    -fsmeta_reads_per_client 16 \
    -fsmeta_page_limit 512 \
    -fsmeta_readdirplus=true \
    -fsmeta_timeout 25m \
    -fsmeta_output results/fsmeta_formal_native_vs_generic_20260425T051640Z.csv
```

`go test ./fsmeta` 的 test binary 工作目录是 `benchmark/fsmeta`，所以 committed result 路径使用 `results/...`。

数据形状：

| Workload | 参数 | 目的 |
|---|---|---|
| `checkpoint-storm` | 8 clients, 16 dirs, 32 files/dir, 512 create ops | 模拟 checkpoint 文件创建风暴 |
| `hotspot-fanin` | 512 files, 8 clients, 16 reads/client, page limit 512 | 模拟单目录 fan-in 下的 `readdir + stat` |

## 结果表

| workload | driver | operation | count | errors | throughput | avg | p50 | p95 |
|---|---|---:|---:|---:|---:|---:|---:|---:|
| checkpoint-storm | native-fsmeta | create_checkpoint | 512 | 0 | 21.048/s | 338.552 ms | 242.777 ms | 977.968 ms |
| checkpoint-storm | generic-kv | create_checkpoint | 512 | 0 | 16.715/s | 434.678 ms | 317.780 ms | 1132.722 ms |
| hotspot-fanin | native-fsmeta | seed_create | 512 | 0 | 6.032/s | 165.070 ms | 160.619 ms | 202.537 ms |
| hotspot-fanin | generic-kv | seed_create | 512 | 0 | 7.308/s | 120.550 ms | 114.639 ms | 148.740 ms |
| hotspot-fanin | native-fsmeta | readdirplus | 128 | 0 | 1.508/s | 12.020 ms | 11.344 ms | 16.696 ms |
| hotspot-fanin | generic-kv | readdirplus | 128 | 0 | 1.827/s | 510.289 ms | 508.717 ms | 539.054 ms |

Ratios:

| Operation | avg generic/native | p50 generic/native | throughput native/generic | 解读 |
|---|---:|---:|---:|---|
| `checkpoint-storm/create_checkpoint` | 1.28x | 1.31x | 1.26x | native 小幅领先，主要来自 server-side assertion 路径 |
| `hotspot-fanin/seed_create` | 0.73x | 0.71x | 0.83x | generic 更快，说明单 mutation 写入 native 多一层 service tax |
| `hotspot-fanin/readdirplus` | 42.45x | 44.84x | 0.83x | native 明显领先，API shape 减少了 N 次 point Get |

## 解读

### 1. `ReadDirPlus` 是 Stage 1 的 headline

`ReadDirPlus` 的差距来自 RPC / KV 操作数量：

| Path | 操作形状 |
|---|---|
| native | `Scan(dentry prefix)` + `BatchGet(inode keys)` |
| generic | `Scan(dentry prefix)` + `Get(inode key)` × 512 |

这正是 fsmeta 这条线要证明的事：元数据 workload 的自然形状不是 raw KV API。把 `readdir + stat` 提升为服务端原语后，系统能少做大量 round trip 和 point lookup。

### 2. Create 路径只有小幅领先

`checkpoint-storm/create_checkpoint` native 平均延迟低约 28%。这条改善来自 `Create` 的原子语义：

- native：直接发 2PC mutation，Percolator prewrite 阶段执行 `AssertionNotExist`。
- generic：client 先 `Get` dentry 和 inode，确认不存在后再 Put。

这条不是 headline，但它说明 server-side assertion 不是只解决正确性，也减少了部分前置读。

### 3. 单点写入 native 不一定更快

`hotspot-fanin/seed_create` 里 generic 更快。原因很清楚：generic driver 直接用 `raftstore/client`，native 多走 `fsmeta gRPC service -> executor -> runner`。对单 mutation 写入来说，native API 的服务层会带来额外开销。

这条必须写清楚。fsmeta 的优势不在"所有操作都更快"，而在 fused metadata primitive 和更强的服务端原子语义。单点写入如果只看 latency，generic path 可能更低。

## Caveats

1. 这是 Docker Compose 单机多容器结果，不能代表裸金属、多机网络或生产部署。
2. 这不是 NoKV vs TiKV / FDB 的系统对比。两个 driver 都跑在 NoKV 上，目的是隔离 native API shape 的收益。
3. `ReadDirPlus` 的 generic baseline 是串行 point Get。并发 fan-out 可以缩小差距，但那是另一个优化 baseline，应该作为第三个 driver 单独测。
4. 第一次完整 run 在集群刚重启后出现过一次 `generic-kv/checkpoint-storm` 的 transient failure。补充错误样本输出后，单独重跑 generic checkpoint 通过，第二次完整 run 通过。本次记录以第二次完整通过 run 为准。
5. 吞吐字段受 workload 总 duration 影响，`hotspot-fanin` 的 seed 和 read 阶段在同一个 workload duration 里统计；判断核心应看 per-operation latency。

## 代码中已经补的护栏

- `generic-kv` driver 复用 `fsmeta/exec.TxnRunner`，保证对比使用同一 raftstore / Percolator substrate。
- `generic-kv` Create 明确不使用 `AssertionNotExist`，测试会检查 mutation 上没有 native assertion。
- `generic-kv` ReadDirPlus 明确使用 point Get，测试会检查没有走 `BatchGet`。
- benchmark CSV 增加 `driver` 列，可以在同一份结果里直接分组画图。
- workload 失败时会输出前几个错误样本，不再只给 `N/M operations failed`。

## 下一步

Stage 1 可以收尾。后续正式结果要补两类：

1. 加 `generic-kv-fanout` driver，衡量 client-side 并发 point Get 能把差距缩到多少。
2. 在 Stage 2 引入 `WatchSubtree` / `RenameSubtree` / `QuotaFence` 前，每条 primitive 先落 design note，再加同集群 native-vs-generic 对照。
