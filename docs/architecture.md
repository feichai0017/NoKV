# NoKV Architecture & Design Overview

> 参考架构：RocksDB（LSM + Manifest + CURRENT/WAL）与 BadgerDB（LSM + ValueLog 分离）。

本设计文档旨在为后续的工业化改造提供统一蓝图，涵盖磁盘布局、核心数据路径、恢复流程以及并发与监控策略。文中的模块名称沿用当前仓库结构，便于逐步重构。

---

## 1. 设计目标

- **一致性**：写入必须先持久化到 WAL / ValueLog，SST 仅承载已确认数据。
- **可恢复**：宕机后通过 CURRENT → MANIFEST → WAL/ValueLog 重放，恢复到最后一次成功的写入。
- **高可用**：引入层级化 Compaction 与 ValueLog GC，限制写放大和空间放大。
- **可观测**：暴露 flush/compaction backlog、磁盘使用、事务冲突等指标。
- **可扩展**：模块化设计，后续可添加列族、快照、备份、一致性协议。

---

## 2. 目录与文件布局

| 文件 / 目录                 | 说明                                                                 |
|---------------------------|----------------------------------------------------------------------|
| `CURRENT`                 | 指向当前活跃的 MANIFEST 文件，例如 `MANIFEST-000123`。                     |
| `MANIFEST-<id>`           | 元数据日志（VersionEdit），记录 SST 创建/删除、ValueLog 依赖等信息。           |
| `000001.wal`              | Write-Ahead Log 段文件，顺序写入，每次 DB Open 建立新段。                      |
| `000001.sst`              | SSTable 数据文件，包含数据块、索引、Filter。                               |
| `000001.vlog`             | ValueLog 数据段，大 Value 或 TTL 数据驻留其中。                            |
| `tmp/`                    | Flush / Compaction 临时文件目录，成功后 rename 生效。                      |
| `archive/` (可选)         | 保留旧 WAL / Manifest 快照，便于调试与备份。                               |

---

## 3. 核心模块与职责

### 3.1 DB Core (`db.go`)
- 维护对 LSM、ValueLog、Oracle、Stats 的引用。
- 提供 `Set/Get/Del/NewTransaction/NewIterator` 等对外 API。
- 控制写入管线（批处理、发送到 WAL + MemTable + flush 管线）。
- 管理关闭流程：阻塞新请求 → flush → 停 compaction → 关闭日志。

### 3.2 WAL 子系统 (`wal/manager.go`)
- 由独立的 `wal.Manager` 统一管理段文件：`Append`、`Sync`、`Rotate`、`Replay` 均已实现并配套单测。
- 段命名固定为 `%05d.wal`，位于 `options.WorkDir/wal/`；达到阈值或 flush 完成后可调用 `Rotate`。
- Flush 任务在完成安装后，通过 manager 的 API 标记并删除旧段（与 Manifest 记录联合保障持久性）。
- Crash 恢复：Open 时按 `CURRENT` → manifest → `wal.Manager.Replay` 顺序重放，重建 MemTable。

### 3.3 MemTable / Flush 管线 (`lsm/flush`)
- 活跃 MemTable 使用 SkipList；达到阈值后被冻结成 Immutable。
- `flush.Manager` 已落地四阶段状态机：`Prepare → Build → Install → Release`，持久化 flush 元数据并与 manifest/wal 协作。
- Flush 失败会保留临时文件与任务描述，重启后可恢复执行；成功后触发 WAL 段释放。
- 后续任务：在安装阶段补充更多观测点（时延、队列长度），并与 compaction 做调度协调。

### 3.4 SSTable 管理 (`lsm/`)
- L0~Ln 层次结构，采用 size-tiered/leveling 混合策略。
- `Manifest` 记录层级文件列表、最大最小 key、ValueLog 引用。
- `Cache` 预加载 Bloom / Index，减少随机 IO。
- Compaction：根据层级大小、Overlap、冷热点调度。

### 3.5 Manifest 管理 (`manifest/manager.go`)
- 采用 VersionEdit + CURRENT 的结构，所有 SST/WAL/ValueLog 事件通过 `manifest.Manager.LogEdit` 记录。
- Open 时读取 CURRENT → MANIFEST 构造 Version，并在必要时重写 manifest 以控制体积。
- rewrite、CURRENT 原子切换等边界情况已有测试覆盖；后续需补充对 ValueLog head/删除事件的记录。

### 3.6 ValueLog 子系统 (`vlog/manager.go` + `vlog.go`)
- ValueLog 现由 `vlog.Manager` 统一管理段的 Append/Rotate/Read/Remove，写入格式复用 WAL 编码。
- GC 重构完成：通过 WAL 解码顺序扫描旧段、重写有效 entry、批量写回并删除原文件；迭代器/事务引用计数全流程可用。
- Crash 恢复：Open 时由 manager 读取现有段和写入 offset，再结合 `!NoKV!head` 与 manifest state 恢复写指针。
- 下一步需将 GC 操作（head 更新、删除列表）纳入 manifest VersionEdit，确保崩溃后也能复原 manager 状态。

### 3.7 Oracle / 事务 (`txn.go`)
- MVCC 时间戳分配、冲突检测、潜在快照隔离点。
- `WaterMark` 追踪读写上限，配合 discardTs 决定可回收版本。
- 事务 commit 复用 DB 写入流水线，保证 WAL / MemTable / ValueLog 原子性。
- 事务迭代器基于 `readTs` 构建 LSM 快照，并合并自身 pending writes，支持多版本遍历与一致快照视图。

### 3.8 统计与监控 (`stats.go`)
- 周期采集 LSM/ValueLog backlog、运行状态，并通过 `expvar` 暴露。
- 当前指标：写入计数、flush/compaction backlog、`NoKV.Compaction.*`、`NoKV.ValueLog.*` 与 `NoKV.Txns.*` 系列；离线巡检可使用 `cmd/nokv stats`。
- 引入 `hotring` 热点追踪器：在事务/DB 读路径中记录 key 访问频次，并在 Stats/CLI 中输出 Top-N 热点，为缓存、限流和调度策略提供决策依据。
- 后续拓展：慢请求日志、压缩比、空间放大、命令行工具导出（如 `nokv stats/manifest`）。

---

## 4. 数据路径

### 4.1 写路径
1. 对请求按 `MaxBatchCount/MaxBatchSize` 聚合。
2. 进入写入流水线：
   - 按顺序写入 WAL（确保 `fsync` / `fdatasync` 语义）。
   - 更新 MemTable（active skiplist）。
3. 若 value 需分离，先写入 ValueLog，记录 `ValuePtr`。
4. MemTable 达阈值 → 冻结 → flush 管线。

### 4.2 读路径
1. 构建多层迭代器：Txn pending writes → `readTs` 快照（MemTable/Immutable/SST）→ ValueLog（按需）。
2. 判断 tombstone / TTL，过滤内部 key（`!NoKV!` 前缀）。
3. 事务读取使用 `readTs`，过滤版本。

### 4.3 Flush / Compaction
同 3.3/3.4 所述，现已具备：
- 背压机制：`compactionManager` 根据 L0 backlog 触发写入限流（`blockWrites`）。
- Compaction 调度：事件驱动（flush、周期性）+ 优先级调度，优先处理 L0→L1。
- 失败重试、暂停/恢复能力。

### 4.4 ValueLog GC
- manager 维护段引用计数；GC 期间若存在活跃迭代器/事务，会延迟删除。
- rewrite 通过 WAL 编码重放旧段并写回有效 entry，完成后调用 `manager.Remove` 删除原段。
- Manifest 记录 ValueLog head 与删段事件；启动时自动回收标记为无效的段，并结合 discard stats 选择候选。

### 4.5 Crash Recovery
1. 读取 `CURRENT` 找到 manifest。
2. 重放 manifest 构造 Version state。
3. 扫描 WAL：重放所有 entry → 恢复 MemTable。
4. 若 ValueLog `head` 落后，按 checkpoint 继续 replay。
5. 修复未完成的 flush/compaction：检查临时文件、补写 manifest（缺失 `.sst` 会在恢复时被移除）。
6. 恢复测试矩阵详见 `docs/recovery.md`，涵盖 WAL 重放、ValueLog 段回收、Manifest 残留清理与 CURRENT rewrite 崩溃等场景。
7. 单测在设置 `RECOVERY_TRACE_METRICS=1` 时会输出结构化 `RECOVERY_METRIC` 日志，可结合 `scripts/recovery_scenarios.sh` 收集并分析恢复过程指标。

### 4.6 关闭流程
1. 阻塞新写（`blockWrites`）。
2. Flush 活跃 MemTable / ValueLog buffer。
3. 停止 compaction、GC、统计后台任务。
4. 顺序关闭 WAL、SST cache、Manifest、ValueLog。
5. 更新 CURRENT、写关闭标记（可选），防止脏退出。

---

## 5. 并发与资源管理

- **引用计数**：MemTable、SST、ValueLog 段、迭代器需统一引用管理。
- **锁粒度**：WAL 使用独立互斥，Manifest 更新串行化；Compaction 使用细粒度读写锁。
- **后台任务协调**：Flush / Compaction / ValueLog GC 使用共享 `Throttle` 控制并发，支持动态限速。
- **错误处理**：避免直接 Panic，返回错误或触发重试/降级。

---

## 6. 监控与工具

- Metrics：写入 TPS、flush backlog、compaction 延迟、value log GC 比例等。
- 当前通过 `expvar` 与 `cmd/nokv stats` 导出的关键指标：
  - `NoKV.Compaction.RunsTotal / LastDurationMs`
  - `NoKV.ValueLog.GcRuns / SegmentsRemoved / HeadUpdates`
  - `NoKV.Txns.{Active,Started,Committed,Conflicts}`
- 恢复测试阶段可通过 `RECOVERY_TRACE_METRICS` 收集额外指标（GC 清理、manifest rewrite、WAL replay 等），并由 `scripts/recovery_scenarios.sh` 自动归档日志。
- 日志：关键阶段打 info/error，支持 trace 调试。
- 运维工具：
  - `nokv stats` / `nokv manifest` / `nokv vlog`
  - `nokv sst inspect`
  - `nokv vlog gc --dry-run`
  - `nokv repair`（快速校验与修复）。

---

## 7. 开发阶段拆分

| 阶段 | 状态 | 说明 |
|------|------|------|
| 1. WAL 重构 | ✅ 已完成 | `wal.Manager` + Replay 流程上线并覆盖单测 |
| 2. Flush 管线 | ✅ 已完成 | `lsm/flush.Manager` 负责状态机、任务恢复、与 WAL 协同 |
| 3. Manifest/Version | ✅ 已完成 | `manifest.Manager` 管理 VersionEdit、CURRENT 切换 |
| 4. ValueLog 重构 | ✅ 已完成 | `vlog.Manager` + WAL 编码 + GC 重写逻辑落地 |
| 5. Compaction 框架 | ✅ 已完成 | `compactionManager` 支持事件触发、L0 背压与写入限流，指标统一 |
| 6. 事务/迭代器完善 | ✅ 已完成 | 快照迭代器 + 多版本遍历落地，并暴露事务活跃/冲突指标 |
| 7. 完整恢复/压力测试 | ✅ 已完成 | 崩溃恢复脚本、混合 workload、CLI 工具（`nokv stats/manifest/vlog` 已就绪）、Prometheus 集成 |

### 测试与验证

- **单元 + 集成测试**：执行 `GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache go test ./...` 覆盖 WAL、Manifest、LSM、ValueLog、事务等核心链路。
- **恢复矩阵**：`RECOVERY_TRACE_METRICS=1 ./scripts/recovery_scenarios.sh` 验证 WAL 重放、缺失 SST、ValueLog 截断等异常场景。
- **性能基准**：`go test ./benchmark -run TestBenchmarkResults -count=1` 生成 NoKV vs Badger/RocksDB 吞吐延迟对比（需启用 `benchmark_rocksdb` 标签以测试 RocksDB）。
- **测试计划**：详见 [docs/testing.md](testing.md)，按模块列出已覆盖项与待补充用例，建议在 CI 中开启覆盖率统计与错误注入脚本。

后续里程碑聚焦于：  
1. **Compaction 策略优化**：结合热度统计/多策略调度，进一步降低写放大。  
2. **Manifest ↔ ValueLog 联动**：在 VersionEdit 中记录 head/删段信息，补齐崩溃恢复闭环。  
3. **观测性与工具**：完善指标、命令行工具与压测场景，靠近 RocksDB/Badger 的运维体验。

每阶段需保证 `go test ./...` 通过，新增针对性单测及集成测试。

---

## 8. 架构示意

```mermaid
graph TD
    Client[Client API] -->|Set/Get| DBCore
    DBCore -->|Append| WAL
    DBCore -->|Insert| MemTable
    DBCore -->|ValuePtr| ValueLog
    MemTable -->|Flush Task| FlushMgr
    FlushMgr -->|Build SST| SSTBuilder
    SSTBuilder -->|Install| Manifest
    Manifest -->|Version State| LSMLevels
    LSMLevels -->|Compaction Jobs| Compactor
    Compactor -->|Discard Stats| ValueLog
    ValueLog -->|GC| Manifest
    DBCore -->|Txn| Oracle
    Oracle -->|Conflict Check| DBCore
    DBCore -->|Metrics| Stats
```

---

## 9. 与 RocksDB / Badger 的对比要点

| 主题              | RocksDB                                 | Badger                                  | NoKV 设计方向                                               |
|------------------|-----------------------------------------|-----------------------------------------|------------------------------------------------------------|
| WAL              | 顺序写、底层 Env 抽象                   | 默认不使用（Managed mode）              | 采用顺序写 + 清晰 flush 管线，为 crash recovery 服务                 |
| Manifest         | VersionEdit + CURRENT                   | 暂存到 value log / table manifest       | 引入 VersionEdit，CURRENT 原子指向最新 manifest                 |
| ValueLog         | 无（数据直接 LSM）                      | 大 Value 存储在 vlog，GC 基于 discard stats | 结合 LSM + ValueLog 分离，减少写放大，GC 与 compaction 协调           |
| Compaction       | leveled / universal 多策略              | 二级 LSM，重点在 value log               | 支持 leveled，添加 backpressure，与 ValueLog GC 协同             |
| Iterator/Txn     | Snapshot + Sequence Number               | MVCC 事务（managed/unmanaged）           | 沿用 Oracle + MVCC，补齐 iterator seek、引用计数                  |
| Observability    | 丰富的 metrics / logging / tools         | Badger CLI / metrics                     | 增强统计模块、提供 CLI 工具、暴露指标                           |

---

## 10. 后续工作

- 完成设计文档评审，确定模块边界与数据格式。
- 在 `docs/` 内继续添加详细子文档（WAL、Manifest、Compaction 等）。
- 按阶段拆分开发任务，建立功能分支与合并策略。
- 定义测试矩阵（功能 / 恢复 / 性能 / 压力），引入 CI Pipeline。

---

如需进一步扩展，可在文末补充开放问题（例如：多列族支持、冷热分层、远程备份、Raft 集成等），帮助后续规划。该设计将作为 NoKV 向工业级存储引擎迈进的蓝图。欢迎 review 后提出改进建议。 
