# Testing & Validation Plan

本计划梳理 NoKV 当前存在的测试覆盖，以及后续需要补齐的模块化与系统级验证项。目标是在持续迭代中保持 WAL、LSM、ValueLog、事务等核心链路的可回归性和可观测性。

---

## 1. 快速开始

```bash
# 基础单测：覆盖 WAL/Manifest/LSM/事务等核心模块
GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache go test ./...

# 事务与迭代器专项
go test ./... -run '^TestTxn|TestConflict|TestTxnIterator'

# 恢复流程矩阵（结合 WAL、ValueLog、Manifest）
RECOVERY_TRACE_METRICS=1 ./scripts/recovery_scenarios.sh

# 性能回归（与 Badger/RocksDB 对比）
go test ./benchmark -run TestBenchmarkResults -count=1
```

> 建议 CI 中使用固定 `GOCACHE`/`GOMODCACHE` 目录，避免权限问题并提升缓存命中率。

---

## 2. 模块化覆盖概览

| 模块            | 现有测试文件                                                   | 覆盖要点                                                                                           | 待补充方向                                                                                      |
|-----------------|----------------------------------------------------------------|----------------------------------------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|
| WAL             | `wal/wal_test.go`, `wal/manager_test.go`                        | 段切换、Sync、重放、崩溃恢复                                                                      | IO 错误注入、写入限速、并发 append                                                              |
| MemTable / LSM  | `lsm/lsm_test.go`, `lsm/memtable_test.go`, `lsm/iterator_test.go`, `lsm/compact_test.go` | SkipList 正确性、memtable recovery、compaction 触发                                               | Back-pressure、冷热表切换、Bloom 误判率                                                        |
| Manifest        | `manifest/manager_test.go`, `manifest/levels_test.go`           | VersionEdit 应用、CURRENT 切换、SST 装载                                                          | Manifest rewrite 崩溃注入、ValueLog 引用一致性                                                  |
| ValueLog        | `vlog/vlog_test.go`, `vlog/gc_test.go`                          | ValuePtr 编解码、GC 重写、段回收                                                                   | 多事务并发读取、GC 期间 iterator/txn 引用计数                                                  |
| 事务 / Oracle   | `txn_test.go`, `txn_iterator_test.go`, `txn_metrics_test.go`    | MVCC 冲突检测、迭代器快照、活跃事务统计                                                          | 长事务+并发写、CommitWith 回调链路、managed 模式                                               |
| DB 集成         | `db_test.go`, `db_recovery_test.go`, `db_recovery_managed_test.go` | Flush、Compaction、Recovery 流程、写入节流                                                         | 组合场景（ValueLog GC + Compaction）、写入限流与吞吐观测                                       |
| CLI / Stats     | `cmd/nokv` 子命令（待补）                                      | —                                                                                                | 命令行工具的端到端测试、指标暴露                                                              |
| Benchmark       | `benchmark/benchmark_test.go`                                   | NoKV vs Badger/RocksDB 写/读/批量/范围对比                                                        | 固定预热与并发策略、长时间压力测试                                                             |

---

## 3. 系统级测试矩阵

| 场景                     | 覆盖脚本 / 测试                        | 说明                                                                                   |
|--------------------------|-----------------------------------------|----------------------------------------------------------------------------------------|
| 崩溃恢复                 | `scripts/recovery_scenarios.sh`         | 依次验证 WAL 重放、缺失 SST、ValueLog 截断等场景                                       |
| 事务冲突 + 并发写入      | `TestConflict`, `TestTxnReadAfterWrite` | 高并发写同一键、读写混合，校验 Oracle 冲突检测与读视图                                |
| 值分离 + GC              | `vlog_test.go`, `db_recovery_test.go`   | 大 Value 写入、GC 重写后的数据一致性                                                   |
| 迭代器一致性             | `txn_iterator_test.go`, `lsm/iterator_test.go` | 事务快照 + LSM 层合并迭代                                                               |
| 写入节流/背压            | `lsm/compact_test.go`, `db_test.go`      | L0 backlog 触发限流逻辑                                                                 |
| 性能回归                 | `benchmark` 包                          | 对比 Badger/RocksDB，检查吞吐/延迟趋势                                                  |

> 建议在 CI 中对系统级脚本单独分阶段执行，并产出结构化日志（`RECOVERY_METRIC` 等）供复盘。

---

## 4. 待办与改进建议

1. **错误注入与压力测试**：为 WAL、Manifest、ValueLog 引入随机 IO 错误与并发压力，补齐上表中的“待补充方向”。
2. **事务长压场景**：编写长事务 + 大批量冲突的专用测试，覆盖 `CommitWith`、managed 模式。
3. **CLI/工具测试**：为 `cmd/nokv` 增加 Golden Test 或端到端流程，保障调试工具质量。
4. **Benchmark 规范化**：拆分冷启动与热态数据，固定并发、预热时长，引入资源监控（CPU/内存/IO）。
5. **覆盖率监控**：在 CI 中启用 `go test -coverprofile` 并按模块输出覆盖率，结合上述矩阵持续跟踪。

---

通过本测试计划，可对 NoKV 的关键路径进行可持续验证，并为后续扩展（多列族、快照、备份等）打下基础。执行过程中如有新增模块或测试场景，应同步更新此文档与自动化脚本。 
