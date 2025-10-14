# Crash Recovery Verification Plan

Phase 7 的首要目标是把“崩溃—重启—校验”流程自动化，确保 WAL、Manifest、ValueLog
在各种中断点都能恢复到一致状态。这里列出需要覆盖的场景与验证要点，后续测试脚本 /
集成测试可直接按矩阵实现。

## 1. 关键中断点

| 类别 | 中断瞬间 | 期望恢复行为 | 关键检查 |
|------|----------|--------------|----------|
| WAL flush | 写入已落 WAL，但 MemTable/Manifest 尚未安装 | 重放 WAL，确保数据仍存在；无重复写入 | 数据一致、WAL 重放指针推进 |
| Flush install | Manifest 写入 VersionEdit，但 SST 暂未 rename | 重启时发现缺失文件并回滚 Edit | Manifest/Level 列表不包含挂起文件 |
| ValueLog append | ValueLog 新增 entry，head 未更新 | Restart 后继续从旧 head 写入，并能读取写入数据 | head 校验、读数据一致 |
| ValueLog rotate | 新段创建但 Manifest 未记录 head | 重启时识别 active FID，避免重复重放 | active FID 匹配，数据完整 |
| ValueLog GC | Manifest 标记删段，但文件未删除 | 重启时自动删除遗留 `.vlog` 文件 | 文件清理，Manifest 与磁盘一致 |
| Manifest rewrite | CURRENT 切换，旧文件保留 | 读取最新 CURRENT，旧 manifest 可忽略 | CURRENT 指向文件存在，Version 正确 |

## 2. 自动化策略

1. **测试框架**：在 `db_recovery_test.go` 中按场景构造数据 → 精确注入故障（删除文件、
   截断、跳过 Close）→ 再次 `Open` → 断言状态。当前已覆盖：
   - `TestRecoveryRemovesStaleValueLogSegment`：验证 manifest 标记删除后，重启时自动清理遗留 `.vlog`。
   - `TestRecoveryWALReplayRestoresData`：验证 WAL 重放能恢复尚未 flush 的写入。
   - `TestRecoveryCleansMissingSSTFromManifest`：验证缺失的 `.sst` 会在恢复时从 manifest 中移除。
   - `TestRecoveryManifestRewriteCrash`：验证 manifest rewrite 崩溃后仍沿用旧 CURRENT，且临时文件被清理。
2. **故障注入方法**：
   - 直接操作磁盘文件（`os.Remove`, `truncate`）模拟 crash。
   - 通过守护线程 kill 进程或跳过 `Close()`，验证未清理资源也能恢复。
   - 针对 ValueLog GC，可手动调用 `lsm.LogValueLogDelete` 并保留 `.vlog` 文件，模拟
     manifest 已写、文件未删。
3. **断言工具**：
   - 重启后使用新的 CLI（`nokv stats/manifest/vlog --json`）或单元测试内部 API
     (`db.Info().Snapshot()`, `db.lsm.ValueLogStatus()`)，确认各项指标。
   - 比较写入数据与期待值；检查活跃 head、段列表、WAL replay 指针等。

## 3. 输出与脚本化

- 集成测试通过后，编写 `scripts/recovery_scenarios.sh`（或 Go-based driver）串行执行
  所有场景，供 CI / 手工诊断使用。
- 当前仓库已提供 `scripts/recovery_scenarios.sh`，运行 `./scripts/recovery_scenarios.sh`
  可依次验证四个 crash 场景（默认复用仓库根目录下的 `.gocache`）。
- 脚本会开启 `RECOVERY_TRACE_METRICS`，将每个场景的 `RECOVERY_METRIC` 日志保存到
  `artifacts/recovery/<TestName>.log`，便于后续分析；手动调试时同样可以通过
  `RECOVERY_TRACE_METRICS=1 go test -run TestRecovery... -v ./...` 查看指标。
- 将每个场景的关键指标写入日志，便于压测或 Prometheus 接入时复用。

以上规划完成后，可进入实现阶段：先落地最小集成测试（例如 ValueLog GC 遗留段、WAL
重放），再逐步扩展到全套故障注入矩阵。*** End Patch
