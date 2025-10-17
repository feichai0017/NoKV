# Distributed KV Integration Notes

## 最近进展
- Ready 流程已经接入 WAL typed record，Raft entries / HardState / Snapshot 经由 `wal.Manager.AppendRecords` 写入，并在 manifest 中记录 `EditRaftPointer`。
- `raftstore` 默认使用新的 `walStorage`（复用 `db.WAL()` 与 `db.Manifest()`）落盘；原有 `DiskStorage` 仅作为回退选项。
- LSM 在回收 WAL segment 前会遍历所有 raft 指针，确保慢 follower 的进度安全；新增的 `wal_storage_test.go` 覆盖了 entries、HardState、Snapshot 的重启恢复。

## 现状概览
- WAL/manifest 成为所有写入的单一事实来源：提交 Ready 时与普通写入共享批量和 fsync，崩溃恢复统一从 WAL 重建。
- Manifest `RaftPointers` 与 LSM log pointer 联动，决定可安全删除的 WAL segment；`raftstore/store_test.go`、`wal_storage_test.go`、`wal/manager_test.go` 已验证基线场景。
- 待完成项集中在极端时序与多 raft 组：慢 follower backlog、append 成功但 manifest 未落盘的双写注入、多组并发指针管理、snapshot resend 等。

## Ready→WAL Integration Draft

### Record Schema
- Extend `wal.Manager` with typed frames: one-byte `RecordType` followed by payload length + payload + CRC (keeping the existing `length|payload|crc` layout for compatibility by defaulting the type to `RecordMutation`).
- New record types:
  - `RecordRaftEntry` – payload is a packed list of `raftpb.Entry` messages (varint count + repeated `len|data` blocks) scoped to a `raftGroupID`.
  - `RecordRaftState` – stores the latest `raftpb.HardState` for a group.
  - `RecordRaftSnapshot` – stores `raftpb.Snapshot.Metadata` plus either inline chunk descriptors or an external file reference.
- Provide helpers under `raftstore/raftwal` (`raftwal.EncodeEntries`, `raftwal.EncodeHardState`, `raftwal.EncodeSnapshot`) to build these payloads and to replay them during recovery without leaking raft-specific knowledge into `wal.Manager`.

### Ready Handling
- 当前实现：`wal_storage` 在 Ready 里为 entries / HardState / Snapshot 追加对应的 typed record，并在成功后更新 manifest 指针；`raftstore/store.go` 直接复用该存储实现。
- 后续增强：继续对齐 TinyKV 的回压策略（批量大小、Ready backlog metrics），并在多 group 场景下拆分/合并批次，避免单个 Ready 拖垮 WAL write。

### Snapshot Materialisation
- Stage snapshot data in `snapshots/<group>/<term>-<index>.snap` using the existing temp-file + rename pattern (mirrors how flush builds SSTs). The WAL record carries:
  - Snapshot file ID (to locate the data)
  - `raftpb.Snapshot.Metadata`
  - Optional inline chunk checksums / sizes for verification
- After the WAL append succeeds, rename the temp snapshot into place and log the manifest edit (see below). Replay uses the manifest metadata to know which snapshot file to load and when it supersedes prior log entries.

### Manifest Updates
- 已完成：`manifest.EditRaftPointer` 记录 `{GroupID, Segment, Offset, AppliedIndex/Term, SnapshotIndex/Term, Commit}`；LSM 通过 `RaftPointerSnapshot` 决定 segment 回收。
- TODO：补充 `MinWalCheckpoint()` / `RaftGroupMinCheckpoint()` 工具方法、面向多 raft 组的统计输出，以及 pointer 滞后的告警机制。

### Recovery Flow
- During DB open:
  1. `manifest.Manager` loads both LSM and raft pointers.
  2. `raftwal.Replayer` opens WAL segments starting at the raft pointer, reconstructs in-memory `MemoryStorage` (`raftstore/storage.go`) by applying `RecordRaftSnapshot` (if any), `RecordRaftEntry`, and `RecordRaftState` in order.
  3. Once replay catches up, the resulting `readyStorage` exposes the same `Storage` interface but is now backed by WAL metadata rather than standalone files.
- Cleanup honours the manifest checkpoint: segments older than both the LSM pointer and *all* raft pointers can be removed; snapshots are deleted when a subsequent pointer indicates a newer snapshot or log truncation has moved past them.

- WAL append failure → 不推进 manifest 指针，保留 snapshot 临时文件，由后续 GC 清理。（需加限时重试/告警。）
- Crash between WAL append 与 manifest edit → entries / HardState / Snapshot 以及 Ready 批量、多 raft group 组合已通过 `wal_storage_test.go` 注入测试覆盖；下一步扩展到真实 peer Ready 批处理。
- Slow follower / 大快照 → 单元 + 集成测试（`levels_slow_follower_test.go`、`store_test.go::TestRaftStoreSlowFollowerRetention`）已验证指针阻塞与补齐，但监控指标、CLI 展示仍待补充。

- **Done** Double-write recovery：`raftstore/store_test.go` 覆盖写入→重启→一致性校验。
- **Done** WAL 重放恢复：`raftstore/wal_storage_test.go` 针对 entries / HardState / Snapshot 分别验证重启后的状态与 manifest 指针。
- **Planned** Crash injection：在 WAL append 成功但 manifest 未落盘、或 HardState/Entries 顺序异常时模拟崩溃并校验恢复。
- **In progress** 慢 follower backlog：`levels_slow_follower_test.go` + `raftstore/store_test.go::TestRaftStoreSlowFollowerRetention` 验证指针滞后时 GC 阻塞与抓取后的补齐，后续继续补全 metrics/告警。
- **In progress** 崩溃注入：`wal_storage_test.go` 覆盖 entries / HardState / Snapshot + Ready 批量、多 raft 组的 manifest 漏更新场景，待在真实 peer Ready 流程中注入。
- **Planned** Snapshot resend：Ready 同时携带 snapshot + entries，验证 follower 重启后 snapshot 应用与增量日志补齐。

## Metrics & CLI Backlog
- Add WAL-facing raft counters: Ready queue depth, per-type WAL bytes, flush latency histogram.
- Surface raft pointer vs LSM pointer deltas in `stats.go` + `cmd/nokv stats --workdir`. **Done** – CLI/expvar 现输出 `Raft.Groups`, `Raft.LaggingGroups`, `Raft.MaxLagSegments` 等指标，用于观测慢 follower backlog。
- Track snapshot creation/apply durations and expose via `/debug/vars` for future alerting.

## Phase 3 — Log GC & Snapshot Prep (work-in-progress)
- [ ] Ready pipeline Snapshot resend：为 follower 提供 snapshot + 增量补写，补充注入/互斥测试。
- [ ] Manifest 记录 truncated index/term，并提供 API/discard 流程（配合 WAL GC）。
- [ ] 多 raft 组指针管理：统计每组 backlog、提供 CLI 过滤查看。
- [ ] 设计 snapshot 文件布局（temp → durable）、失败回滚策略，并把场景加入 `db_recovery_test.go`。
