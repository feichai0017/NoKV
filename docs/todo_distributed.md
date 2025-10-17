# Distributed KV Integration Notes

## Recent Progress
- Ready 流程已经接入 WAL typed record，Raft entries / HardState / Snapshot 都经由 `wal.Manager.AppendRecords` 写入并实时更新 manifest `EditRaftPointer`。
- `raftstore.Peers` 默认使用新的 `walStorage`（传入 `db.WAL()` + `db.Manifest()`）来落盘，原先的 `DiskStorage` 仍保留作回退路径。
- Manifest 记录了 `RaftPointers`，LSM 在回收 WAL segment 前会检查所有 raft 进度，避免删除还未消费的日志。

## Current Snapshot
- `raftstore.Peer` persists Ready state straight into `DiskStorage` (`raftstore/store.go:194`), which in turn writes `raft.log`, `raft.hard`, and `raft.snap` beside the DB (`raftstore/storage.go:33-187`). This bypasses the shared WAL/manifest pipeline and duplicates crash-recovery logic.
- Recovery flows for the core engine already rely on WAL segments + manifest checkpoints (`lsm/levels.go:160-215`, `lsm/memtable.go:96-168`), while raft recovery simply reloads the flat files before wrapping an in-memory `MemoryStorage`.
- Integration tests confirm replication and restart (`raftstore/store_test.go`), but they only exercise the bespoke raft files, so WAL/manfiest consistency is currently untested for raft.

## Ready→WAL Integration Draft

### Record Schema
- Extend `wal.Manager` with typed frames: one-byte `RecordType` followed by payload length + payload + CRC (keeping the existing `length|payload|crc` layout for compatibility by defaulting the type to `RecordMutation`).
- New record types:
  - `RecordRaftEntry` – payload is a packed list of `raftpb.Entry` messages (varint count + repeated `len|data` blocks) scoped to a `raftGroupID`.
  - `RecordRaftState` – stores the latest `raftpb.HardState` for a group.
  - `RecordRaftSnapshot` – stores `raftpb.Snapshot.Metadata` plus either inline chunk descriptors or an external file reference.
- Provide helpers under `raftstore/raftwal` (`raftwal.EncodeEntries`, `raftwal.EncodeHardState`, `raftwal.EncodeSnapshot`) to build these payloads and to replay them during recovery without leaking raft-specific knowledge into `wal.Manager`.

### Ready Handling
- Replace direct `DiskStorage` writes in `handleReady` with a `raftwal.Writer` that batches all Ready components:
  1. Gather `rd.Entries` and encode them as a `RecordRaftEntry`. Keep the batch bounded (e.g. split if payload > 4 MiB) to match WAL buffering behaviour.
  2. If `rd.HardState` is non-empty, append a `RecordRaftState` record *after* the entries to preserve raft’s durability order.
  3. If `rd.Snapshot` is non-empty, write a `RecordRaftSnapshot` record referencing the snapshot contents (see below).
- Submit the records through `db.EnqueueWalBatch(records)` (a thin wrapper around `wal.Manager.AppendRecords`) so raft traffic observes the same backpressure and Sync semantics as regular writes. The call should return the `wal.EntryInfo` of each frame so manifest updates can checkpoint progress.

### Snapshot Materialisation
- Stage snapshot data in `snapshots/<group>/<term>-<index>.snap` using the existing temp-file + rename pattern (mirrors how flush builds SSTs). The WAL record carries:
  - Snapshot file ID (to locate the data)
  - `raftpb.Snapshot.Metadata`
  - Optional inline chunk checksums / sizes for verification
- After the WAL append succeeds, rename the temp snapshot into place and log the manifest edit (see below). Replay uses the manifest metadata to know which snapshot file to load and when it supersedes prior log entries.

### Manifest Updates
- Introduce a new edit kind, e.g. `EditRaftLogPointer`, carrying `{GroupID, SegmentID, Offset, TruncatedIndex, TruncatedTerm, SnapshotFile}`.
- Extend `manifest.Version` with:
  - `RaftPointers map[uint64]RaftPointer` (one per raft group; start with group `1` for single-region).
  - `MinWalCheckpoint()` helper that returns `min(lsmLogPointer, minRaftPointer)` so segment GC only reclaims files acknowledged by both LSM and raft.
- Ready processing pipeline:
  1. Append raft records via WAL.
  2. On success, emit `EditRaftLogPointer` with the latest `(segmentID, offset)` returned by the append plus the corresponding truncated index/term derived from raft storage.
  3. If a snapshot was written, include `SnapshotFile` so replay can short-circuit log replay after applying the snapshot.
- Update manifest replay logic to load raft pointers into memory, hook `levelManager` so WAL retention honours them, and surface the active pointer to the raft recovery code.

### Recovery Flow
- During DB open:
  1. `manifest.Manager` loads both LSM and raft pointers.
  2. `raftwal.Replayer` opens WAL segments starting at the raft pointer, reconstructs in-memory `MemoryStorage` (`raftstore/storage.go`) by applying `RecordRaftSnapshot` (if any), `RecordRaftEntry`, and `RecordRaftState` in order.
  3. Once replay catches up, the resulting `readyStorage` exposes the same `Storage` interface but is now backed by WAL metadata rather than standalone files.
- Cleanup honours the manifest checkpoint: segments older than both the LSM pointer and *all* raft pointers can be removed; snapshots are deleted when a subsequent pointer indicates a newer snapshot or log truncation has moved past them.

### Failure / Corner Cases
- WAL append failure → do not advance manifest pointer, leave snapshot temp files for GC.
- Crash between WAL append and manifest edit → recovery replays the raft frames again; manifest pointer ensures idempotence since raft storage will see duplicate entries but keep the highest indices.
- Slow followers / large snapshots → keep placeholders for future throttling logic; this doc lists them under pending tests.

## Testing Plan
- **Double-write recovery**: start a single peer, propose entries, force a restart, run WAL replay, and validate `raftStorage`+NoKV state machine see identical data (enshrine in `raftstore/store_test.go`).
- **Manifest/WAL drift**: inject crash after WAL append but before manifest edit to ensure replay is idempotent and no WAL segment is reclaimed early.
- **Slow follower backlog (TODO)**: simulate follower lagging behind the WAL checkpoint; ensure segment GC waits for raft pointer advancement.
- **Snapshot resend (TODO)**: push Ready that includes snapshots and verify restart applies snapshot then incremental entries.

## Metrics & CLI Backlog
- Add WAL-facing raft counters: Ready queue depth, per-type WAL bytes, flush latency histogram.
- Surface raft pointer vs LSM pointer deltas in `stats.go` + `cmd/nokv stats --workdir`.
- Track snapshot creation/apply durations and expose via `/debug/vars` for future alerting.
