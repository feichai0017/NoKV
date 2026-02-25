# MemTable Flush Pipeline

NoKV's flush subsystem translates immutable memtables into persisted SSTables while coordinating WAL checkpoints and ValueLog discard statistics. The code lives in [`lsm/flush`](../lsm/flush) and is tightly integrated with `DB.doWrites` and `manifest.Manager`.

---

## 1. Responsibilities

1. **Reliability** – ensure immutables become SSTables atomically, and failures are recoverable.
2. **Coordination** – release WAL segments only after manifest commits, and feed discard stats to ValueLog GC.
3. **Observability** – expose queue depth, stage durations, and task counts through `Stats.collect` and the CLI.

Compared with RocksDB: the stage transitions mirror RocksDB's flush job lifecycle (`PickMemTable`, `WriteLevel0Table`, `InstallMemTable`), while the discard stats channel is inspired by Badger's integration with vlog GC.

---

## 2. Stage Machine

```mermaid
flowchart LR
    Active[Active MemTable]
    Immutable[Immutable MemTable]
    FlushQ[flush.Manager queue]
    Build[StageBuild]
    Install[StageInstall]
    Release[StageRelease]

    Active -->|threshold reached| Immutable --> FlushQ
    FlushQ --> Build --> Install --> Release --> Active
```

- **StagePrepare** – `Manager.Submit` assigns a task ID, records enqueue time, and bumps queue metrics.
- **StageBuild** – `Manager.Next` hands tasks to background workers. `buildTable` serialises data into a temporary `.sst.tmp` using `lsm/builder.go`.
- **StageInstall** – manifest edits (`EditAddFile`, `EditLogPointer`) are logged. Only on success is the temp file renamed and the WAL checkpoint advanced.
- **StageRelease** – metrics record release duration, discard stats are flushed to `valueLog.lfDiscardStats`, and `wal.Manager.Remove` drops obsolete segments.

`Manager.Update` transitions between stages and collects timing data (`WaitNs`, `BuildNs`, `ReleaseNs`). These surface via `StatsSnapshot.Flush` fields (for example `QueueLength`, `BuildMs`) in `nokv stats` output.

---

## 3. Key Types

```go
type Task struct {
    ID        uint64
    SegmentID uint32
    Stage     Stage
    Data      any      // memtable pointer, temp file info, etc.
    Err       error
}

type Manager struct {
    queue []*Task
    active map[uint64]*Task
    cond  *sync.Cond
    // atomic metrics fields (pending, queueLen, waitNs...)
}
```

- `Stage` enumerates `StagePrepare`, `StageBuild`, `StageInstall`, `StageRelease`.
- `Metrics` aggregates pending/active counts and nanosecond accumulators; the CLI converts them to human-friendly durations.
- The queue uses condition variables to coordinate between background workers and producers; the design avoids busy waiting, unlike some RocksDB flush queues.

---

## 4. Execution Path in Code

1. `DB.applyBatches` detects when the active memtable is full and hands it to `lsm.LSM.scheduleFlush`, which calls `flush.Manager.Submit`.
2. Background goroutines call `Next` to retrieve tasks; `lsm.(*LSM).runFlushMemTable` performs the build and install phases.
3. `lsm.(*LSM).installLevel0Table` writes the manifest edit and renames the SST (atomic `os.Rename`, same as RocksDB's flush job).
4. After install, `valueLog.updateDiscardStats` is called so GC can reclaim vlog entries belonging to dropped keys.
5. Once release completes, `wal.Manager.Remove` evicts segments whose entries are fully represented in SSTs, matching RocksDB's `LogFileManager::PurgeObsoleteLogs`.

---

## 5. Recovery Considerations

- **Before Install** – temp files remain in `tmp/`. On restart, no manifest entry exists, so `lsm.LSM.replayManifest` ignores them and the memtable is rebuilt from WAL.
- **After Install but before Release** – manifest records the SST while WAL segments may still exist. Recovery sees the edit, ensures the file exists, and release metrics resume from StageRelease.
- **Metrics** – because timing data is stored atomically in the manager, recovery resets counters but does not prevent the CLI from reporting backlog immediately after restart.

RocksDB uses flush job logs; NoKV reuses metrics and CLI output for similar visibility.

---

## 6. Observability & CLI

- `StatsSnapshot.Flush.QueueLength` – number of pending tasks.
- `StatsSnapshot.Flush.WaitMs` – average wait time before build.
- `StatsSnapshot.Flush.BuildMs` – average build duration.
- `StatsSnapshot.Flush.Completed` – cumulative tasks finished.

The CLI command `nokv stats --workdir <dir>` prints these metrics alongside compaction and transaction statistics, enabling operators to detect stalled flush workers or WAL backlog quickly.

---

## 7. Interplay with ValueLog GC

Flush completion sends discard stats via `db.lsm.SetDiscardStatsCh(&(db.vlog.lfDiscardStats.flushChan))`. ValueLog GC uses this feed to determine how much of each vlog segment is obsolete, similar to Badger's discard ratio heuristic. Without flush-driven stats, vlog GC would have to rescan SSTables, so this channel is crucial for keeping GC cheap.

---

## 8. Testing Matrix

- `lsm/flush/manager_test.go` (implicit via `lsm/lsm_test.go`) validates stage transitions and metrics.
- `db_test.go` covers crash scenarios before/after install, ensuring WAL replay plus manifest reconciliation recovers gracefully.
- Future additions: inject write failures during `StageBuild` to test retry logic, analogous to RocksDB's simulated IO errors.

See the [recovery plan](recovery.md) and [testing matrix](testing.md) for more context.
