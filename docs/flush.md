# MemTable Flush Pipeline

NoKV's flush path converts immutable memtables into L0 SST files, then advances the manifest WAL checkpoint and reclaims obsolete WAL segments. The task scheduler is in [`lsm/flush`](../lsm/flush); SST persistence and manifest install are in [`lsm/builder.go`](../lsm/builder.go) and [`lsm/levels.go`](../lsm/levels.go).

---

## 1. Responsibilities

1. **Persistence**: materialize immutable memtables into SST files.
2. **Ordering**: publish SST metadata to manifest only after the SST is durably installed (strict mode).
3. **Cleanup**: remove WAL segments once checkpoint and raft constraints allow removal.
4. **Observability**: export queue/build/release timing through flush metrics.

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

- **StagePrepare**: `flush.Manager.Submit` enqueues task and records wait-start time.
- **StageBuild**: worker pulls task via `flush.Manager.Next`, builds SST (`levelManager.flush` -> `openTable` -> `tableBuilder.flush`).
- **StageInstall**: after SST + manifest edits succeed, worker marks install complete (`flush.Manager.Update(..., StageInstall, ...)`).
- **StageRelease**: worker removes immutable from in-memory list, closes memtable, records release metrics, and completes task.

---

## 3. SST Persistence Modes

Flush uses two write modes controlled by `Options.ManifestSync`:

1. **Fast path (`ManifestSync=false`)**
   - Writes SST directly to final filename with `O_CREATE|O_EXCL`.
   - No temp file/rename step.
   - Highest throughput, weaker crash-consistency guarantees.

2. **Strict path (`ManifestSync=true`)**
   - Writes to `"<table>.tmp.<pid>.<ns>"`.
   - `tmp.Sync()` to persist SST bytes.
   - `RenameNoReplace(tmp, final)` installs file atomically. If unsupported by platform/filesystem, returns `vfs.ErrRenameNoReplaceUnsupported`.
   - `SyncDir(workdir)` is called before manifest edit so directory entry is durable.

This is the durability ordering used by current code.

---

## 4. Execution Path in Code

1. `lsm.Set`/`lsm.SetBatch` detects `walSize + estimate > MemTableSize` and rotates memtable.
2. Rotated memtable is submitted to `flush.Manager` (`lsm.submitFlush`).
3. Worker executes `levelManager.flush(mt)`:
   - iterates memtable entries,
   - builds SST via `tableBuilder`,
   - prepares manifest edits: `EditAddFile` + `EditLogPointer`.
4. In strict mode, `SyncDir` runs before `manifest.LogEdits(...)`.
5. On successful manifest commit, table is added to L0 and `wal.RemoveSegment` runs when allowed.

---

## 5. Recovery Notes

- Startup rebuild (`levelManager.build`) validates manifest SST entries against disk.
- Missing or unreadable SSTs are treated as stale and removed from manifest via `EditDeleteFile`, allowing startup to continue.
- Temp SST names are only used in strict mode and are created in `WorkDir` with suffix `.tmp.<pid>.<ns>` (not a dedicated `tmp/` directory).

---

## 6. Metrics & CLI

`flush.Manager.Stats()` feeds `StatsSnapshot.Flush`:

- `pending`, `queue`, `active`
- wait/build/release totals, counts, last, max
- `completed`

Use:

```bash
nokv stats --workdir <dir>
```

to inspect flush backlog and latency.

---

## 7. Related Tests

- `lsm/flush/manager_test.go`: queue/stage transitions and timing counters.
- `db_test.go::TestRecoveryWALReplayRestoresData`: replay still restores data after crash before flush completion.
- `db_test.go::TestRecoveryCleansMissingSSTFromManifest` and `db_test.go::TestRecoveryCleansCorruptSSTFromManifest`: stale manifest SST cleanup on startup.

See also [recovery.md](recovery.md), [memtable.md](memtable.md), and [wal.md](wal.md).
