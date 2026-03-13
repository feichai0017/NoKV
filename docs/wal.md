# WAL Subsystem

NoKV's write-ahead log mirrors RocksDB's durability model and is implemented as a compact Go module similar to Badger's journal. WAL appends happen alongside memtable writes (via `lsm.SetBatch` in the DB write pipeline), while values that are routed to the value log are written *before* the WAL so that replay always sees durable value pointers.

---

## 1. File Layout & Naming

- Location: `${Options.WorkDir}/wal/`.
- Naming pattern: `%05d.wal` (e.g. `00001.wal`).
- Rotation threshold: configurable via `wal.Config.SegmentSize` (defaults to 64 MiB, minimum 64 KiB).
- Verification: `wal.VerifyDir` ensures the directory exists prior to `DB.Open`.

On open, `wal.Manager` scans the directory, sorts segment IDs, and resumes the highest ID—exactly how RocksDB re-opens its MANIFEST and WAL sequence files.

---

## 2. Record Format

```text
uint32 length (big-endian, includes type + payload)
uint8  type
[]byte payload
uint32 checksum (CRC32 Castagnoli over type + payload)
```

- Checksums use `kv.CastagnoliCrcTable`, the same polynomial used by RocksDB (Castagnoli). Record encoding/decoding lives in `wal/record.go`.
- The type byte allows mixing LSM mutations with raft log/state/snapshot records in the same WAL segment.
- Appends are buffered by `bufio.Writer` so batches become single system calls.
- Replay stops cleanly at truncated tails; tests simulate torn writes by truncating the final bytes and verifying replay remains idempotent (`wal/manager_test.go::TestManagerReplayHandlesTruncate`).

---

## 3. Public API (Go)

```go
mgr, _ := wal.Open(wal.Config{Dir: path})
info, _ := mgr.AppendEntry(entry)
batchInfo, _ := mgr.AppendEntryBatch(entries)
typedInfos, _ := mgr.AppendRecords(wal.Record{
    Type:    wal.RecordTypeRaftEntry,
    Payload: raftPayload,
})
_ = mgr.Sync()
_ = mgr.Rotate()
_ = mgr.Replay(func(info wal.EntryInfo, payload []byte) error {
    // reapply to memtable
    return nil
})
```

Key behaviours:
- `AppendEntry`/`AppendEntryBatch`/`AppendRecords` automatically call `ensureCapacity` to decide when to rotate; they return `EntryInfo{SegmentID, Offset, Length}` so upper layers can keep storage WAL checkpoints and store-local raft replay pointers.
- `Sync` flushes the active file (used for `Options.SyncWrites`).
- `Rotate` forces a new segment (used after flush/compaction checkpoints similar to RocksDB's `LogFileManager::SwitchLog`).
- `Replay` iterates segments in numeric order, forwarding each payload to the callback. Errors abort replay so recovery can surface corruption early.
- Metrics (`wal.Manager.Metrics`) reveal the active segment ID, total segments, and number of removed segments—these feed directly into `StatsSnapshot` and `nokv stats` output.

Compared with Badger: Badger keeps a single vlog for both data and durability. NoKV splits WAL (durability) from vlog (value separation), matching RocksDB's separation of WAL and blob files.

---

## 4. Integration Points

| Call Site | Purpose |
| --- | --- |
| `lsm.memTable.setBatch` | Encodes each entry (`kv.EncodeEntry`) and appends to WAL before inserting into the active memtable index (`ART` by default, `skiplist` when explicitly selected). |
| `DB.commitWorker` | Commit worker applies batched writes via `writeToLSM`, which calls `lsm.SetBatch` and appends one WAL entry-batch record per request batch. |
| `DB.Set` / `DB.SetBatch` / `DB.SetWithTTL` / `DB.Del` / `DB.DeleteRange` / `DB.ApplyInternalEntries` | User/internal writes all flow through the same commit queue and eventually reach `lsm.SetBatch` + WAL append. |
| `lsm/levels.go::flush` | Persists WAL checkpoint via `manifest.LogEdits(EditAddFile, EditLogPointer)` during flush install. |
| `lsm/levels.go::flush` + `lsm/levelsRuntime.canRemoveWalSegment` | Removes obsolete WAL segments after storage checkpoint and `raftstore/meta` replay constraints are satisfied. |
| `db.runRecoveryChecks` | Ensures WAL directory invariants before manifest replay, similar to Badger's directory bootstrap. |

---

## 5. Metrics & Observability

`Stats.collect` reads the manager metrics and exposes them as:
- `NoKV.Stats.wal.active_segment`
- `NoKV.Stats.wal.segment_count`
- `NoKV.Stats.wal.segments_removed`

The CLI command `nokv stats --workdir <dir>` prints these alongside backlog, making WAL health visible without manual inspection. In high-throughput scenarios the active segment ID mirrors RocksDB's `LOG` number growth.

---

## 6. WAL Watchdog (Auto GC)

The WAL watchdog runs inside the DB process to keep WAL backlog in check and
surface warnings when raft-typed records dominate the log. It:

- Samples WAL metrics + per-segment metrics and combines them with
  `raftstore/meta` raft pointer snapshots to compute removable segments.
- Removes up to `WALAutoGCMaxBatch` segments when at least
  `WALAutoGCMinRemovable` are eligible.
- Exposes counters (`wal.auto_gc_runs/removed/last_unix`) and warning state
  (`wal.typed_record_ratio/warning/reason`) through `StatsSnapshot.WAL`.

Relevant options (see `options.go` for defaults):
- `EnableWALWatchdog`
- `WALAutoGCInterval`
- `WALAutoGCMinRemovable`
- `WALAutoGCMaxBatch`
- `WALTypedRecordWarnRatio`
- `WALTypedRecordWarnSegments`

---

## 7. Recovery Walkthrough

1. `wal.Open` reopens the highest segment, leaving the file pointer at the end (`switchSegmentLocked`).
2. `manifest.Manager` supplies the WAL checkpoint (segment + offset) while building the version. Replay skips entries up to this checkpoint, ensuring we only reapply writes not yet materialised in SSTables.
3. `wal.Manager.Replay` (invoked by the LSM recovery path) rebuilds memtables from entries newer than the manifest checkpoint. Value-log recovery only validates/truncates segments and does not reapply data.
4. If the final record is partially written, the CRC mismatch stops replay and the segment is truncated during recovery tests, mimicking RocksDB's tolerant behaviour.

---

## 8. Operational Tips

- Configure `Options.SyncWrites=true` for synchronous durability (default async, similar to RocksDB's default).
- After large flushes, forcing `Rotate` keeps WAL files short, reducing replay time.
- Archived WAL segments can be copied alongside manifest files for hot-backup strategies—since the manifest contains the WAL log number, snapshots behave like RocksDB's `Checkpoints`.

---

## 9. Truncation Metadata

- `raftstore/engine/wal_storage` keeps a per-group index of `[firstIndex,lastIndex]` spans for each WAL record so it can map raft log indices back to the segment that stored them.
- When a log is truncated (either via snapshot or future compaction hooks), the store-local metadata in `raftstore/meta` is updated with the index/term, segment ID (`RaftLogPointer.SegmentIndex`), and byte offset (`RaftLogPointer.TruncatedOffset`) that delimit the remaining WAL data.
- `lsm/levelsRuntime.canRemoveWalSegment` blocks garbage collection whenever any raft group still references a segment through that store-local truncation metadata, preventing slow followers from losing required WAL history while letting aggressively compacted groups release older segments earlier.

For broader context, read the [architecture overview](architecture.md) and [flush pipeline](flush.md) documents.
