# WAL Subsystem

NoKV's write-ahead log mirrors RocksDB's durability model and is implemented as a compact Go module similar to Badger's journal. WAL appends happen alongside memtable writes (via `lsm.Set`), while values that are routed to the value log are written *before* the WAL so that replay always sees durable value pointers.

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
uint32 length (big-endian)
[]byte payload (encoded Entry batch)
uint32 checksum (CRC32 Castagnoli)
```

- Checksums use `kv.CastagnoliCrcTable`, the same polynomial used by RocksDB (Castagnoli).
- Appends are buffered by `bufio.Writer` so batches become single system calls.
- Replay stops cleanly at truncated tails; tests simulate torn writes by truncating the final bytes and verifying replay remains idempotent (`wal/manager_test.go::TestReplayTruncatedTail`).

---

## 3. Public API (Go)

```go
mgr, _ := wal.Open(wal.Config{Dir: path})
infos, _ := mgr.Append(batchPayload)
_ = mgr.Sync()
_ = mgr.Rotate()
_ = mgr.Replay(func(info wal.EntryInfo, payload []byte) error {
    // reapply to memtable
    return nil
})
```

Key behaviours:
- `Append` automatically calls `ensureCapacity` to decide when to rotate; it returns `EntryInfo{SegmentID, Offset, Length}` for each payload so higher layers can build value pointers or manifest checkpoints.
- `Sync` flushes the active file (used for `Options.SyncWrites`).
- `Rotate` forces a new segment (used after flush/compaction checkpoints similar to RocksDB's `LogFileManager::SwitchLog`).
- `Replay` iterates segments in numeric order, forwarding each payload to the callback. Errors abort replay so recovery can surface corruption early.
- Metrics (`wal.Manager.Metrics`) reveal the active segment ID, total segments, and number of removed segments—these feed directly into `StatsSnapshot` and `nokv stats` output.

Compared with Badger: Badger keeps a single vlog for both data and durability. NoKV splits WAL (durability) from vlog (value separation), matching RocksDB's separation of WAL and blob files.

---

## 4. Integration Points

| Call Site | Purpose |
| --- | --- |
| `lsm.memTable.set` | Encodes each entry (`kv.EncodeEntry`) and appends to WAL before inserting into the skiplist. |
| `DB.commitApplyWorker(s)` | Apply workers use `writeToLSM` to apply batched writes, which flow into `lsm.Set` and thus WAL. |
| `DB.Set` | Direct write path: calls `lsm.Set`, which appends to WAL and updates the memtable. |
| `manifest.Manager.LogEdit` | Uses `EntryInfo.SegmentID` to persist the WAL checkpoint (`EditLogPointer`). This acts as the `log number` seen in RocksDB manifest entries. |
| `lsm/flush.Manager.Update` | Once an SST is installed, WAL segments older than the checkpoint are released (`wal.Manager.Remove`). |
| `db.runRecoveryChecks` | Ensures WAL directory invariants before manifest replay, similar to Badger's directory bootstrap. |

---

## 5. Metrics & Observability

`Stats.collect` reads the manager metrics and exposes them as:
- `NoKV.WAL.ActiveSegment`
- `NoKV.WAL.SegmentCount`
- `NoKV.WAL.RemovedSegments`

The CLI command `nokv stats --workdir <dir>` prints these alongside backlog, making WAL health visible without manual inspection. In high-throughput scenarios the active segment ID mirrors RocksDB's `LOG` number growth.

---

## 6. Recovery Walkthrough

1. `wal.Open` reopens the highest segment, leaving the file pointer at the end (`switchSegmentLocked`).
2. `manifest.Manager` supplies the WAL checkpoint (segment + offset) while building the version. Replay skips entries up to this checkpoint, ensuring we only reapply writes not yet materialised in SSTables.
3. `wal.Manager.Replay` feeds payloads into `DB.replayFunction`, which repopulates memtables and the value log head—mirroring RocksDB's `DBImpl::RecoverLogFile`.
4. If the final record is partially written, the CRC mismatch stops replay and the segment is truncated during recovery tests, mimicking RocksDB's tolerant behaviour.

---

## 7. Operational Tips

- Configure `SyncOnWrite` for synchronous durability (default async like RocksDB's default). For latency-sensitive deployments, consider enabling to emulate Badger's `SyncWrites`.
- After large flushes, forcing `Rotate` keeps WAL files short, reducing replay time.
- Archived WAL segments can be copied alongside manifest files for hot-backup strategies—since the manifest contains the WAL log number, snapshots behave like RocksDB's `Checkpoints`.

---

## 8. Truncation Metadata

- `raftstore/engine/wal_storage` keeps a per-group index of `[firstIndex,lastIndex]` spans for each WAL record so it can map raft log indices back to the segment that stored them.
- When a log is truncated (either via snapshot or future compaction hooks), the manifest is updated via `LogRaftTruncate` with the index/term, segment ID (`RaftLogPointer.SegmentIndex`), and byte offset (`RaftLogPointer.TruncatedOffset`) that delimit the remaining WAL data.
- `lsm/levelManager.canRemoveWalSegment` now blocks garbage collection whenever any raft group still references a segment through its truncation metadata, preventing slow followers from losing required WAL history while letting aggressively compacted groups release older segments earlier.

For broader context, read the [architecture overview](architecture.md) and [flush pipeline](flush.md) documents.
