# Memtable Design & Lifecycle

NoKV's write path mirrors RocksDB: every write lands in the **WAL** and an in-memory **memtable** backed by a skiplist. The implementation lives in [`lsm/memtable.go`](../lsm/memtable.go) and ties directly into the flush manager (`lsm/flush`).

---

## 1. Structure

```go
type memTable struct {
    lsm        *LSM
    segmentID  uint32       // WAL segment backing this memtable
    sl         *utils.Skiplist
    buf        *bytes.Buffer
    maxVersion uint64
    walSize    int64
}
```

* **Skiplist arena** – `utils.NewSkiplist` allocates a pointer-friendly arena sized via [`arenaSizeFor`](../lsm/memtable.go#L23-L36). The arena doubles in size until it hits the configured `Options.MemTableSize`, similar to Badger's design.
* **WAL coupling** – every `Set` uses [`kv.EncodeEntry`](../lsm/memtable.go#L42-L59) to materialise the payload to the active WAL segment before inserting into the skiplist. `walSize` tracks how much of the segment is consumed so flush can release it later.
* **Segment ID** – [`LSM.NewMemtable`](../lsm/memtable.go#L38-L51) atomically increments `levels.maxFID`, switches the WAL to a new segment (`wal.Manager.SwitchSegment`), and tags the memtable with that FID. This matches RocksDB's `logfile_number` field.

---

## 2. Lifecycle

```mermaid
sequenceDiagram
    participant WAL
    participant MT as MemTable
    participant Flush
    participant Manifest
    WAL->>MT: Append+Set(entry)
    MT->>Flush: freeze (Size() >= limit)
    Flush->>Manifest: LogPointer + AddFile
    Manifest-->>Flush: ack
    Flush->>WAL: Release segments ≤ segmentID
```

1. **Active → Immutable** – when `mt.Size()` crosses thresholds (`Options.MemTableSize`), the memtable is swapped out and pushed onto the flush queue. The new active memtable triggers another WAL segment switch.
2. **Flush** – the flush manager drains immutable memtables, builds SSTables, logs manifest edits, and releases the WAL segment ID recorded in `memTable.segmentID` once the SST is durably installed.
3. **Recovery** – [`LSM.recovery`](../lsm/memtable.go#L58-L116) scans WAL files, reopens memtables per segment (most recent becomes active), and deletes segments ≤ the manifest's log pointer. Entries are replayed via [`wal.Manager.ReplaySegment`](../lsm/memtable.go#L116-L149) into fresh skiplists, rebuilding `maxVersion` for MVCC.

Badger follows the same pattern, while RocksDB often uses skiplist-backed arenas with reference counting—NoKV reuses Badger's arena allocator for simplicity.

---

## 3. Read Semantics

* `memTable.Get` looks up the skiplist and returns a copy of the entry. MVCC versions stay encoded in the key suffix (`KeyWithTs`), so iterators naturally merge across memtables and SSTables.
* `MemTable.IncrRef/DecrRef` delegate to the skiplist, allowing iterators to hold references while the flush manager processes immutable tables—mirroring RocksDB's `MemTable::Ref/Unref` lifecycle.
* WAL-backed values that exceed the value threshold are stored as pointers; the skiplist stores the encoded pointer, and the transaction/iterator logic reads from the vlog on demand.

---

## 4. Integration with Other Subsystems

| Subsystem | Interaction |
| --- | --- |
| Transactions | `Txn.commitAndSend` writes entries into the active memtable after WAL append; pending writes bypass the memtable until commit so per-txn isolation is preserved. |
| Manifest | Flush completion logs `EditLogPointer(segmentID)` so restart can discard WAL files already persisted into SSTs. |
| Stats | `Stats.Snapshot` pulls `FlushPending/Active/Queue` counters via [`lsm.FlushMetrics`](../lsm/lsm.go#L120-L128), exposing how many immutables are waiting. |
| Value Log | `lsm.flush` emits discard stats keyed by `segmentID`, letting the value log GC know when entries become obsolete. |

---

## 5. Comparison

| Aspect | RocksDB | BadgerDB | NoKV |
| --- | --- | --- | --- |
| Data structure | Skiplist + arena | Skiplist + arena | Skiplist + arena (`utils.Skiplist`) |
| WAL linkage | `logfile_number` per memtable | Segment ID stored in vlog entries | `segmentID` on `memTable`, logged via manifest |
| Recovery | Memtable replays from WAL, referencing `MANIFEST` | Replays WAL segments | Replays WAL segments, prunes ≤ manifest log pointer |
| Flush trigger | Size/entries/time | Size-based | Size-based with explicit queue metrics |

---

## 6. Operational Notes

* Tuning `Options.MemTableSize` affects WAL segment count and flush latency. Larger memtables reduce flush churn but increase crash recovery time.
* Monitor `NoKV.Stats.Flush.*` metrics to catch stalled immutables—an ever-growing queue often indicates slow SST builds or manifest contention.
* Because memtables carry WAL segment IDs, deleting WAL files manually can lead to recovery failures; always rely on the engine's manifest-driven cleanup.

See [`docs/flush.md`](flush.md) for the end-to-end flush scheduler and `[docs/architecture.md](architecture.md#3-end-to-end-write-flow)` for where memtables sit in the write pipeline.
