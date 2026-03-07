# Memtable Design & Lifecycle

NoKV's write path mirrors RocksDB: every write lands in the **WAL** and an in-memory **memtable** backed by a selectable in-memory index (skiplist or ART). The implementation lives in [`lsm/memtable.go`](../lsm/memtable.go) and ties directly into the flush manager (`lsm/flush`).

---

## 1. Structure

```go
type memTable struct {
    lsm        *LSM
    segmentID  uint32       // WAL segment backing this memtable
    index      memIndex
    maxVersion uint64
    walSize    int64
}
```

The memtable index is an interface that can be backed by either a skiplist or ART:

```go
type memIndex interface {
    Add(*kv.Entry)
    Search([]byte) ([]byte, kv.ValueStruct)
    NewIterator(*utils.Options) utils.Iterator
    MemSize() int64
    IncrRef()
    DecrRef()
}
```

* **Memtable engine** – `Options.MemTableEngine` selects `art` (default) or `skiplist` via `newMemIndex`. ART now uses a reversible mem-comparable route key so its trie ordering matches the LSM internal-key comparator; `skiplist` remains available as the simpler alternative.
* **Arena sizing** – both `utils.NewSkiplist` and `utils.NewART` use `arenaSizeFor` to derive arena capacity from `Options.MemTableSize`.
* **WAL coupling** – every `Set` uses `kv.EncodeEntry` to materialise the payload to the active WAL segment before inserting into the chosen index. `walSize` tracks how much of the segment is consumed so flush can release it later.
* **Segment ID** – `LSM.NewMemtable` atomically increments `levels.maxFID`, switches the WAL to a new segment (`wal.Manager.SwitchSegment`), and tags the memtable with that FID. This matches RocksDB's `logfile_number` field.
* **ART specifics** – ART stores prefix-compressed inner nodes (Node4/16/48/256), uses copy-on-write payload/node clones with CAS installs for writes, and keeps reads lock-free on immutable snapshots.

---

## 2. Lifecycle

```mermaid
sequenceDiagram
    participant WAL
    participant MT as MemTable
    participant Flush
    participant Manifest
    WAL->>MT: Append+Set(entry)
    MT->>Flush: freeze (walSize + incomingEstimate > limit)
    Flush->>Manifest: LogPointer + AddFile
    Manifest-->>Flush: ack
    Flush->>WAL: Release segments ≤ segmentID
```

1. **Active → Immutable** – when `mt.walSize + estimate` exceeds `Options.MemTableSize`, the memtable is rotated and pushed onto the flush queue. The new active memtable triggers another WAL segment switch.
2. **Flush** – the flush manager drains immutable memtables, builds SSTables, logs manifest edits, and releases the WAL segment ID recorded in `memTable.segmentID` once the SST is durably installed.
3. **Recovery** – `LSM.recovery` scans WAL files, reopens memtables per segment (most recent becomes active), and deletes segments ≤ the manifest's log pointer. Entries are replayed via `wal.Manager.ReplaySegment` into fresh indexes and the active in-memory state is rebuilt.

Badger follows the same pattern, while RocksDB often uses skiplist-backed arenas with reference counting—NoKV reuses Badger's arena allocator for simplicity.

---

## 3. Read Semantics

* `memTable.Get` looks up the chosen index and returns a borrowed, ref-counted `*kv.Entry` from the internal pool. The index search returns the **matched internal key** plus value struct, so memtable hit entries carry the concrete version key instead of the query sentinel key. Internal callers must release borrowed entries with `DecrRef` when done.
* `MemTable.IncrRef/DecrRef` delegate to the index, allowing iterators to hold references while the flush manager processes immutable tables—mirroring RocksDB's `MemTable::Ref/Unref` lifecycle.
* WAL-backed values that exceed the value threshold are stored as pointers; the memtable stores the encoded pointer, and the transaction/iterator logic reads from the vlog on demand.
* `DB.Get` returns detached entries; callers must not call `DecrRef` on them.
* `DB.GetInternalEntry` returns borrowed entries; callers must call `DecrRef` exactly once.

---

## 4. Integration with Other Subsystems

| Subsystem | Interaction |
| --- | --- |
| Distributed 2PC | `kv.Apply` + `percolator` write committed MVCC versions through the same WAL/memtable pipeline in raft mode. |
| Manifest | Flush completion logs `EditLogPointer(segmentID)` so restart can discard WAL files already persisted into SSTs. |
| Stats | `Stats.Snapshot` pulls `FlushPending/Active/Queue` counters via [`lsm.FlushMetrics`](../lsm/lsm.go#L120-L128), exposing how many immutables are waiting. |
| Value Log | `lsm.flush` emits discard stats keyed by `segmentID`, letting the value log GC know when entries become obsolete. |

---

## 5. Comparison

| Aspect | RocksDB | BadgerDB | NoKV |
| --- | --- | --- | --- |
| Data structure | Skiplist + arena | Skiplist + arena | Skiplist or ART + arena |
| WAL linkage | `logfile_number` per memtable | Segment ID stored in vlog entries | `segmentID` on `memTable`, logged via manifest |
| Recovery | Memtable replays from WAL, referencing `MANIFEST` | Replays WAL segments | Replays WAL segments, prunes ≤ manifest log pointer |
| Flush trigger | Size/entries/time | Size-based | WAL-size budget (`walSize`) with explicit queue metrics |

---

## 6. Operational Notes

* Tuning `Options.MemTableSize` affects WAL segment count and flush latency. Larger memtables reduce flush churn but increase crash recovery time.
* Monitor `NoKV.Stats.flush.*` fields to catch stalled immutables—an ever-growing queue often indicates slow SST builds or manifest contention.
* Because memtables carry WAL segment IDs, deleting WAL files manually can lead to recovery failures; always rely on the engine's manifest-driven cleanup.

See [`docs/flush.md`](flush.md) for the end-to-end flush scheduler and `[docs/architecture.md](architecture.md#3-end-to-end-write-flow)` for where memtables sit in the write pipeline.
