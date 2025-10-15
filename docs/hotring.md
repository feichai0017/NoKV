# HotRing – Hot Key Tracking

`hotring` is NoKV's built-in hot-key tracker. It samples read/write access frequency per key and exposes the hottest entries to the stats subsystem and CLI. The implementation resides in [`hotring/`](../hotring).

---

## 1. Motivation

* **Cache hints** – `DB.prefetchLoop` (see [`db.go`](../db.go)) consumes hot keys to schedule asynchronous reads into the block cache.
* **Operational insight** – `StatsSnapshot.HotKeys` and `nokv stats --json` surface the hottest keys, aiding debugging of traffic hotspots.
* **Throttling** – `HotRing.TouchAndClamp` enables simple rate caps: once a key crosses a threshold, callers can back off or log alerts.

Compared with RocksDB (which exposes block access stats via `perf_context`) and Badger (which lacks built-in hot-key reporting), NoKV offers a lightweight but concurrent-friendly tracker out of the box.

---

## 2. Data Structure

```text
HotRing
  tables[] -> per-bucket circular linked list (Node)
  hashFn   -> hash(key) -> uint32
  hashMask -> selects bucket (power of two size)
```

* Each bucket stores a circular linked list of [`Node`](../hotring/node.go#L9-L66) sorted by `(tag, key)`, where `tag` is derived from the upper bits of the hash. This keeps insertion/search O(bucket_length).
* `defaultTableBits = 12` → 4096 buckets by default (`NewHotRing`). The mask ensures cheap modulo operations.
* Nodes keep a `count` (int32) updated atomically, plus `next`/`head` pointers using `unsafe.Pointer` to avoid additional allocations.

```mermaid
flowchart LR
    Key(key) -->|hash| Bucket[table[index]]
    Bucket --> Node1
    Node1 --> Node2
    Node2 --> Node3
    Node3 --> Node1
```

---

## 3. Core Operations

| Method | Behaviour | Notes |
| --- | --- | --- |
| [`Touch`](../hotring/hotring.go#L39-L66) | Insert or increment key's counter. | Creates node on miss, resets counter to 0 then increments. |
| [`Frequency`](../hotring/hotring.go#L68-L84) | Read-only counter lookup. | No side effects; uses `RLock`. |
| [`TouchAndClamp`](../hotring/hotring.go#L86-L125) | Increment unless `count >= limit`, returning `(count, limited)`. | Useful for throttling heavy keys. |
| [`TopN`](../hotring/hotring.go#L149-L181) | Snapshot hottest keys sorted by count desc. | Clones slice to avoid exposing internal pointers. |
| [`KeysAbove`](../hotring/hotring.go#L182-L212) | Return all keys with counters ≥ threshold. | Supports targeted throttling.

The internal helpers [`searchLocked`](../hotring/hotring.go#L214-L259) and [`insertLocked`](../hotring/hotring.go#L261-L306) enforce bucket ordering and deduplicate keys.

Concurrency is handled via a global `RWMutex`; buckets are small enough that the coarse lock keeps overhead modest. The per-node counters use `atomic.AddInt32` so concurrent touches within the same bucket remain safe.

---

## 4. Integration Points

* **DB reads** – `Txn.Get` and iterators call `db.recordRead`, which in turn invokes `HotRing.Touch` for every successful lookup. Writes can also touch the ring if configured (see `Options.TrackWrites` in `db.go`).
* **Stats** – [`StatsSnapshot`](../stats.go#L41-L87) copies `hot.TopN` into `HotKeys`. `expvar` publishes the same view under `NoKV.Stats.HotKeys` for automation.
* **Caching** – `lsm/cache` can promote blocks referenced by frequently touched keys, keeping the hot tier warm.

---

## 5. Comparisons

| Engine | Approach |
| --- | --- |
| RocksDB | External – `TRACE` / `perf_context` requires manual sampling. |
| Badger | None built-in. |
| NoKV | In-process ring with expvar/CLI export and throttling helpers. |

The HotRing emphasises simplicity: no approximate counting sketches, just per-bucket rings adequate for hundreds of thousands of keys while keeping allocations minimal.

---

## 6. Operational Tips

* `Options.HotRingTopK` controls how many keys show up in stats; default 16. Increase it when investigating workloads with broad hot sets.
* Combine `TouchAndClamp` with request middleware to detect abusive tenants: when `limited` is true, log the key and latency impact.
* Resetting the ring is as simple as instantiating a new `HotRing`—useful for benchmarks that require clean counters between phases.

For end-to-end examples see [`docs/stats.md`](stats.md#hot-key-export) and the CLI walkthrough in [`docs/cli.md`](cli.md#hot-key-output).
