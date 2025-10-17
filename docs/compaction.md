# Compaction & Cache Strategy

> NoKV’s compaction pipeline borrows the leveled‑LSM layout from RocksDB, but layers it with an ingest buffer, lightweight cache telemetry, and simple concurrency guards so the implementation stays approachable while still handling bursty workloads.

---

## 1. Overview

Compactions are orchestrated by `lsm.levelManager`.  Each level owns two lists of tables:

- `tables` – the canonical sorted run for the level.
- `ingest` – a staging buffer that temporarily holds SSTables moved from the level above when there is not yet enough work (or bandwidth) to do a full merge.

The compaction manager periodically calls `pickCompactLevels()` to build a list of `compactionPriority` entries.  The priorities consider three signals:

1. **L0 table count** – loosely capped by `Options.NumLevelZeroTables`.
2. **Level size vs target** – computed by `levelTargets()`, which dynamically adjusts the “base” level depending on total data volume.
3. **Ingest buffer backlog** – if a level’s `ingest` slice has data, it receives an elevated score so the staged tables are merged promptly.

The highest adjusted score is processed first.  L0 compactions can either move tables into the ingest buffer of the base level (cheap re‑parenting) or compact directly into a lower level when the overlap warrants it.

---

## 2. Ingest Buffer

`moveToIngest` (see `lsm/compact.go`) performs a metadata-only migration:

1. Records a `manifest.EditDeleteFile` for the source level.
2. Logs a new `manifest.EditAddFile` targeting the destination level.
3. Removes the table from `thisLevel.tables` and appends it to `nextLevel.ingest`.

This keeps write amplification low when many small L0 tables arrive at once.  Reads still see the newest data because `levelHandler.searchIngestSST` checks `ingest` before consulting `tables`.

Compaction tests (`lsm/compaction_cache_test.go`) now assert that after calling `moveToIngest` the table disappears from the source level and shows up in the ingest buffer.

---

## 3. Concurrency Guards

To prevent overlapping compactions:

- `compactStatus.compareAndAdd` tracks the key range of each in-flight compaction per level.
- Attempts to register a compaction whose ranges intersect an existing one are rejected.
- When a compaction finishes, `compactStatus.delete` removes the ranges and table IDs from the guard.

This mechanism is intentionally simple—just a mutex‐protected slice—yet effective in tests (`TestCompactStatusGuards`) that simulate back‑to‑back registrations on the same key range.

---

## 4. Cache Telemetry

NoKV’s cache is split into three parts (`lsm/cache.go`):

| Component | Purpose | Metrics hook |
| --- | --- | --- |
| Block cache (hot) | LRU list capturing most recent hits (typically L0/L1). | `cacheMetrics.recordBlock(level, hit)` |
| Block cache (cold) | CLOCK cache for deeper levels, keeping the memory footprint bounded. | Same as above |
| Bloom cache | Stores decoded bloom filters to reduce disk touches. | `recordBloom(hit)` |

`CacheMetrics()` on `DB` surfaces hits/misses per layer, which is especially helpful when tuning ingest behaviour—if L0/L1 cache misses spike, the ingest buffer likely needs to be drained faster.  `TestCacheHotColdMetrics` verifies that the hot and cold tiers tick the counters as expected.

---

## 5. Interaction with Value Log

Compaction informs value‑log GC via discard statistics:

1. During `subcompact`, every entry merged out is inspected.  If it stores a `ValuePtr`, the amount is added to the discard map.
2. At the end of subcompaction, the accumulated discard map is pushed through `setDiscardStatsCh`.
3. `valueLog` receives the stats and can safely rewrite or delete vlog segments with predominantly obsolete data.

This tight coupling keeps the value log from growing indefinitely after heavy overwrite workloads.

---

## 6. Testing Checklist

Relevant tests to keep compaction healthy:

- `lsm/compaction_cache_test.go`
  - `TestCompactionMoveToIngest` – ensures metadata migration works and the ingest buffer grows.
  - `TestCacheHotColdMetrics` – validates cache hit accounting.
  - `TestCompactStatusGuards` – checks overlap detection.
- `lsm/lsm_test.go`
  - `TestCompact` / `TestHitStorage` – end‑to‑end verification that data remains queryable across memtable flushes and compactions.

When adding new compaction heuristics or cache tiers, extend these tests (or introduce new ones) so the behaviour stays observable.

---

## 7. Practical Tips

- Tune `Options.IngestCompactBatchSize` when ingest queues build up; increasing it lets a single move cover more tables.
- Observe `DB.CacheMetrics()` and `DB.CompactionStats()` via the CLI (`nokv stats`) to decide whether you need more compaction workers or bigger caches.
- For workloads dominated by range scans, raise `Options.BlockCacheHotFraction` so the hot LRU tier holds more blocks from L0/L1.
- Keep an eye on `NoKV.ValueLog.GcRuns` and `ValueLogHeadUpdates`; if compactions are generating discard stats but the value log head doesn’t move, GC thresholds may be too conservative.

With these mechanisms, NoKV stays resilient under bursty writes while keeping the code path small and discoverable—ideal for learning or embedding.  Dive into the source files referenced above for deeper implementation details.

