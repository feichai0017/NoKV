# Range Filter

NoKV's range filter is a read-path pruning layer for the LSM tree. It is inspired by the paper [GRF: A Global Range Filter for LSM-Trees with Shape Encoding](https://people.iiis.tsinghua.edu.cn/~huanchen/publications/grf-sigmod24.pdf), but it is deliberately more conservative and much simpler.

The current implementation is best described as:

- in-memory only
- correctness-first
- advisory pruning, not a source of truth
- table-level pruning plus table-internal block-range pruning

It is designed to reduce unnecessary SST and block probes on point lookups and narrow bounded scans without changing recovery or manifest semantics.

---

## 1. Goals

The range filter exists to cut down the part of the read path that answers:

- which SSTs are even worth probing
- which block range inside a chosen SST is even worth scanning

This is most valuable for:

- point misses
- point hits on non-overlapping levels with many SSTs
- narrow bounded iterators

It is not intended to change write behavior, compaction policy, or persistence format.

---

## 2. Inspiration

The design direction comes from the SIGMOD 2024 GRF paper:

- [GRF: A Global Range Filter for LSM-Trees with Shape Encoding](https://people.iiis.tsinghua.edu.cn/~huanchen/publications/grf-sigmod24.pdf)

The paper's main idea is stronger than what NoKV currently implements:

- a true global range filter
- shape encoding tied to LSM shape and run IDs
- version/snapshot-aware maintenance
- maintenance rules that assume more deterministic compaction behavior

NoKV does **not** implement full GRF or Shape Encoding. The current design only borrows the high-level idea:

- do pruning before expensive per-run / per-table probes
- keep the pruning layer global enough to matter
- preserve correctness by falling back when uncertain

---

## 3. Current Design

### 3.1 Per-level table spans

Each `levelHandler` maintains a `rangeFilter` built from its current table set:

- source: [`lsm/range_filter.go`](../lsm/range_filter.go)
- owner: [`lsm/level_handler.go`](../lsm/level_handler.go)

Each span records:

- minimum base key (`CF + user key`)
- maximum base key (`CF + user key`)
- table pointer

For non-overlapping levels, the filter supports binary-search candidate lookup. For overlapping or small levels, it falls back to a conservative linear filter.

The implementation deliberately works on base-key semantics rather than pure
user-key semantics so different column families are never collapsed into the
same pruning range.

### 3.2 Point-read pruning

Point lookups first prune candidate tables:

- [`lsm/level_handler.go`](../lsm/level_handler.go)

If a non-overlapping level collapses to a single exact candidate, NoKV uses a thinner point-read path:

- [`lsm/table.go`](../lsm/table.go)

This avoids the heavier generic iterator-style path for the common "one table could contain this key" case.

### 3.3 Bounded iterator pruning

Bounded iterators use the same per-level filter to prune whole tables before iterator assembly:

- [`lsm/level_handler.go`](../lsm/level_handler.go)

Inside a table, NoKV then uses SST block base keys from the decoded table index to shrink the block range that the iterator needs to touch:

- [`lsm/table.go`](../lsm/table.go)

This is the current "table-internal" stage of the design.

### 3.4 Rebuild behavior

The filter is rebuilt when a level's table set changes, for example after:

- sort/install
- compaction replacement
- table deletion

The filter is not updated on every write. It is rebuilt on table-set changes under the existing level ownership rules.

---

## 4. Correctness Model

The range filter is intentionally **not authoritative**.

Rules:

- if the filter is unsure, fall back
- if the level overlaps, fall back
- if the level is too small to amortize pruning overhead, fall back
- never allow false-negative pruning

This is why the design is safe to deploy without changing startup, manifest, or recovery behavior.

`L0` is handled conservatively:

- it remains overlap-first
- it does not use the non-overlapping exact-candidate fast path

That choice trades peak possible speed for simpler correctness.

---

## 5. What This Is Not

This document describes a practical NoKV optimization, not a claim of full GRF compatibility.

NoKV currently does **not** implement:

- Shape Encoding
- a persisted global range filter
- run-ID encoding
- snapshot/version-aware filter state
- compaction-policy constraints required by full GRF

This is deliberate. Those features would couple the filter much more tightly to compaction scheduling, version management, and recovery semantics.

---

## 6. Observability

Range-filter behavior is exported through LSM diagnostics and top-level stats:

- [`lsm/diagnostics.go`](../lsm/diagnostics.go)
- [`stats.go`](../stats.go)

Current counters include:

- `PointCandidates`
- `PointPruned`
- `BoundedCandidates`
- `BoundedPruned`
- `Fallbacks`

These counters are useful for deciding whether a workload is actually benefiting from pruning or mostly falling back.

---

## 7. Performance Notes

Microbenchmarks show strong gains when candidate-pruning is the dominant cost:

- point misses
- point hits on many-table non-overlapping levels
- narrow bounded scans

System-level YCSB behavior is more nuanced:

- read-heavy workloads benefit more clearly
- mixed read/write workloads depend more on L0 shape, block loads, flush, and compaction pressure

This is expected. The range filter optimizes query planning and table/block selection. It does not remove the rest of the read path.

---

## 8. Why NoKV Stops Short of Full GRF

Full paper alignment is not the current goal.

Reasons:

1. NoKV's current compaction and level behavior does not naturally satisfy the paper's stronger deterministic requirements.
2. A persisted global filter would add metadata, rebuild, and recovery complexity.
3. The current implementation already captures the most practical early win:
   - prune whole tables first
   - then prune block ranges inside the chosen table
4. Current bottlenecks for read-heavy workloads are now more visible in:
   - `L0` overlap handling
   - block loading
   - remaining table probe cost

For NoKV, this simpler design is a better engineering tradeoff today.

---

## 9. Source Map

Primary implementation files:

- [`lsm/range_filter.go`](../lsm/range_filter.go)
- [`lsm/level_handler.go`](../lsm/level_handler.go)
- [`lsm/table.go`](../lsm/table.go)
- [`lsm/diagnostics.go`](../lsm/diagnostics.go)
- [`stats.go`](../stats.go)

Related benchmark coverage:

- [`lsm/lsm_bench_test.go`](../lsm/lsm_bench_test.go)
