# Ingest Buffer Architecture

The ingest buffer is a per-level staging area for SSTables—typically promoted from L0—designed to **absorb bursts, reduce overlap, and unlock parallel compaction** without touching the main level tables immediately. It combines fixed sharding, adaptive scheduling, and optional `IngestKeep` (ingest-merge) passes to keep write amplification and contention low.

```mermaid
flowchart LR
  L0["L0 SSTables"] -->|moveToIngest| Ingest["Ingest Buffer (sharded)"]
  subgraph levelN["Level N"]
    Ingest -->|IngestDrain: ingest-only| MainTables["Main Tables"]
    Ingest -->|IngestKeep: ingest-merge| Ingest
  end
  Ingest -.read path merge.-> ClientReads["Reads/Iterators"]
```

## Design Highlights
- **Sharded by key prefix**: ingest tables are routed into fixed shards (top bits of the first byte). Sharding cuts cross-range overlap and enables safe parallel drain.
- **Snapshot-friendly reads**: ingest tables are read under the level `RLock`, and iterators hold table refs so mmap-backed data stays valid without additional snapshots.
- **Two ingest paths**:
  - *Ingest-only compaction*: drain ingest → main level (or next level) with optional multi-shard parallelism guarded by `compact.State`.
  - *Ingest-merge*: compact ingest tables back into ingest (stay in-place) to drop superseded versions before promoting, reducing downstream write amplification.
- **IngestMode enum**: plans carry an `IngestMode` with `IngestNone`, `IngestDrain`, and `IngestKeep`. `IngestDrain` corresponds to ingest-only (drain into main tables), while `IngestKeep` corresponds to ingest-merge (compact within ingest).
- **Adaptive scheduling**:
  - Shard selection is driven by `compact.PickShardOrder` / `compact.PickShardByBacklog` using per-shard size, age, and density.
  - Shard parallelism scales with backlog score (based on shard size/target file size) bounded by `IngestShardParallelism`.
  - Batch size scales with shard backlog to drain faster under pressure.
  - Ingest-merge triggers when backlog score exceeds `IngestBacklogMergeScore` (default 2.0), with dynamic lowering under extreme backlog/age.
- **Observability**: expvar/stats expose `IngestDrain` vs `IngestKeep` counts, duration, and tables processed, plus ingest size/value density per level/shard.

## Configuration
- `IngestShardParallelism`: max shards to compact in parallel (default `max(NumCompactors/2, 2)`, auto-scaled by backlog).
- `IngestCompactBatchSize`: base batch size per ingest compaction (auto-boosted by shard backlog).
- `IngestBacklogMergeScore`: backlog score threshold to trigger `IngestKeep`/ingest-merge (default 2.0).

## Benefits
- **Lower write amplification**: bursty L0 SSTables land in ingest first; `IngestKeep`/ingest-merge prunes duplicates before full compaction.
- **Reduced contention**: sharding + `compact.State` allow parallel ingest drain with minimal overlap.
- **Predictable reads**: ingest is part of the read snapshot, so moving tables in/out does not change read semantics.
- **Tunable and observable**: knobs for parallelism and merge aggressiveness, with per-path metrics to guide tuning.

## Future Work
- Deeper adaptive policies (IO/latency-aware), richer shard-level metrics, and more exhaustive parallel/restart testing under fault injection.
