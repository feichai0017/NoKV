# Stats & Observability Pipeline

NoKV exposes runtime health through:

- `StatsSnapshot` (structured in-process snapshot)
- `expvar` (`/debug/vars`)
- `nokv stats` CLI (plain text or JSON)

The implementation lives in [`stats.go`](../stats.go), and collection runs continuously once DB is open.

---

## 1. Architecture

```mermaid
flowchart TD
    subgraph Collectors
        LSM[lsm.* metrics]
        WAL[wal metrics]
        VLog[value log metrics]
        TXN[oracle txn metrics]
        HOT[hotring]
        REGION[region metrics]
        TRANSPORT[grpc transport metrics]
        REDIS[redis gateway metrics]
    end
    Collectors --> SNAP[Stats.Snapshot()]
    SNAP --> EXP[Stats.collect -> expvar]
    SNAP --> CLI[nokv stats]
```

Two-layer design:

- `metrics` layer: only collects counters/gauges/snapshots.
- `stats` layer: aggregates cross-module data and exports.

---

## 2. Snapshot Schema

`StatsSnapshot` is now domain-grouped (not flat):

- `entries`
- `flush.*`
- `compaction.*`
- `value_log.*` (includes `value_log.gc.*`)
- `wal.*`
- `raft.*`
- `write.*`
- `txn.*`
- `region.*`
- `hot.*`
- `cache.*`
- `lsm.*`
- `transport.*`
- `redis.*`

Representative fields:

- `flush.pending`, `flush.queue_length`, `flush.last_wait_ms`
- `compaction.backlog`, `compaction.max_score`, `compaction.value_weight`
- `value_log.segments`, `value_log.pending_deletes`, `value_log.gc.gc_runs`
- `wal.active_segment`, `wal.segment_count`, `wal.typed_record_ratio`
- `raft.group_count`, `raft.lagging_groups`, `raft.max_lag_segments`
- `write.queue_depth`, `write.avg_request_wait_ms`, `write.hot_key_limited`
- `txn.active`, `txn.started`, `txn.conflicts`
- `region.total`, `region.running`, `region.tombstone`
- `hot.read_keys`, `hot.write_keys`, `hot.read_ring`, `hot.write_ring`
- `cache.block_l0_hit_rate`, `cache.bloom_hit_rate`, `cache.iterator_reused`
- `lsm.levels`, `lsm.value_bytes_total`, `lsm.column_families`

---

## 3. expvar Export

`Stats.collect` exports a single structured object:

- `NoKV.Stats`

All domains (`flush`, `compaction`, `value_log`, `wal`, `txn`, `region`, `hot`, `cache`, `lsm`, `transport`, `redis`) are nested under this object.

Legacy scalar compatibility keys are removed. Consumers should read fields from `NoKV.Stats` directly.

---

## 4. CLI & JSON

- `nokv stats --workdir <dir>`: offline snapshot from local DB
- `nokv stats --expvar <host:port>`: snapshot from running process `/debug/vars`
- `nokv stats --json`: machine-readable nested JSON

Example:

```json
{
  "entries": 1048576,
  "flush": {
    "pending": 2,
    "queue_length": 2
  },
  "value_log": {
    "segments": 6,
    "pending_deletes": 1,
    "gc": {
      "gc_runs": 12
    }
  },
  "hot": {
    "read_keys": [
      {"key": "user:123", "count": 42}
    ]
  }
}
```

---

## 5. Operational Guidance

- `flush.queue_length` + `compaction.backlog` both rising:
  flush/compaction under-provisioned.
- `value_log.discard_queue` high for long periods:
  check `value_log.gc.*` and compaction pressure.
- `write.throttle_active=true` frequently:
  L0 pressure likely high; inspect `cache.block_l0_hit_rate` and compaction.
- `write.hot_key_limited` increasing:
  hot key write throttling is active.
- `raft.lag_warning=true`:
  at least one group exceeds lag threshold.

---

## 6. Comparison

| Engine | Built-in observability |
| --- | --- |
| RocksDB | Rich metrics/perf context, often needs additional tooling/parsing |
| Badger | Optional metrics integrations |
| NoKV | Native expvar + structured snapshot + CLI with offline/online modes |
