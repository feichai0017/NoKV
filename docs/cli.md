# CLI (`cmd/nokv`) Reference

`nokv` provides operational visibility similar to RocksDB `ldb` / Badger CLI, with script-friendly JSON output.

---

## Installation

```bash
go install ./cmd/nokv
```

---

## Shared Flags

- `--workdir <path>`: NoKV database directory (must contain `CURRENT` for manifest commands)
- `--json`: JSON output (default is plain text)
- `--expvar <url>`: for `stats`, fetch from `/debug/vars`
- `--no-region-metrics`: for offline `stats`, skip attaching runtime region metrics

---

## Subcommands

### `nokv stats`

- Reads `StatsSnapshot` either offline (`--workdir`) or online (`--expvar`)
- JSON output is nested by domain (not flat)

Common fields:

- `entries`
- `flush.pending`, `flush.queue_length`, `flush.last_wait_ms`
- `compaction.backlog`, `compaction.max_score`
- `value_log.segments`, `value_log.pending_deletes`, `value_log.gc.*`
- `wal.active_segment`, `wal.segment_count`, `wal.typed_record_ratio`
- `write.queue_depth`, `write.queue_entries`, `write.hot_key_limited`
- `txn.active`, `txn.committed`, `txn.conflicts`
- `region.total`, `region.running`, `region.removing`
- `hot.read_keys`, `hot.write_keys`
- `lsm.levels`, `lsm.value_bytes_total`
- `transport.*`, `redis.*`

Example:

```bash
nokv stats --workdir ./testdata/db --json | jq '.flush.queue_length'
```

### `nokv manifest`

- Reads manifest version state
- Shows log pointer, per-level file info, and value-log metadata

### `nokv vlog`

- Lists value-log segments and current head per bucket
- Useful after GC/recovery checks

### `nokv regions`

- Dumps manifest-backed region catalog (state/range/epoch/peers)
- Supports `--json`

### `nokv serve`

- Starts TinyKv gRPC service backed by local `raftstore`
- Requires `--workdir`, `--store-id`, and `--pd-addr`
- Common flags:
  - `--addr` (default `127.0.0.1:20160`)
  - `--peer storeID=address` (repeatable)
  - `--election-tick`, `--heartbeat-tick`
  - `--raft-max-msg-bytes`, `--raft-max-inflight`
  - `--raft-tick-interval`, `--raft-debug-log`

Example:

```bash
nokv serve \
  --workdir ./artifacts/cluster/store-1 \
  --store-id 1 \
  --addr 127.0.0.1:20170 \
  --pd-addr 127.0.0.1:2379 \
  --peer 2=127.0.0.1:20171 \
  --peer 3=127.0.0.1:20172
```

---

## Integration Tips

- Combine with `RECOVERY_TRACE_METRICS=1` for recovery validation.
- In CI, compare JSON snapshots to detect observability regressions.
- Use `nokv stats --expvar` for online diagnostics and `--workdir` for offline forensics.
