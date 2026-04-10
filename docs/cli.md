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
- `region.total`, `region.running`, `region.removing`
- `hot.write_keys`
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

- Dumps the local peer catalog used for store recovery (state/range/epoch/peers)
- Supports `--json`

### `nokv migrate plan`

- Runs a read-only preflight check for standalone -> cluster-seed migration
- Verifies manifest, WAL, and value-log structure without repairing tails
- Reports current mode, local catalog occupancy, blockers, and next step

### `nokv migrate init`

- Converts a standalone workdir into a single-store seeded cluster directory
- Writes `MODE.json`, the full-range local region catalog entry, and the initial
  raft durable metadata
- Exports a logical seed snapshot under `RAFTSTORE_SNAPSHOTS/region-<id>`
- After `init`, ordinary standalone opens must reject the workdir unless the
  caller explicitly opts into distributed modes

### `nokv migrate status`

- Reads `MODE.json` when present and otherwise reports `standalone`
- Shows current mode plus seed identifiers (`store`, `region`, `peer`)

### `nokv migrate expand`

- Sends one or more `AddPeer` requests to the leader store's admin gRPC endpoint
- Supports sequential rollout with repeated `--target <store>:<peer>[@addr]`
- Optionally polls leader and target stores until each new peer is published,
  hosted, and has applied at least one raft index
- Common flags:
  - `--addr` leader store admin address
  - `--region`
  - `--target <store>:<peer>[@addr]` (repeatable)
  - `--wait` overall wait timeout (`0` disables waiting)
  - `--poll-interval`

### `nokv migrate remove-peer`

- Sends one `RemovePeer` request to the leader store's admin gRPC endpoint
- Optionally waits until the leader metadata drops the peer and the target store
  no longer reports it as hosted
- Common flags:
  - `--addr` leader store admin address
  - `--target-addr` target store admin address for removal wait checks
  - `--region`, `--peer`
  - `--wait`, `--poll-interval`

### `nokv migrate transfer-leader`

- Sends one `TransferLeader` request to the leader store's admin gRPC endpoint
- Optionally waits until the target peer becomes the observed region leader
- Common flags:
  - `--addr` current leader store admin address
  - `--target-addr` target store admin address for leader wait checks
  - `--region`, `--peer`
  - `--wait`, `--poll-interval`

### `nokv serve`

- Starts NoKV gRPC service backed by local `raftstore`
- Requires `--store-id`
- Also requires enough address metadata to reach the Coordinator and all remote
  raft peers:
  - either explicit flags (`--workdir`, `--addr`, `--coordinator-addr`, `--store-addr`)
  - or `--config <raft_config.json> --scope host|docker`
- Common flags:
  - `--addr` (default `127.0.0.1:20160`)
  - `--config`, `--scope host|docker`
  - `--metrics-addr` (optional expvar endpoint, exposes `/debug/vars`)
  - `--store-addr storeID=address` (repeatable override for remote store transport addresses)
  - `--election-tick`, `--heartbeat-tick`
  - `--raft-max-msg-bytes`, `--raft-max-inflight`
  - `--raft-tick-interval`, `--raft-debug-log`

Example:

```bash
nokv serve \
  --config ./raft_config.example.json \
  --scope host \
  --store-id 1 \
  --workdir ./artifacts/cluster/store-1
```

Restart semantics:

- hosted peers come from `raftstore/localmeta`, not `raft_config.json` region lines
- `--config` is used only to resolve:
  - store listen address
  - Coordinator address
  - `storeID -> addr`, which `serve` expands into remote `peerID -> addr`
- `--store-id` must match the durable workdir identity once the workdir has been used
- `--store-addr` is only an exceptional static override; it is keyed by stable
  `storeID`, not by mutable runtime `peerID`

### `nokv coordinator`

- Starts the Coordinator gRPC service used by distributed mode.
- Common flags:
  - `--addr` (default `127.0.0.1:2379`)
  - `--workdir` (optional persistence directory for region catalog + allocator state)
  - `--config` + `--scope host|docker` (resolve defaults from `raft_config.json`)
  - `--id-start`, `--ts-start` (allocator start values)
  - `--metrics-addr` (optional expvar endpoint, exposes `/debug/vars`)

Example:

```bash
nokv coordinator \
  --config ./raft_config.example.json \
  --scope host \
  --metrics-addr 127.0.0.1:23790
```

---

## Integration Tips

- Combine with `RECOVERY_TRACE_METRICS=1` for recovery validation.
- In CI, compare JSON snapshots to detect observability regressions.
- Use `nokv stats --expvar` for online diagnostics and `--workdir` for offline forensics.
