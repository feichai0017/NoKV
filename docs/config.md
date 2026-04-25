# Configuration & Options

NoKV exposes two configuration surfaces:

1. **Runtime options** for the embedded engine (`Options` in `options.go`).
2. **Cluster topology** for distributed mode (`raft_config.example.json` via
   `config.LoadFile/Validate`).

---

## 1. Runtime Options (Embedded Engine)

`NoKV.NewDefaultOptions()` returns a tuned baseline. Override fields before
calling `NoKV.Open(opt)`, which now returns `(*DB, error)`.

Key option groups (see `options.go` for the full list):

- **Paths & durability**
  - `WorkDir`, `SyncWrites`, `ManifestSync`, `ManifestRewriteThreshold`
- **Write pipeline**
  - `WriteBatchMaxCount`, `WriteBatchMaxSize`, `WriteBatchWait`
  - `MaxBatchCount`, `MaxBatchSize`
  - `WriteThrottleMinRate`, `WriteThrottleMaxRate`
- **Value log**
  - `ValueThreshold`, `ValueLogFileSize`, `ValueLogMaxEntries`
  - `ValueLogGCInterval`, `ValueLogGCDiscardRatio`
  - `ValueLogGCParallelism`, `ValueLogGCReduceScore`, `ValueLogGCSkipScore`
  - `ValueLogGCReduceBacklog`, `ValueLogGCSkipBacklog`
  - `ValueLogGCSampleSizeRatio`, `ValueLogGCSampleCountRatio`,
    `ValueLogGCSampleFromHead`
  - `ValueLogBucketCount`
- **LSM & compaction**
  - `MemTableSize`, `MemTableEngine`, `SSTableMaxSz`, `NumCompactors`
  - `NumLevelZeroTables`, `IngestCompactBatchSize`, `IngestBacklogMergeScore`
  - `CompactionValueWeight`, `CompactionValueAlertThreshold`
- **Caches**
  - `BlockCacheBytes`, `IndexCacheBytes`
- **Hot key throttling**
  - `WriteHotKeyLimit`
  - `ThermosEnabled`, `ThermosTopK`, decay/window settings
  - `ThermosNodeCap`, `ThermosNodeSampleBits`, `ThermosRotationInterval`
- **WAL watchdog**
  - `EnableWALWatchdog`, `WALAutoGCInterval`
  - `WALAutoGCMinRemovable`, `WALAutoGCMaxBatch`
  - `WALTypedRecordWarnRatio`, `WALTypedRecordWarnSegments`
- **Raft lag warnings (stats only)**
  - `RaftLagWarnSegments`

Example:

```go
opt := NoKV.NewDefaultOptions()
opt.WorkDir = "./data"
opt.SyncWrites = true
opt.ValueThreshold = 2048
opt.WriteBatchMaxCount = 128
db, err := NoKV.Open(opt)
if err != nil {
	log.Fatalf("open failed: %v", err)
}
defer db.Close()
```

Notes:
- `NewDefaultOptions()` populates concrete compaction/ingest defaults up front.
  `Open()` resolves constructor-owned defaults once, then the DB and LSM layers
  consume the resolved values directly.
- `WriteBatchMaxCount`, `WriteBatchMaxSize`, `MaxBatchCount`, `MaxBatchSize`,
  `WriteThrottleMinRate`, `WriteThrottleMaxRate`, and `WALBufferSize` now also
  expose concrete defaults through `NewDefaultOptions()`. If you construct
  `Options` manually, leaving these fields at zero lets `Open()` resolve the
  constructor defaults.
- Batch knobs are split by owner:
  - `WriteBatchMaxCount` / `WriteBatchMaxSize` bound commit-worker request
    coalescing.
  - `MaxBatchCount` / `MaxBatchSize` bound internal apply/rewrite batches such
    as `batchSet` and value-log GC rewrites.
- Write slowdown is bandwidth-driven: `WriteThrottleMaxRate` applies when
  slowdown first becomes active, and pressure lowers the target rate toward
  `WriteThrottleMinRate` as compaction debt approaches the stop threshold.

### Load Options From TOML

For convenience, you can load engine options from a TOML file. Unspecified
fields keep their defaults from `NewDefaultOptions`.

```go
opt, err := NoKV.LoadOptionsFile("nokv.options.toml")
if err != nil {
    log.Fatal(err)
}
db, err := NoKV.Open(opt)
if err != nil {
	log.Fatalf("open failed: %v", err)
}
defer db.Close()
```

Example (TOML):

```toml
work_dir = "./data"
mem_table_engine = "art"
value_threshold = 2048
write_hot_key_limit = 128
value_log_gc_interval = "30s"
```

Notes:
- Field names are case-insensitive; `_` / `-` / `.` are ignored.
- Durations accept Go-style strings (e.g. `"30s"`, `"200ms"`). Numeric durations
  are interpreted as nanoseconds.
- File extensions `.toml` and `.tml` are accepted.
- JSON option files are rejected by design.
- Unknown fields return an error so typos do not silently pass.

---

## 2. Raft Topology File

`raft_config.example.json` is consumed by every CLI in distributed mode plus
`cmd/nokv-redis`.

### Two-layer semantics

The file has **two independent layers** with different lifecycles. Confusing
them is a common deployment mistake, so be explicit:

| Layer | Keys | Lifecycle | Source of truth |
|---|---|---|---|
| **Address directory** | `meta_root.peers`, `coordinator`, `stores`, `store_work_dir_template`, `max_retries` | Read on **every** CLI invocation. Keep in sync with deployed containers/hosts. | **This file is the source of truth.** Nothing else knows where to dial. |
| **Bootstrap seed** | `regions` | Read **only** on first startup by `scripts/ops/bootstrap.sh`. Once a store has `CURRENT`, bootstrap skips it. | After first bootstrap, **meta-root** owns the runtime region topology. Inspect with `nokv-config regions` or `nokv eunomia-audit`. |

Consequence: editing `regions` after bootstrap is a no-op for running
clusters. Editing addresses is effective on the next CLI invocation
(restart / docker compose up).

### Precedence

When a value can come from both CLI and config file, **CLI wins**. Config is
a source of defaults:

```
--root-peer=1=host:2380   → explicit, used
(absent)                  → fall back to meta_root.peers[0].addr
```

### Minimal shape

```jsonc
{
  "max_retries": 8,
  "meta_root": {
    "peers": [
      { "node_id": 1,
        "addr": "127.0.0.1:2380",         // coordinator/eunomia-audit dial here
        "docker_addr": "nokv-meta-root-1:2380",
        "transport_addr": "127.0.0.1:3380", // sibling meta-root peers dial here for raft
        "docker_transport_addr": "nokv-meta-root-1:2480",
        "work_dir": "./artifacts/cluster/meta-root-1",
        "docker_work_dir": "/var/lib/nokv-meta-root" },
      { "node_id": 2, "...": "..." },
      { "node_id": 3, "...": "..." }
    ]
  },
  "coordinator": {
    "addr": "127.0.0.1:2379",
    "docker_addr": "nokv-coordinator-1:2379,nokv-coordinator-2:2379,nokv-coordinator-3:2379"
  },
  "store_work_dir_template": "./artifacts/cluster/store-{id}",
  "store_docker_work_dir_template": "/var/lib/nokv/store-{id}",
  "stores": [
    { "store_id": 1,
      "listen_addr": "127.0.0.1:20170",
      "addr": "127.0.0.1:20170",
      "docker_listen_addr": "0.0.0.0:20160",
      "docker_addr": "nokv-store-1:20160" }
  ],
  "regions": [
    { "id": 1, "start_key": "", "end_key": "m",
      "epoch": { "version": 1, "conf_version": 1 },
      "peers": [{ "store_id": 1, "peer_id": 101 }],
      "leader_store_id": 1 }
  ]
}
```

### Field notes

- **`meta_root.peers`**: exactly 3 entries. `addr` is the gRPC service port
  (coordinators/eunomia-audit dial it). `transport_addr` is the raft transport
  port (sibling meta-root peers dial it for raft messages). They MUST be
  different ports on the same host.
- **`coordinator.addr` / `docker_addr`**: may be a single endpoint or
  comma-separated for multi-coord HA (`coord1:2379,coord2:2379,coord3:2379`).
  Gateways and stores use this list to failover on lease-not-held errors.
- **`stores[i]`**: `addr` is what other processes dial; `listen_addr` is what
  the store binds locally. Usually the same on host scope; different on
  docker scope (`0.0.0.0:20160` vs `nokv-store-1:20160`).
- **Store workdir resolution** (`ResolveStoreWorkDir`):
  1. store-scoped override (`stores[i].work_dir` / `docker_work_dir`)
  2. global template (must contain `{id}`)
  3. empty — caller falls back to its own default
- **`start_key` / `end_key`** accept plain strings, `hex:<bytes>`, or base64.
  Empty or `"-"` means unbounded.
- **`leader_store_id`** is bootstrap metadata only. Runtime routing comes
  from coordinator (`GetRegionByKey`), never from this field.

### CLI integration

`nokv meta-root --config <file> --node-id N` resolves `--peer`,
`--transport-addr`, `--workdir` from the meta_root section. Explicit flags
still override.

`nokv coordinator --config <file>` resolves `--addr` from `coordinator.addr`
and `--root-peer` from `meta_root.peers`. This is how docker-compose keeps
meta-root addresses in a single file.

Programmatic loading:

```go
cfg, _ := config.LoadFile("raft_config.example.json")
if err := cfg.Validate(); err != nil { /* handle */ }
peers := cfg.MetaRootServicePeers("docker") // id → gRPC addr
```

### Related tools

- `scripts/dev/cluster.sh --config raft_config.example.json`
- `scripts/ops/serve-meta-root.sh --config ... --node-id 1`
- `scripts/ops/serve-coordinator.sh --config ... --coordinator-id c1`
- `go run ./cmd/nokv-redis --coordinator-addr 127.0.0.1:2379`
- `nokv-config stores` / `nokv-config regions` — query current rooted
  topology (not the JSON). Use these to diff against the deployment manifest
  after scheduler operations.
