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
  - `HotRingEnabled`, `HotRingTopK`, decay/window settings
  - `HotRingNodeCap`, `HotRingNodeSampleBits`, `HotRingRotationInterval`
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

`raft_config.example.json` is the single source of truth for distributed
topology. It is consumed by scripts, `cmd/nokv-redis`, and the `config` package.

Precedence rule: when a value can be provided by both CLI flags and config
file, CLI flags take precedence; config acts as startup defaults.

Minimal shape:

```jsonc
{
  "max_retries": 8,
  "coordinator": {
    "addr": "127.0.0.1:2379",
    "docker_addr": "nokv-coordinator:2379",
    "work_dir": "./artifacts/cluster/coordinator",
    "docker_work_dir": "/var/lib/nokv-coordinator"
  },
  "store_work_dir_template": "./artifacts/cluster/store-{id}",
  "store_docker_work_dir_template": "/var/lib/nokv/store-{id}",
  "stores": [
    {
      "store_id": 1,
      "listen_addr": "127.0.0.1:20170",
      "addr": "127.0.0.1:20170",
      "work_dir": "./artifacts/cluster/store-1",
      "docker_work_dir": "/var/lib/nokv/store-1"
    }
  ],
  "regions": [
    {
      "id": 1,
      "start_key": "-",
      "end_key": "-",
      "epoch": { "version": 1, "conf_version": 1 },
      "peers": [{ "store_id": 1, "peer_id": 101 }],
      "leader_store_id": 1
    }
  ]
}
```

Notes:
- `start_key` / `end_key` accept plain strings, `hex:<bytes>`, or base64. Use
  `"-"` or empty for unbounded ranges.
- `stores` define both host and docker addresses for local runs vs containers.
- Store workdir can be configured per store (`stores[i].work_dir` / `docker_work_dir`)
  or via global templates (`store_work_dir_template` /
  `store_docker_work_dir_template`, both must include `{id}`).
- `coordinator.addr` is the default Coordinator endpoint for host scope; `coordinator.docker_addr` is used
  when tools run in docker scope.
- `coordinator.work_dir` / `coordinator.docker_work_dir` are optional Coordinator persistence directories
  used by bootstrap tooling and `nokv coordinator --config ...` when `--workdir` is not
  set explicitly.
- Store workdir resolution order (`ResolveStoreWorkDir`):
  1. store-scoped override
  2. global template
  3. empty (caller falls back to its own default)
- `leader_store_id` is optional bootstrap metadata. Runtime routing in cluster
  mode is resolved through Coordinator (`GetRegionByKey`), not static leader hints.

Programmatic loading:

```go
cfg, _ := config.LoadFile("raft_config.example.json")
if err := cfg.Validate(); err != nil { /* handle */ }
```

Related tools:
- `scripts/dev/cluster.sh --config raft_config.example.json`
- `go run ./cmd/nokv-redis --raft-config raft_config.example.json`
