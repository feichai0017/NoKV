# Configuration & Options

NoKV exposes two configuration surfaces:

1. **Runtime options** for the embedded engine (`Options` in `options.go`).
2. **Cluster topology** for distributed mode (`raft_config.example.json` via
   `config.LoadFile/Validate`).

---

## 1. Runtime Options (Embedded Engine)

`NoKV.NewDefaultOptions()` returns a tuned baseline. Override fields before
calling `NoKV.Open(opt)`.

Key option groups (see `options.go` for the full list):

- **Paths & durability**
  - `WorkDir`, `SyncWrites`, `ManifestSync`, `ManifestRewriteThreshold`
- **Write pipeline**
  - `WriteBatchMaxCount`, `WriteBatchMaxSize`, `WriteBatchWait`
- **Value log**
  - `ValueThreshold`, `ValueLogFileSize`, `ValueLogMaxEntries`
  - `ValueLogGCInterval`, `ValueLogGCDiscardRatio`
  - `ValueLogGCParallelism`, `ValueLogGCReduceScore`, `ValueLogGCSkipScore`
  - `ValueLogGCReduceBacklog`, `ValueLogGCSkipBacklog`
  - `ValueLogGCSampleSizeRatio`, `ValueLogGCSampleCountRatio`,
    `ValueLogGCSampleFromHead`
  - `ValueLogBucketCount`, `ValueLogHotBucketCount`, `ValueLogHotKeyThreshold`
- **LSM & compaction**
  - `MemTableSize`, `MemTableEngine`, `SSTableMaxSz`, `NumCompactors`
  - `NumLevelZeroTables`, `IngestCompactBatchSize`, `IngestBacklogMergeScore`
  - `CompactionValueWeight`, `CompactionValueAlertThreshold`
- **Caches**
  - `BlockCacheSize`, `BloomCacheSize`
- **Hot key throttling**
  - `WriteHotKeyLimit`, `HotWriteBurstThreshold`, `HotWriteBatchMultiplier`
  - `HotRingEnabled`, `HotRingTopK`, decay/window settings
  - `HotRingNodeCap`, `HotRingNodeSampleBits`, `HotRingRotationInterval`
  - `ValueLogHotRingOverride` + `ValueLogHotRing*` overrides
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
opt.ValueThreshold = 1024
opt.WriteBatchMaxCount = 128
db := NoKV.Open(opt)
defer db.Close()
```

### Load Options From TOML

For convenience, you can load engine options from a TOML file. Unspecified
fields keep their defaults from `NewDefaultOptions`.

```go
opt, err := NoKV.LoadOptionsFile("nokv.options.toml")
if err != nil {
    log.Fatal(err)
}
db := NoKV.Open(opt)
defer db.Close()
```

Example (TOML):

```toml
work_dir = "./data"
mem_table_engine = "art"
value_threshold = 1024
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

Minimal shape:

```jsonc
{
  "max_retries": 8,
  "stores": [
    { "store_id": 1, "listen_addr": "127.0.0.1:20170", "addr": "127.0.0.1:20170" }
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
- `leader_store_id` is optional; clients use it for initial routing hints.

Programmatic loading:

```go
cfg, _ := config.LoadFile("raft_config.example.json")
if err := cfg.Validate(); err != nil { /* handle */ }
```

Related tools:
- `scripts/run_local_cluster.sh --config raft_config.example.json`
- `go run ./cmd/nokv-redis --raft-config raft_config.example.json --pd-addr 127.0.0.1:2379`
