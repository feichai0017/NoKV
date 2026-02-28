# Scripts Overview

NoKV ships a small collection of helper scripts to streamline local experimentation, demos, diagnostics, and automation. This page summarises what each script does, how to use it, and which shared configuration it consumes.

---

## Cluster helpers

### `scripts/run_local_cluster.sh`
- **Purpose** – builds `nokv` and `nokv-config`, reads `raft_config.json`, seeds manifests, starts PD-lite, and starts the TinyKv nodes. If a store directory already contains a manifest (`CURRENT`), the seeding step is skipped so previously bootstrapped data is reused.
- **Usage**
  ```bash
  ./scripts/run_local_cluster.sh --config ./raft_config.example.json --workdir ./artifacts/cluster
  ```
`--config` defaults to the repository’s `raft_config.example.json`; `--workdir` chooses the data root (`./artifacts/cluster` by default). For every entry under `stores` the script creates `store-<id>` and calls `nokv-config manifest`, then launches `nokv pd` and the store processes. The script runs in the foreground—press `Ctrl+C` to stop all spawned processes.
When `--pd-listen` is omitted, the script reads `pd.addr` from config and falls back to `127.0.0.1:2379`.

> ❗️ **Shutdown / restart note** — To avoid WAL/manifest mismatches, stop the script with `Ctrl+C` and wait for child processes to exit. If you crash the process or the host, clean the workdir (`rm -rf ./artifacts/cluster`) before starting again; otherwise the replay step may panic when it encounters truncated WAL segments.

### `scripts/bootstrap_from_config.sh`
- **Purpose** – manifest-only bootstrap, typically used in Docker Compose before the nodes start. Stores that already hold a manifest are detected and skipped.
- **Usage**
  ```bash
  ./scripts/bootstrap_from_config.sh --config /etc/nokv/raft_config.json --path-template /data/store-{id}
  ```
  The script iterates over every store in the config and writes Region metadata via `nokv-config manifest` into the provided path template.

### `scripts/serve_from_config.sh`
- **Purpose** – translate `raft_config.json` into a `nokv serve` command, avoiding manual `--peer` lists. It resolves peer IDs from the region metadata and maps every peer (other than the local store) to its advertised address so that gRPC transport works out of the box.
- **Usage**
  ```bash
  ./scripts/serve_from_config.sh \
      --config ./raft_config.json \
      --store-id 1 \
      --workdir ./artifacts/cluster/store-1 \
      --scope local   # use --scope docker inside containers
  ```
  `--scope` decides whether to use the local addresses or the container-friendly ones. The script also resolves PD from `config.pd` unless `--pd-addr` explicitly overrides it. It assembles all peer mappings (excluding the local store) and execs `nokv serve`.

---

## Diagnostics & benchmarking

| Script | Purpose |
| --- | --- |
| `scripts/recovery_scenarios.sh` | Runs crash-recovery scenarios across WAL/manifest/vlog. Set `RECOVERY_TRACE_METRICS=1` to collect metrics under `artifacts/recovery/`. |
| `scripts/transport_chaos.sh` | Injects disconnects/blocks/delay into the `raftstore` transport to observe behaviour under faulty networks. |
| `scripts/run_benchmarks.sh` | Executes YCSB benchmarks (default engines: NoKV/Badger/Pebble, workloads A-G; optional RocksDB via build tags). |
| `scripts/debug.sh` | Convenience wrapper around `dlv test` for targeted debugging. |
| `scripts/gen.sh` | Generates mock data or helper artefacts (see inline comments for details). |

---

## Other helpers

### `cmd/nokv pd`
PD-lite service used by local scripts and compose for:
- routing (`GetRegionByKey`)
- ID allocation (`AllocID`)
- timestamp allocation (`Tso`)

Example:
```bash
go run ./cmd/nokv pd --addr 127.0.0.1:2379 --id-start 1 --ts-start 100
```

---

## Relationship with `nokv-config`

- `nokv-config stores` / `regions` / `pd` provide structured views over `raft_config.json`, making it easy for scripts and CI to query the topology.
- `nokv-config manifest` writes Region metadata into manifests and replaces the historical `manifestctl` binary.
- `cmd/nokv-redis` reads the same config and uses `config.pd` by default in raft mode (`--pd-addr` remains an override).
- Go tools or custom scripts can import `github.com/feichai0017/NoKV/config` and call `config.LoadFile` / `Validate` to consume the same `raft_config.json`, avoiding divergent schemas.

Maintaining a single `raft_config.json` keeps local scripts, Docker Compose, Redis gateway, and automated tests aligned.
