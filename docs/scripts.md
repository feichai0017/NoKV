# Scripts Overview

NoKV ships a small collection of helper scripts to streamline local experimentation, demos, diagnostics, and automation. This page summarises what each script does, how to use it, and which shared configuration it consumes.

---

## Cluster helpers

### `scripts/run_local_cluster.sh`
- **Purpose** – builds `nokv`, `nokv-config`, and `nokv-tso`, reads `raft_config.json`, seeds manifests, and starts the TinyKv nodes (plus TSO when configured). If a store directory already contains a manifest (`CURRENT`), the seeding step is skipped so previously bootstrapped data is reused.
- **Usage**
  ```bash
  ./scripts/run_local_cluster.sh --config ./raft_config.example.json --workdir ./artifacts/cluster
  ```
`--config` defaults to the repository’s `raft_config.example.json`; `--workdir` chooses the data root (`./artifacts/cluster` by default). For every entry under `stores` the script creates `store-<id>`, calls `nokv-config manifest`, and, if `tso.listen_addr` is set, launches `nokv-tso`. The script runs in the foreground—press `Ctrl+C` to stop all spawned processes.

> ❗️ **Shutdown / restart note** — To avoid WAL/manifest mismatches, always stop the script with `Ctrl+C` and wait for the `Shutting down...` message. If you crash the process or the host, clean the workdir (`rm -rf ./artifacts/cluster`) before starting again; otherwise the replay step may panic when it encounters truncated WAL segments.

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
  `--scope` decides whether to use the local addresses or the container-friendly ones. The script assembles all peer mappings (excluding the local store) and execs `nokv serve`.

---

## Diagnostics & benchmarking

| Script | Purpose |
| --- | --- |
| `scripts/recovery_scenarios.sh` | Runs crash-recovery scenarios across WAL/manifest/vlog. Set `RECOVERY_TRACE_METRICS=1` to collect metrics under `artifacts/recovery/`. |
| `scripts/transport_chaos.sh` | Injects disconnects/blocks/delay into the `raftstore` transport to observe behaviour under faulty networks. |
| `scripts/run_benchmarks.sh` | Executes the comparison benchmarks (NoKV vs Badger/RocksDB). |
| `scripts/analyze_pprof.sh` | Aggregates CPU/heap profiles from `pprof_output/` and renders SVG/PNG summaries. |
| `scripts/debug.sh` | Convenience wrapper around `dlv test` for targeted debugging. |
| `scripts/gen.sh` | Generates mock data or helper artefacts (see inline comments for details). |

---

## Other helpers

### `scripts/tso`
A small Go program (not shell) that exposes an HTTP timestamp oracle:
```bash
go run ./scripts/tso --addr 0.0.0.0:9494 --start 100
```
`run_local_cluster.sh` and Docker Compose invoke it automatically when `tso.listen_addr` is present in the shared config.

---

## Relationship with `nokv-config`

- `nokv-config stores` / `regions` / `tso` provide structured views over `raft_config.json`, making it easy for scripts and CI to query the topology.
- `nokv-config manifest` writes Region metadata into manifests and replaces the historical `manifestctl` binary.
- `cmd/nokv-redis` reads the same config; when `--tso-url` is omitted it falls back to the `tso` section.
- Go tools or custom scripts can import `github.com/feichai0017/NoKV/config` and call `config.LoadFile` / `Validate` to consume the same `raft_config.json`, avoiding divergent schemas.

Maintaining a single `raft_config.json` keeps local scripts, Docker Compose, Redis gateway, and automated tests aligned.
