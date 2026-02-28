# PD-lite Package

`pd/` is NoKV's lightweight control-plane implementation.

It provides:

- Region routing (`GetRegionByKey`)
- Store/Region heartbeats (`StoreHeartbeat`, `RegionHeartbeat`)
- ID allocation (`AllocID`)
- Timestamp allocation (`Tso`)
- Region metadata removal (`RemoveRegion`)

## Package Layout

- `pd/core`: in-memory cluster model and allocators.
- `pd/server`: gRPC service implementation (`pb.PDServer`).
- `pd/client`: gRPC client wrapper consumed by raftstore/redis gateway.
- `pd/adapter`: bridge from `raftstore/scheduler.RegionSink` to PD RPCs.
- `pd/tso`: monotonic timestamp allocator.

## Persistence Model

PD-lite can run fully in-memory, or with `--workdir` persistence:

- Region catalog persistence: manifest `EditRegion` records.
- Allocator checkpoints: `PD_STATE.json` (`id_current`, `ts_current`).

On restart, `cmd/nokv pd` restores Region metadata from manifest and raises
allocator starts from checkpoint (`current + 1`).

## Routing Semantics

Runtime route source is PD.

- `raftstore/client` resolves regions by key through PD (`GetRegionByKey`) and
  caches returned routes.
- `raft_config.json` region entries are bootstrap/deployment metadata, not
  runtime routing truth.

## Scope and Limits

PD-lite is intentionally minimal:

- single-process control plane (no embedded etcd quorum)
- best-effort scheduling hints (leader transfer only at this stage)
- no production-grade multi-PD HA/failover yet

It is designed for local clusters, integration tests, and architecture
experiments while keeping the API close to a real PD control plane.
