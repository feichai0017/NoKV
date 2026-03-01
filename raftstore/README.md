# RaftStore (`raftstore/`)

`raftstore/` is NoKV's distributed replication and command execution layer.

It sits between:
- the control plane (`pd/`) for routing/scheduling metadata
- the storage engine (`NoKV DB`) for persistent KV state

## Package map

- `store/`: peer lifecycle, region catalog, command pipeline, heartbeat loop, operation scheduler
- `peer/`: raft peer runtime (`RawNode` ready/apply flow)
- `engine/`: raft log/state storage on top of NoKV WAL/manifest
- `transport/`: gRPC raft transport
- `kv/`: TinyKv RPC service and apply bridge
- `server/`: node assembly (`raftstore.Server`)
- `client/`: region-aware distributed client with retries
- `scheduler/`: store-side scheduler abstractions/types

## Runtime note

In current architecture, distributed `serve` path uses PD as the control-plane source.

## Detailed docs

Use the full deep-dive:

- [`docs/raftstore.md`](../docs/raftstore.md)

