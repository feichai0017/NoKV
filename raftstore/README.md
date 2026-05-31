# raftstore

`raftstore` is NoKV's Rust distributed data plane for fsmeta. It is no longer
a second implementation beside the old Go raftstore; the Go data-plane tree has
been removed from the mainline.

The target shape is mount-scoped fsmeta Raft:

```text
fsmeta semantic API
  -> meta/root + coordinator control plane
  -> one Rust Raft group per mount by default
  -> Holt multi-tree state-machine storage
```

Go remains responsible for `fsmeta`, `meta/root`, `coordinator`, protobuf
definitions, and binary wiring. Rust owns replicated execution: OpenRaft
regions, proposal/apply, the segmented Raft log, Holt-backed state-machine
storage, snapshots, apply notifications, and the compatibility service surface
needed by fsmeta.

## Workspace

The Rust workspace keeps responsibilities small:

- `nokv-proto`: Rust bindings generated from the repository `pb/*.proto` files.
- `nokv-mvcc`: legacy StoreKV/MVCC compatibility scaffolding. Do not extend it
  as the long-term fsmeta transaction model.
- `nokv-holtstore`: Holt multi-tree state-machine layout and recovery state.
- `nokv-raftlog`: append-only segmented Raft log.
- `nokv-raftnode`: OpenRaft boundary, proposals, apply, snapshots, and
  transport codecs.
- `nokv-raftstore-server`: tonic service, startup, coordinator publication,
  admin wiring, and diagnostics.

## Running Locally

The server defaults to an in-memory backend for local compatibility tests. Set
`NOKV_RAFTSTORE_HOLT_DIR` to use Holt-backed state-machine storage.

```bash
NOKV_RAFTSTORE_ADDR=127.0.0.1:23880 \
NOKV_RAFTSTORE_REGION_ID=1 \
NOKV_RAFTSTORE_STORE_ID=1 \
NOKV_RAFTSTORE_PEER_ID=1 \
NOKV_RAFTSTORE_BOOTSTRAP=true \
NOKV_RAFTSTORE_PEER_ENDPOINTS=2=127.0.0.1:23881,3=127.0.0.1:23882 \
cargo run --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server
```

Joining peers use the same binary with `NOKV_RAFTSTORE_BOOTSTRAP=false` and
their own store and peer identity. `NOKV_RAFTSTORE_PEER_ENDPOINTS` is only
needed on nodes that accept membership requests.

Useful environment variables:

- `NOKV_RAFTSTORE_ADDR`: bind address, default `127.0.0.1:23880`.
- `NOKV_RAFTSTORE_ADVERTISE_ADDR`: address published to coordinator/OpenRaft.
- `NOKV_RAFTSTORE_HOLT_DIR`: enables persistent Holt-backed storage.
- `NOKV_RAFTSTORE_LOG_DIR`: overrides the Raft log directory.
- `NOKV_RAFTSTORE_COORDINATOR_ADDR`: comma-separated coordinator endpoints.
- `NOKV_RAFTSTORE_REGIONS`: multi-region process identity list,
  `region_id:store_id:peer_id:bootstrap`.
- `NOKV_RAFTSTORE_REGION_RANGES`: explicit ranges for multi-region bootstrap,
  `region_id=start_hex:end_hex`.

`nokv-raftstore-server` accepts `--metrics-addr=<addr>`. The diagnostics server
serves `/debug/vars` with a `nokv_raftstore` JSON block containing hosted region
counts, leader regions, traffic rates, and pending-admin state.

## Current Boundary

This directory is the active distributed data-plane implementation. It should
not grow generic database semantics. The v1 target is the smallest replicated
execution layer needed by fsmeta:

- committed atomic mutation groups for one mount/region;
- leader-fresh reads and later explicitly proven follower reads;
- apply-ordered watch notifications;
- snapshot/read frontiers tied to committed apply state;
- restart and membership recovery through Holt state plus Raft log replay.

Peras witness services, legacy migration/SST, the old Go Percolator stack, and
generic distributed KV product features are intentionally outside this mainline.

See [MAINLINE_PLAN.md](MAINLINE_PLAN.md) for the remaining implementation plan.
