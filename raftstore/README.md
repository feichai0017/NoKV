# raftstore

`raftstore` is NoKV's Rust distributed metadata data plane. It executes
mount-scoped fsmeta commands through OpenRaft and applies committed metadata
records into Holt-backed state-machine storage.

The main path is:

```text
fsmeta command
  -> coordinator route
  -> MetadataPlane CommitMetadata
  -> mount Raft group
  -> Holt atomic apply
  -> apply/watch frontier
```

Go still owns `fsmeta`, `meta/root`, `coordinator`, protobuf definitions, and
binary wiring. Rust owns replicated execution, Raft log persistence, state
machine storage, snapshots, apply notifications, and the MetadataPlane/RaftAdmin
service boundary.

## Workspace

- `nokv-proto`: Rust bindings generated from repository protobufs.
- `nokv-metadata-state`: metadata command and versioned-record primitives.
- `nokv-holtstore`: Holt multi-tree state-machine layout and recovery state.
- `nokv-raftlog`: segmented append-only Raft log.
- `nokv-raftnode`: OpenRaft integration, proposal/apply, snapshot, transport.
- `nokv-raftstore-server`: tonic server, startup, coordinator/admin wiring,
  diagnostics.

## Running

The default backend is Holt. The in-memory backend is only for targeted tests.

```bash
NOKV_RAFTSTORE_ADDR=127.0.0.1:23880 \
NOKV_RAFTSTORE_REGION_ID=1 \
NOKV_RAFTSTORE_STORE_ID=1 \
NOKV_RAFTSTORE_PEER_ID=1 \
NOKV_RAFTSTORE_BOOTSTRAP=true \
NOKV_RAFTSTORE_HOLT_DIR=/tmp/nokv-raftstore-holt \
NOKV_RAFTSTORE_LOG_DIR=/tmp/nokv-raftstore-log \
cargo run --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server
```

Important options:

- `NOKV_RAFTSTORE_BACKEND`: `holt` by default; `memory` is test-only.
- `NOKV_RAFTSTORE_COORDINATOR_ADDR`: comma-separated coordinator endpoints.
- `NOKV_RAFTSTORE_REGIONS`: multi-region process identity list,
  `region_id:store_id:peer_id:bootstrap`.
- `NOKV_RAFTSTORE_RAFTLOG_SYNC`: `buffered` by default, so regular Raft log
  appends do not fsync before completing callbacks. Use `group` when a test or
  deployment needs grouped fsync.
- `NOKV_RAFTSTORE_RAFTLOG_GROUP_COMMIT_MS`: group fsync delay when sync mode is
  `group`.

`nokv-raftstore-server` also accepts `--metrics-addr=<addr>` and serves
`/debug/vars`.

## Boundary

This directory should stay focused on replicated metadata execution for fsmeta.
It is not a generic database product layer. Peras witness services, legacy
migration/SST, the old Go Percolator stack, and generic distributed KV features
are outside the mainline.
