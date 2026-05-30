# raftstore-rs

This workspace is the Rust data-plane replacement track for NoKV raftstore.

The first slice keeps the existing Go protobuf contract and intentionally does
not introduce new external APIs. `nokv-proto` generates Rust bindings from the
repository `pb/*.proto` files, `nokv-mvcc` implements the current `StoreKV`
transaction semantics for single-node tests, `nokv-holtstore` fixes the future
Holt multi-tree layout, `nokv-raftlog` owns the append-only Raft WAL format, and
`nokv-raftstore-server` exposes the compatible tonic services.

The server binary uses an in-memory MVCC engine by default for compatibility
tests. Set `NOKV_RUST_RAFTSTORE_HOLT_DIR=/path/to/store` to run the same
`StoreKV` service against Holt-backed MVCC trees. In both modes, mutating
`StoreKV` calls pass through a region-local apply wrapper so the RPC layer is
already separated from direct state-machine mutation before OpenRaft is wired.

OpenRaft replication, membership changes, snapshots, and WatchApply delivery are
staged behind these boundaries. Experimental Peras witness services and legacy
migration/SST paths are outside v1.

See [PARITY_PLAN.md](PARITY_PLAN.md) for the full Go `raftstore` parity plan and
the cutover rule: keep the workspace named `raftstore-rs` while the Go
implementation still exists, then rename it to `raftstore` after the Go package
is removed.
