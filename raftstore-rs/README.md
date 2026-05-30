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
The tonic service also enforces the current single-region admission boundary:
request context, region epoch, target store, leader state, and key range are
validated before the MVCC engine observes the request.
When Holt mode is enabled, the server loads or bootstraps that single-region
descriptor through the `region_meta` tree so the admission boundary has a
durable metadata source before OpenRaft owns topology changes.

OpenRaft replication, membership changes, snapshots, and WatchApply delivery are
staged behind these boundaries. Experimental Peras witness services and legacy
migration/SST paths are outside v1.
The current `raftnode` crate already accepts the existing `RaftCmdRequest`
payload shape for local apply, and `StoreKV` handlers execute through that
boundary. The future replicated proposal path can therefore carry the Go
raftstore command contract unchanged; `Proposal` encodes that command payload
directly and validates that the proposal region matches the command header.
`RaftStoreConfig` fixes the OpenRaft type boundary around that proposal/result
shape without exposing OpenRaft types through the service or Go protobuf
boundary. `raftnode` also owns the codec between OpenRaft entries and
`nokv-raftlog` records, so the low-level segmented WAL stays independent from
OpenRaft while still preserving normal command, blank, and membership entries.
The `SegmentedEntryLog` wrapper fixes a region-local append/recover boundary and
pre-encodes batches before append, so an invalid entry cannot partially write a
log batch.

See [PARITY_PLAN.md](PARITY_PLAN.md) for the full Go `raftstore` parity plan and
the cutover rule: keep the workspace named `raftstore-rs` while the Go
implementation still exists, then rename it to `raftstore` after the Go package
is removed.

Run the explicit Go compatibility smoke test with:

```bash
go test -tags rust_raftstore -run TestRustRaftstoreEndpointClientAtomicMutateGetAndWatch -count=1 ./raftstore/client
```

The tag keeps Cargo-backed cross-language tests out of the regular Go unit-test
path until Rust raftstore becomes the default data plane.
