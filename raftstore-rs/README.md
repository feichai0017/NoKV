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

The repository Docker image builds the same binary at
`/usr/local/bin/nokv-raftstore-server`. Docker compose still starts the Go
store path by default until the Rust data plane passes the compose fsmeta smoke
and benchmark gates.
The image also includes `serve-rust-store.sh` and
`join-rust-raftstore-peers.sh` for one-region Rust parity runs; they translate
NoKV config files into the current Rust environment variables and drive the
existing `RaftAdmin AddPeer` wire contract through `nokv raft-admin`.

Standalone multi-process tests can start a seed peer with the default bootstrap
mode and start joining peers with explicit identity plus bootstrap disabled:

```bash
NOKV_RUST_RAFTSTORE_ADDR=127.0.0.1:23880 \
NOKV_RUST_RAFTSTORE_REGION_ID=1 \
NOKV_RUST_RAFTSTORE_STORE_ID=1 \
NOKV_RUST_RAFTSTORE_PEER_ID=1 \
NOKV_RUST_RAFTSTORE_BOOTSTRAP=true \
NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS=2=127.0.0.1:23881,3=127.0.0.1:23882 \
cargo run --manifest-path raftstore-rs/Cargo.toml -p nokv-raftstore-server

NOKV_RUST_RAFTSTORE_ADDR=127.0.0.1:23881 \
NOKV_RUST_RAFTSTORE_STORE_ID=2 \
NOKV_RUST_RAFTSTORE_PEER_ID=2 \
NOKV_RUST_RAFTSTORE_BOOTSTRAP=false \
cargo run --manifest-path raftstore-rs/Cargo.toml -p nokv-raftstore-server
```

`NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS` is only needed on nodes that accept
`RaftAdmin AddPeer`; missing endpoints fail the membership RPC instead of
recording an unreachable placeholder address.

For config-driven local smoke runs, build the binaries and launch one process
per store-region:

```bash
go build -o build/nokv ./cmd/nokv
go build -o build/nokv-config ./cmd/nokv-config
cargo build --manifest-path raftstore-rs/Cargo.toml -p nokv-raftstore-server
PATH="$PWD/build:$PWD/raftstore-rs/target/debug:$PATH" \
  scripts/ops/serve-rust-store.sh --config raft_config.example.json \
  --region-id 1000 --store-id 1 --workdir artifacts/rust-store-1
```

After all peers for that region are serving, run:

```bash
PATH="$PWD/build:$PATH" \
  scripts/ops/join-rust-raftstore-peers.sh --config raft_config.example.json \
  --region-id 1000
```

When `NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR` is set, the server publishes its
`StoreJoined` root event during startup. The bootstrap peer also publishes the
initial `RegionBootstrapped` descriptor. Holt mode persists those startup root
events before publishing so coordinator outages can be retried through the same
pending root-event queue used by later topology changes. Comma-separated
coordinator endpoint lists are accepted; heartbeat and topology publication try
the configured endpoints until one succeeds or a permanent root validation
failure is returned.

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
log batch. `AppliedKvEngine` can now apply OpenRaft entries using the committed
entry log id, and command execution advances the applied index once per Raft
command rather than once per inner request. Holt mode wraps the apply engine
with an apply-status sink so successful writes persist the latest region apply
status for restart bootstrap.

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
