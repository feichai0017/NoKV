# raftstore Mainline Plan

`raftstore` is the distributed fsmeta data plane. The old Go raftstore, local
engine stack, generic transaction packages, migration/SST paths, and
experimental Peras tree are no longer mainline surfaces.

The product boundary is now:

```text
fsmeta model/layout/exec/backend
  -> local Pebble runtime for demo and small deployments
  -> meta/root + coordinator for distributed control
  -> Rust raftstore for replicated mount execution
```

## Design Position

The first distributed target is **one Raft group per mount**. That is the right
v1 default because it removes cross-shard metadata transactions from the hot
path: all namespace writes inside a mount are serialized by one replicated log,
and fsmeta can treat the committed apply index as the mount commit frontier.

This is not the theoretical maximum throughput design. It is the simplest
correct design for fsmeta. Later scale-out should split a hot mount only when
there is clear evidence that one mount group is saturated. Splits should be
directory/subtree aware and rooted in `meta/root`; they should not reintroduce a
generic distributed KV transaction layer by accident.

## Status and Remaining Work

### Phase 1: Stable fsmeta Command Contract

Status: implemented in the current mainline. `fsmeta/backend.Store` exposes
`CommitMetadata`, Rust exposes `MetadataPlane.CommitMetadata`, and fsmeta write
paths no longer depend on the old StoreKV/Percolator-shaped API. The Rust
state-machine core now keeps only metadata-native command/versioned-value
semantics.

- Define the Rust write command around fsmeta backend needs: request id,
  mount/region context, predicate set, mutation set, and watch/snapshot
  projection metadata.
- Keep protobuf ownership explicit: metadata data-plane APIs live in
  `pb/metadata`; deleted StoreKV/Percolator APIs are not compatibility
  surfaces.
- Return a committed apply frontier as the fsmeta commit version.
- Keep `fsmeta/exec` as the semantic owner; Rust executes compiled effects and
  must not import Go fsmeta packages.

Gate:

```bash
cargo test --manifest-path raftstore/Cargo.toml --workspace
go test -count=1 ./fsmeta/backend ./fsmeta/exec ./fsmeta/runtime/local ./fsmeta/contract
git diff --check
```

### Phase 2: Distributed Runtime Adapter

Status: implemented as `fsmeta/runtime/raftstore`. The adapter resolves routes,
timestamps, IDs, and mounts through coordinator, sends fsmeta commands to the
Rust `MetadataPlane`, retries stale route/leader errors, streams watch apply
events, and publishes snapshot epochs into root through coordinator.

- Add a new `fsmeta/runtime/raftstore` adapter against the cleaned
  `fsmeta/backend.Store` contract.
- Route each mount to the coordinator-provided raftstore endpoint.
- Map backend reads to leader-fresh raftstore reads.
- Map backend mutations to one committed raftstore command.
- Keep local Pebble runtime independent; it remains the default demo path.

Gate:

```bash
go test -count=1 ./fsmeta/runtime/raftstore ./fsmeta/contract
go test -tags rust_raftstore -count=1 ./fsmeta/runtime/raftstore
cargo test --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server
```

### Phase 3: Mount Lifecycle

- Root owns mount truth, peer membership, and lifecycle events.
- Coordinator rebuilds serving routes from root truth and store heartbeats.
- Raftstore startup publishes store and hosted mount-group state.
- Mount creation bootstraps a raft group, publishes the descriptor, and becomes
  routable only after root/coordinator observe it.
- Restart reopens every hosted mount group and rejects unknown or retired
  mounts.

Gate:

```bash
cargo test --manifest-path raftstore/Cargo.toml --workspace
go test -count=1 ./meta/root/... ./coordinator/...
```

### Phase 4: Reads, Watch, Snapshot, and GC

Status: strong leader-routed reads, apply-ordered watch streams, and rooted
snapshot epoch publish/retire are wired. Durable apply-history replay for
watch resume is wired through the Holt `watch_apply` tree. The data plane now
has a metadata retention primitive that prunes per-key MVCC history below a
rooted snapshot floor while keeping the floor anchor version needed by reads at
or above that floor. RaftAdmin exposes `PruneMetadataVersions`, and coordinator
heartbeats now derive metadata-retention prune operations from the rooted
snapshot-retention floor so pruning is driven by root truth rather than local
best effort.

- Strong reads use leader freshness or a documented ReadIndex-equivalent signal.
- Follower reads stay disabled until there is an explicit freshness proof.
- Watch events are emitted after committed apply and carry a stable cursor.
- Snapshot tokens pin a mount apply frontier.
- Retention/GC must be driven by rooted snapshot epochs, not local best effort.

Gate:

```bash
go test -count=1 ./fsmeta/contract ./fsmeta/runtime/raftstore
go test -tags rust_raftstore -count=1 ./fsmeta/runtime/raftstore
cargo test --manifest-path raftstore/Cargo.toml -p nokv-raftnode -p nokv-raftstore-server
```

### Phase 5: Fault and Benchmark Gate

Status: in progress. Fault coverage now includes removed-peer restart,
coordinator rebuild after retired mount, leader handoff with old-leader stop,
watch cursor replay after restart, and retention prune. CI now runs
`make fsmeta-rust-smoke`, and the fsmeta benchmark workflow includes a tiny
Rust distributed `mdtest-easy` smoke workload that starts `meta-root`,
`coordinator`, Rust `raftstore`, and `nokv-fsmeta --runtime=raftstore` as real
processes.

- Cover leader crash during mutation, follower restart, membership-change
  restart, snapshot install, log compaction, stale leader retry, and stale route
  retry.
- Prove Holt state, Raft log, apply state, and region descriptor advance
  monotonically after restart.
- Run the Rust distributed fsmeta benchmark gate after the runtime adapter is
  real. This launcher starts `meta-root`, `coordinator`, Rust `raftstore`, and
  `nokv-fsmeta --runtime=raftstore` as separate processes before running the
  benchmark client.

Gate:

```bash
make fsmeta-rust-smoke
NOKV_FSMETA_BENCH_MODE=rust NOKV_FSMETA_WORKLOADS=mdtest-easy NOKV_FSMETA_CLIENTS=1 NOKV_FSMETA_DIRS=1 NOKV_FSMETA_FILES_PER_DIR=2 make fsmeta-bench
cargo test --manifest-path raftstore/Cargo.toml --workspace
go test -tags rust_raftstore -count=1 ./fsmeta/runtime/raftstore
make lint
make test
```

## Non-Goals

- v1 is not a generic TiKV-style distributed transaction system.
- v1 does not revive the deleted Go raftstore, `txn/`, `local/`, `storage/`, or
  migration/SST code paths.
- v1 does not move fsmeta operation compilation into Rust.
- v1 does not include Peras witness or other experimental visible-commit paths.
- v1 does not make subtree split the default routing unit.
