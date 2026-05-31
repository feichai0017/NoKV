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
paths no longer depend on the old StoreKV/Percolator-shaped API.

- Define the Rust write command around fsmeta backend needs: request id,
  mount/region context, predicate set, mutation set, and watch/snapshot
  projection metadata.
- Keep protobuf compatibility where it helps startup, but do not treat the old
  StoreKV/Percolator API as the long-term product surface.
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
watch resume and snapshot-driven data-plane GC are still remaining work.

- Strong reads use leader freshness or a documented ReadIndex-equivalent signal.
- Follower reads stay disabled until there is an explicit freshness proof.
- Watch events are emitted after committed apply and carry a stable cursor.
- Snapshot tokens pin a mount apply frontier.
- Retention/GC is tied to rooted snapshot epochs, not local best effort.

Gate:

```bash
go test -count=1 ./fsmeta/contract ./fsmeta/runtime/raftstore
cargo test --manifest-path raftstore/Cargo.toml -p nokv-raftnode -p nokv-raftstore-server
```

### Phase 5: Fault and Benchmark Gate

- Cover leader crash during mutation, follower restart, membership-change
  restart, snapshot install, log compaction, stale leader retry, and stale route
  retry.
- Prove Holt state, Raft log, apply state, and region descriptor advance
  monotonically after restart.
- Run fsmeta compose smoke and benchmark only after the runtime adapter is real.

Gate:

```bash
cargo test --manifest-path raftstore/Cargo.toml --workspace
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
