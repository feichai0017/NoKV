# raftstore-rs Mainline Plan

`raftstore-rs` is the Rust replacement track for NoKV's distributed fsmeta data
plane. The new mainline target is **mount-scoped fsmeta Raft**, not a full
Percolator-compatible distributed KV clone.

The stable NoKV product boundary is:

```text
fsmeta semantic API
  -> meta/root + coordinator control plane
  -> one Rust Raft group per mount by default
  -> Holt/Pebble ordered storage behind the state machine
```

`meta/root` remains the durable cluster truth for mounts, regions, peers,
snapshots, quota fences, and lifecycle events. `coordinator` remains the
rebuildable serving plane for route lookup, store heartbeat, mount admission,
and admin scheduling. The Rust data plane owns replicated execution only.

## Target Semantics

- A mount is the default Raft authority and routing unit.
- All writes for a mount are serialized by that mount's Raft log.
- The Go `fsmeta/exec` compiler remains the only owner of inode/dentry/session
  semantics. Rust executes compiled predicates and storage effects; it does not
  interpret namespace operations.
- The commit version observed by fsmeta is derived from the committed Raft
  apply frontier, not from Percolator start/commit timestamp pairs.
- Reads use the mount group's applied frontier. Strong reads use leader
  freshness; bounded-stale follower reads stay disabled until an explicit
  freshness proof is implemented.
- Watch events are emitted only after a committed command is applied.
- Snapshot tokens pin a mount read frontier; retention and GC remain coordinated
  through rooted snapshot epochs.

## Compatibility Boundary

Rust v1 keeps enough of the existing protobuf surface to let current Go
`fsmeta/runtime/raftstore` talk to the Rust endpoint while the old Go
`raftstore` still exists:

- Keep `Get`, `BatchGet`, `Scan`, `TryAtomicMutate`, `InstallPreparedMVCCEntries`,
  `WatchApply`, and `RaftAdmin`.
- Treat `TryAtomicMutate` as the compiled fsmeta mutation command: predicates
  are checked and effects are applied atomically inside one committed Raft
  command.
- Stop extending `Prewrite`, `Commit`, `BatchRollback`, `ResolveLock`,
  `CheckTxnStatus`, and `TxnHeartBeat` in Rust. Existing Rust parity code is
  legacy scaffolding and should be removed once the mount-scoped path passes the
  fsmeta gates.
- Do not add Peras witness, legacy migration/SST, or new public protobuf APIs to
  Rust v1.

This is not a long-term dual mode. The StoreKV-shaped transport is a temporary
wire shape for fsmeta backend mutations, not a promise that Rust v1 is a
general-purpose distributed KV transaction engine.

## Implementation Phases

### Phase 1: Freeze Percolator Parity

- Update code and docs so new Rust work targets mount-scoped fsmeta Raft.
- Mark Rust Percolator command handlers and tests as legacy compatibility.
- Keep Go `txn/percolator` and Go `raftstore` as the old regression baseline
  until the fsmeta Rust path is proven.
- Remove Rust parity goals for `Prewrite`, `Commit`, `Rollback`,
  `ResolveLock`, heartbeat, and lock TTL behavior.

Gate:

```bash
cargo test --manifest-path raftstore-rs/Cargo.toml --workspace
cargo build --manifest-path raftstore-rs/Cargo.toml --bins
go test -tags rust_raftstore -count=1 ./raftstore/client ./fsmeta/integration
git diff --check
```

### Phase 2: Mount-Scoped Command Path

- Introduce a Rust-owned command boundary for compiled fsmeta mutations:
  predicate list, mutation list, caller request id, and mount/region context.
- Execute that command through OpenRaft; apply checks predicates and writes the
  full mutation set atomically.
- Return the committed apply frontier as the fsmeta commit version.
- Make `fsmeta/runtime/raftstore.Runner` prefer the read-ordered atomic path
  against Rust endpoints and stop falling back to Percolator for that runtime.
- Keep `fsmeta/exec` unchanged except for any necessary backend capability
  probing; Rust must not import fsmeta packages.

Gate:

```bash
go test -tags rust_raftstore -count=1 ./fsmeta/runtime/raftstore ./fsmeta/integration
cargo test --manifest-path raftstore-rs/Cargo.toml -p nokv-raftnode -p nokv-raftstore-server
```

### Phase 3: Mount Lifecycle and Admin

- Coordinator mount registration creates or routes to exactly one default Raft
  group per mount.
- Rust startup publishes store membership and hosted mount-region descriptors to
  the coordinator through existing root-event paths.
- `AddPeer`, `RemovePeer`, and `TransferLeader` operate on mount groups and
  persist terminal topology state before reporting success.
- Restart reopens every hosted mount group, restores descriptors, and rejects
  writes for retired or unknown mounts.

Gate:

```bash
go test -tags rust_raftstore -count=1 ./raftstore/client ./fsmeta/integration
cargo test --manifest-path raftstore-rs/Cargo.toml -p nokv-raftstore-server
```

### Phase 4: Watch, Snapshot, and GC Frontiers

- Emit watch events only after committed apply and preserve the existing cursor
  shape `(region_id, term, index, commit_version)`.
- Map fsmeta snapshot tokens to mount apply frontiers.
- Ensure rooted snapshot epochs pin storage history until retirement.
- Remove Percolator lock-resolution events from the fsmeta watch contract on
  the Rust path.

Gate:

```bash
go test -tags rust_raftstore -count=1 ./fsmeta/contract ./fsmeta/integration
```

### Phase 5: Recovery and Fault Coverage

- Cover leader crash during mutation, follower restart, membership-change
  restart, snapshot install, log compaction, stale leader retry, and stale epoch
  retry.
- Prove Holt state, raft log, apply state, and region descriptor advance
  monotonically after restart.
- Keep old Go raftstore only as a comparison baseline until these gates pass.

Gate:

```bash
cargo test --manifest-path raftstore-rs/Cargo.toml --workspace
go test -tags rust_raftstore -count=1 ./raftstore/client ./fsmeta/integration
```

### Phase 6: Default Cutover and Deletion

- Switch Docker compose and fsmeta distributed runtime defaults to Rust after
  the smoke, benchmark, and fault gates pass.
- Delete Rust Percolator parity code.
- Delete Go distributed fsmeta dependence on `txn/percolator`.
- In a later cleanup PR, remove or archive the old Go `raftstore` data plane and
  rename `raftstore-rs` to `raftstore`.

Gate:

```bash
NOKV_RAFTSTORE_IMPL=rust NOKV_FSMETA_BENCH_MODE=compose NOKV_FSMETA_PROFILE=median make fsmeta-bench
NOKV_RAFTSTORE_IMPL=rust NOKV_FSMETA_BENCH_MODE=compose NOKV_FSMETA_PROFILE=official make fsmeta-bench
make lint
make test
```

## Non-Goals

- Rust v1 is not a generic TiKV-style transaction system.
- Rust v1 does not implement Peras `SegmentWitness`.
- Rust v1 does not revive legacy migration, SST ingest/export, or old LSM fast
  paths.
- Rust v1 does not move fsmeta operation compilation into Rust.
- Subtree split is not the default v1 routing unit; it remains a later scale-out
  feature after mount-scoped groups are stable.
