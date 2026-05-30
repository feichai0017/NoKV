# raftstore-rs Parity Plan

`raftstore-rs` is the temporary name for the Rust replacement track for the Go
`raftstore` data plane. The name stays explicit while both implementations live
in the repository. After the Go `raftstore` package is removed, this workspace
can be renamed to `raftstore`.

The target is not a new API. The Rust implementation must preserve the existing
Go protobuf wire contract for `StoreKV` and `RaftAdmin`, keep fsmeta semantics
in Go, and move only replicated data-plane execution into Rust.

## Scope

In scope:

- `StoreKV` request semantics from `pb/kv/kv.proto`.
- `RaftAdmin` membership and status semantics from `pb/admin/admin.proto`.
- Region-local Raft proposal, apply, recovery, snapshot, and watch delivery.
- MVCC and Percolator behavior currently served by Go `raftstore`.
- Holt-backed state-machine storage through multi-tree storage.
- Go client and fsmeta runtime compatibility against the Rust endpoint.

Out of scope for v1:

- Experimental Peras `SegmentWitness`.
- Legacy migration, SST import/export, and old LSM fast paths.
- New protobuf APIs.
- Path-aware region split policy or Holt-specific placement optimization.
- Long-term dual maintenance of Go and Rust raftstore.

## Current Go Responsibilities

The Go `raftstore` owns these runtime responsibilities today:

| Area | Current packages | Rust parity requirement |
| --- | --- | --- |
| RPC service boundary | `raftstore/kv`, `raftstore/admin` | Preserve `StoreKV` and `RaftAdmin` wire behavior, including region errors. |
| Client behavior | `raftstore/client`, `raftstore/admin` | Existing Go clients must work against Rust endpoints without semantic changes. |
| Region routing and freshness | `raftstore/store/router`, `raftstore/store/region`, `raftstore/localmeta` | Enforce region id, epoch, peer, store id, key range, leader, and stale command checks. |
| Command execution | `raftstore/store`, `raftstore/command`, `raftstore/kv/write_batcher` | Batch, propose, apply, and return command responses with matching failure behavior. |
| MVCC and lock protocol | `raftstore/mvcc`, `raftstore/kv`, `txn/*` | Match read timestamp, prewrite, commit, rollback, resolve-lock, heartbeat, status, and maintenance behavior. |
| Prepared install | `raftstore/kv/prepared_mvcc_install.go` | Keep `InstallPreparedMVCCEntries` because fsmeta segment install depends on it. |
| Apply observation | `raftstore/kv/watch.go`, `raftstore/store/observer` | Preserve `WatchApply` event ordering, prefix filtering, index/term, and dropped-event reporting. |
| Peer lifecycle | `raftstore/peer`, `raftstore/server`, `raftstore/transport` | Support bootstrap, restart, leader transfer, membership changes, and inter-store transport. |
| Raft log and snapshots | `raftstore/raftlog`, `raftstore/snapshot` | Persist log/apply state and install snapshots for peer catch-up and restart recovery. |
| Coordinator integration | `raftstore/store/scheduler_runtime.go` | Keep store heartbeat, region stats, topology publication, and recovery of pending local events. |

## Current Rust State

The first slices are intentionally narrow:

- `nokv-proto` generates Rust bindings from existing repository protobufs.
- `nokv-mvcc` implements an in-memory `KvEngine` for `StoreKV` semantics.
- `nokv-holtstore` adapts Holt multi-tree storage for persistent MVCC tests.
- `nokv-raftlog` provides a standalone segmented Raft WAL.
- `nokv-raftnode` exposes a NoKV-owned OpenRaft boundary and a single-region
  `AppliedKvEngine` apply wrapper. It can already execute existing
  `RaftCmdRequest` payloads against the MVCC state machine, which fixes the
  proposal/apply payload shape before OpenRaft is wired. `Proposal` also
  round-trips those commands through prost bytes and rejects region/header
  mismatches. `RaftStoreConfig` pins the OpenRaft data, response, node,
  entry, snapshot, and runtime types to this NoKV-owned proposal boundary.
  The crate now also encodes OpenRaft entries into `nokv-raftlog` records and
  decodes them back, covering normal command, blank, and membership entries.
  The durable entry codec preserves the full OpenRaft log id, including the
  leader node id, so replicated followers compare exactly the same log identity
  that the leader proposed.
  `SegmentedEntryLog` wraps the low-level WAL with a region-local OpenRaft
  entry append/recover boundary and rejects mismatched-region batches before any
  record is appended. It also exposes range reads, last-log-id lookup, conflict
  suffix truncation, and purge markers that survive restart. `AppliedKvEngine`
  now has an OpenRaft-entry apply path
  that uses the committed entry's log index and term for apply status and watch
  events, while direct command execution advances the local applied index once
  per Raft command. Holt server mode wraps the apply engine with an apply-status
  sink, so writes persist the latest region apply status for restart bootstrap.
- `RegionLogStorage` and `RegionStateMachine` implement OpenRaft's v2 storage
  boundary over `SegmentedEntryLog` and `AppliedKvEngine`. The log reader shares
  the live region log with append/truncate/purge, because OpenRaft can keep a
  reader across later appends. The implementation is intentionally limited to
  append/read/truncate/purge/apply; purge markers also preserve the full
  OpenRaft log id. Real snapshots remain an explicit gap.
- `OpenRaftRegion` can bootstrap a single-node OpenRaft group with the v2 log
  store and state machine, initialize local membership, wait for leadership,
  and apply an existing `RaftCmdRequest` through `client_write`. On Holt
  restart, the single-node path restores the latest membership from the
  persisted raft log and seeds a restart vote above the last log term only for a
  single-voter membership so it can elect again and accept writes without
  biasing multi-node elections.
- `MemoryRaftNetworkRegistry` provides an in-process OpenRaft network for
  parity tests. A three-node test now initializes a region, elects a leader,
  commits an existing `RaftCmdRequest`, and verifies that every peer applies the
  committed value through its own MVCC state machine.
- `OpenRaftRegion` exposes NoKV-owned voter-change helpers over OpenRaft
  learner and membership APIs. The in-process testkit covers adding a new voter,
  committing a real metadata KV command to the joined peer, and removing that
  voter back out of the membership.
- `RaftAdmin` now wires `AddPeer` and `RemovePeer` onto those
  `OpenRaftRegion` voter-change helpers and returns an updated protobuf region
  descriptor from the service-local topology. Non-OpenRaft apply engines still
  return an explicit `Unimplemented` error for membership RPCs.
- `StoreKV` now depends on an async raft-command executor, and the tonic
  service has coverage against both the direct apply engine and
  `OpenRaftRegion`.
- The standalone Rust raftstore server now boots `OpenRaftRegion` by default
  for both memory and Holt modes. Holt mode keeps the persistent apply-status
  sink behind the OpenRaft state machine.
- `nokv-raftstore-server` exposes compatible tonic `StoreKV` and `RaftAdmin`
  services, including `WatchApply`, apply status, and a single-region admission
  gate for context, epoch, store, leader, and key-range errors. `StoreKV`
  handlers now execute through the same `RaftCmdRequest` boundary that OpenRaft
  proposals will carry.

Known gaps:

- OpenRaft proposal/apply now has in-process three-node replication coverage
  and raftnode-level voter add/remove helpers, but the external tonic raft
  transport, route integration, and `RaftAdmin` RPC wiring are still being built
  out.
- The default server startup is mounted behind a single-node OpenRaft node;
  multi-node networking and route integration are still being built out.
- Region metadata has a Holt persistence point for descriptors and apply-state
  records, and Holt server mode persists apply status after successful write
  commands. The single-region service still bootstraps a default descriptor
  until coordinator-provided topology is wired.
- Admin `AddPeer`/`RemovePeer` RPCs are wired for `OpenRaftRegion`.
  `TransferLeader` remains explicitly unimplemented until raftnode exposes a
  matching leadership-transfer boundary.
- Restart recovery now covers single-node Holt restart and write-after-restart.
  Multi-node restart still needs durable vote, membership-change state, and
  snapshot install before it is production-complete.
- Snapshot checkpoint/install is not implemented.
- Go fsmeta and raftstore client tests do not yet run against Rust by default.

## Target Architecture

```text
Go fsmeta/runtime/raftstore
        |
        | existing StoreKV / RaftAdmin protobufs
        v
raftstore-rs/server
        |
        | NoKV-owned raftnode traits
        v
raftstore-rs/raftnode
        |
        +-- OpenRaft region groups
        +-- raftlog segmented WAL
        +-- Holt-backed MVCC state machine
```

OpenRaft types must stay inside `raftnode`. Holt types must stay inside
`holtstore`. The service crate should only see NoKV-owned traits and generated
protobuf structs.

## Phase 0: Wire Contract and Harness

Goal: make incompatibility visible before replication work begins.

Implementation:

- Add Rust service golden tests for every `StoreKV` and `RaftAdmin` RPC shape.
- Add a Go test harness that starts `raftstore-rs` and runs existing
  `raftstore/client` tests against it.
- Add environment/config wiring so `fsmeta/runtime/raftstore` can target a Rust
  endpoint in integration tests.
- Keep cross-language tests behind the `rust_raftstore` build tag until the Rust
  data plane is the default runtime.

Gate:

```bash
cargo test --manifest-path raftstore-rs/Cargo.toml --workspace
go test -tags rust_raftstore -run TestRustRaftstoreEndpointClientAtomicMutateGetAndWatch -count=1 ./raftstore/client
go test -count=1 ./raftstore/client ./raftstore/admin
go test -count=1 ./fsmeta/runtime/raftstore ./fsmeta/contract ./fsmeta/integration
```

## Phase 1: MVCC Behavior Parity

Goal: match Go MVCC and Percolator semantics before distributing them.

Implementation:

- Port or golden-test these operations:
  `Get`, `BatchGet`, `Scan`, `Prewrite`, `Commit`, `BatchRollback`,
  `ResolveLock`, `CheckTxnStatus`, `TxnHeartBeat`, `TryAtomicMutate`,
  `InstallPreparedMVCCEntries`, and `MVCCMaintenance`.
- Match `KeyError` payloads, including locked, write conflict, already exists,
  retryable, abort, and commit-ts-expired.
- Match unsupported behavior such as reverse scan handling.
- Persist lock, write, data, rollback, apply-state, and watch metadata in Holt
  trees.

Gate:

- Rust MVCC tests cover every Go `raftstore/kv` and `raftstore/mvcc` behavior.
- Go client golden tests pass against Rust single-region service.
- Holt-backed tests pass after process restart.

## Phase 2: Region Admission and Error Parity

Goal: make the Rust service reject the same invalid commands as Go.

Implementation:

- Add region catalog and descriptor persistence in Holt `region_meta`.
- Validate `Context.region_id`, `region_epoch`, `peer`, store id, and key range.
- Return protobuf `RegionError` variants for not leader, epoch mismatch, stale
  command, store mismatch, region not found, and key not in region.
- Preserve read consistency fields and follower-read admission semantics even
  if follower serving initially remains disabled.

Gate:

- Existing Go route-cache and retry tests pass against Rust endpoint.
- Rust tests cover stale epoch, stale peer, wrong store, wrong key range, and
  not-leader behavior.

## Phase 3: OpenRaft Proposal and Apply

Goal: replace the single-node apply wrapper with replicated region groups.

Implementation:

- Define a NoKV command payload that carries existing `RaftCmdRequest` bytes.
- Implement the OpenRaft network wrapper without leaking OpenRaft types above
  `raftnode`.
- Implement per-region state-machine apply for read-only and write commands.
- Preserve write batching and per-region apply ordering.
- Emit apply status and `WatchApply` events only after committed apply.

Gate:

- Three-node Rust cluster can elect a leader, commit writes, serve reads, and
  report matching apply index/term.
- Leader crash during a write either commits exactly once or returns retryable
  region/transport error.

## Phase 4: Durable Recovery

Goal: restart must recover exactly the committed prefix.

Implementation:

- Persist raft log with `nokv-raftlog` and group fsync policy.
- Persist apply state, truncated state, region descriptor, and peer metadata in
  Holt.
- On restart, recover Holt checkpoint, apply state, raft log, and region
  membership before serving.
- Rebuild apply-watch cursors without emitting phantom events.

Gate:

- Crash/restart tests cover before append, after append before commit, after
  commit before apply-state sync, after apply-state sync, and after compaction.
- Existing Go `RegionRuntimeStatus` and `ExecutionStatus` expectations remain
  valid.

## Phase 5: Snapshot and Catch-up

Goal: peers can join or recover without replaying unbounded logs.

Implementation:

- Define a Rust snapshot manifest using Holt checkpoint metadata, region
  descriptor, apply state, and truncated log state.
- Implement OpenRaft snapshot build, install, and validation.
- Use the snapshot only for raft peer bootstrap and recovery, not for operator
  migration.
- Compact raft logs after a snapshot is safely installed.

Gate:

- Add-peer follower catches up from snapshot.
- Restart from snapshot plus suffix log reconstructs the same MVCC state.
- Corrupt or stale snapshot is rejected before state mutation.

## Phase 6: Admin and Coordinator Parity

Goal: Rust nodes participate in the existing control-plane workflow.

Implementation:

- Implement `AddPeer`, `RemovePeer`, and `TransferLeader`.
- Publish region descriptors and terminal topology status through the existing
  Go coordinator surfaces.
- Preserve local pending-event recovery semantics or replace them with an
  equivalent Rust-owned durable event queue.
- Report region stats, leader counts, pending admin state, and apply indexes.

Gate:

- Existing Go admin tests pass against Rust service.
- Store heartbeat and coordinator route refresh work with Rust stores.
- Membership change tests pass across restart and leader transfer.

## Phase 7: fsmeta Integration

Goal: fsmeta distributed runtime runs through Rust by configuration.

Implementation:

- Add command/config selection for Rust store endpoints.
- Run fsmeta contract and integration suites against Rust raftstore.
- Run Docker compose fsmeta smoke and official benchmark profiles against Rust.
- Keep fsmeta model, layout, backend, and exec code unchanged except endpoint
  wiring.

Gate:

```bash
go test -count=1 ./fsmeta/backend ./fsmeta/exec ./fsmeta/runtime/raftstore
go test -count=1 ./fsmeta/contract ./fsmeta/integration
cargo test --manifest-path raftstore-rs/Cargo.toml --workspace
```

## Phase 8: Default Cutover

Goal: make Rust the default distributed data plane and stop adding Go raftstore
features.

Implementation:

- Switch compose and command defaults to Rust raftstore.
- Keep Go raftstore only as a temporary regression baseline.
- Document the removal condition and stop extending Go raftstore.
- Remove Go raftstore once fsmeta integration, admin, recovery, and benchmark
  gates pass on Rust.
- Rename `raftstore-rs` to `raftstore` only after the Go package is gone.

Gate:

- `make lint`, `make test`, and Rust workspace tests pass.
- Docker fsmeta smoke and benchmark gates pass on Rust.
- Fault suite covers leader crash, follower restart, membership changes,
  snapshot install, and client retry behavior.

## Safety Invariants

Every phase must preserve these invariants:

- A write is applied only after region authority and freshness checks pass.
- A response carries either a valid command result or a region error, not both.
- A committed Raft entry is applied at most once.
- A visible apply-watch event is emitted only for an applied command.
- Snapshot install never moves apply state backwards.
- Membership changes are durable before the node reports the new topology.
- Holt state, raft log, and apply state agree after restart.

## Review Checklist

Before claiming parity, include exact evidence for:

- Rust `cargo test --workspace`.
- Go client/admin tests against Rust endpoint.
- fsmeta contract/integration tests against Rust endpoint.
- Three-node crash/restart and membership test logs.
- Snapshot install and log compaction tests.
- Docker compose fsmeta smoke benchmark with Rust default.
- `git diff --check`, `make lint`, and `make test`.
