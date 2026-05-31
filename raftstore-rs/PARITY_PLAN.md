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
  OpenRaft log id. Region vote and committed log-id metadata are persisted next
  to the segmented log and covered by reopen tests. The state machine now builds
  real OpenRaft snapshots from a NoKV-owned MVCC snapshot contract, installs
  them into empty peers, and rejects stale snapshots before mutating state.
- `OpenRaftRegion` can bootstrap a single-node OpenRaft group with the v2 log
  store and state machine, initialize local membership, wait for leadership,
  and apply an existing `RaftCmdRequest` through `client_write`. On Holt
  restart, the single-node path restores the latest membership from the
  persisted raft log and seeds a restart vote above the last log term only for a
  single-voter membership so it can elect again and accept writes without
  biasing multi-node elections. Bootstrap also waits for OpenRaft's
  linearizable barrier after leadership, so the first post-restart client write
  is not consumed by a leader no-op and returns the real command response.
  GET/SCAN commands now use OpenRaft's linearizable read boundary and execute
  against the state machine without appending a new raft log entry.
- `MemoryRaftNetworkRegistry` provides an in-process OpenRaft network for
  parity tests. A three-node test now initializes a region, elects a leader,
  commits an existing `RaftCmdRequest`, and verifies that every peer applies the
  committed value through its own MVCC state machine. The testkit also covers a
  joining peer catching up from a leader snapshot after the snapshot-covered log
  prefix is purged, which validates the real OpenRaft install-snapshot path
  rather than only the state-machine snapshot codec.
- `nokv-raftnode` now owns a prost transport codec for OpenRaft
  `AppendEntries`, `Vote`, and `InstallSnapshot` request/response payloads.
  The codec is intentionally internal to the Rust data plane and reuses the
  durable entry and membership encoding so external network transport can move
  OpenRaft RPCs across process boundaries without changing the existing Go
  `StoreKV` or `RaftAdmin` protobuf contract.
- `EncodedRaftNetworkRegistry` wires that codec into OpenRaft's
  `RaftNetworkFactory` boundary for tests. The encoded network still runs
  in-process, but every replication RPC is encoded to bytes, decoded at the
  target, applied through the target Raft node, encoded as a response, and
  decoded by the caller. This keeps the next tonic transport slice focused on
  IO rather than OpenRaft type conversion.
- `TonicRaftNetworkFactory` and `RaftTransportServer` now provide the first
  real Rust gRPC OpenRaft transport boundary. The transport is internal to
  `raftnode`, uses the NoKV-owned encoded payloads above instead of changing
  repository protobufs, and derives target endpoints from OpenRaft
  `BasicNode.addr`. The testkit starts three tonic transport servers, elects a
  leader, commits a real `RaftCmdRequest`, and verifies all peers apply it.
  The network factory also keeps a per-target tonic channel cache so normal
  replication does not reconnect on every Raft RPC.
- `OpenRaftRegion` exposes NoKV-owned voter-change helpers over OpenRaft
  learner and membership APIs. The in-process testkit covers adding a new voter,
  committing a real metadata KV command to the joined peer, restarting the
  two-voter membership without reseeding a single-node vote, committing again
  after the restarted quorum elects a leader, and removing that voter back out
  of the membership.
- `RaftAdmin` now wires `AddPeer`, `RemovePeer`, and `TransferLeader` onto
  those `OpenRaftRegion` voter-change helpers and returns an updated protobuf
  region descriptor from the service-local topology. Non-OpenRaft apply engines
  still return an explicit `Unimplemented` error for membership RPCs.
  `TransferLeader` accepts idempotent transfer to the current leader, lets a
  target peer elect itself, and routes source-initiated directed transfer to the
  target peer when that peer has a configured admin endpoint.
  `RegionRuntimeStatus` now reports OpenRaft-derived local peer, leader peer,
  and leader/follower state instead of assuming every service endpoint is peer 1.
  `AddPeer` resolves the target peer's OpenRaft `BasicNode.addr` through a
  server-side peer endpoint catalog, and the standalone binary can load that
  catalog from `NOKV_RUST_RAFTSTORE_PEER_ENDPOINTS`. This keeps real tonic
  membership changes from recording unreachable placeholder node addresses.
  Successful Rust admin peer changes now also feed `ExecutionStatus.Topology`
  with applied peer-change records and publish-not-required state, matching the
  existing diagnostic contract when no root scheduler publication is involved.
- `StoreKV` now depends on an async raft-command executor, and the tonic
  service has coverage against both the direct apply engine and
  `OpenRaftRegion`. Read-only commands stay read-only behind the
  `OpenRaftRegion` executor instead of being converted into write proposals.
  Service-level tests now exercise the transaction RPC
  surface through `Prewrite`, `Commit`, `BatchGet`, `Scan`, `BatchRollback`,
  `ResolveLock`, `CheckTxnStatus`, `TxnHeartBeat`, and
  `InstallPreparedMVCCEntries`.
- The standalone Rust raftstore server now boots `OpenRaftRegion` by default
  for both memory and Holt modes. It uses the internal tonic Raft network
  factory and mounts the internal `RaftTransport` service beside StoreKV and
  RaftAdmin, so the process boundary can carry OpenRaft replication traffic
  without changing the public Go protobuf services. Holt mode keeps the
  persistent apply-status sink behind the OpenRaft state machine. Region
  descriptors are now also replicated as an internal NoKV proposal after
  membership changes, so every active Holt peer persists the same
  epoch/peer-list descriptor instead of leaving the update only on the admin
  leader. The binary also accepts explicit region/store/peer identity and a
  non-bootstrap startup mode, so additional processes can start as
  uninitialized joining peers and be added through the existing `RaftAdmin
  AddPeer` wire contract. The repository Docker image now packages this server
  as `/usr/local/bin/nokv-raftstore-server`, but compose still starts the Go
  store path until the Rust data plane owns the full fsmeta smoke and benchmark
  gates.
- The standalone server has an optional coordinator heartbeat path behind
  `NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR`. It reports the existing
  `StoreHeartbeat` wire shape with store identity, client/raft address,
  region count, leader count, leader region ids, and minimal per-region runtime
  stats. Coordinator-returned leader-transfer operations now execute through
  the local `RaftAdmin.TransferLeader` service path; split and merge operations
  are validated and reported as explicit unsupported scheduler outcomes until
  Rust owns those admin RPCs. When the same
  coordinator endpoint is configured, successful Rust `AddPeer` and
  `RemovePeer` operations also persist terminal rooted peer-change events in
  Holt before attempting publication back to the Go coordinator, so a
  temporarily unavailable coordinator does not lose the rooted descriptor
  update after the Rust data plane applies the membership change.
  Startup now also publishes the store membership root event and, for the
  bootstrap peer, the initial region bootstrap descriptor through the same
  coordinator root-event path. Holt mode persists those startup root events in
  the pending queue before publish, so transient coordinator outages are retried
  instead of requiring manual test seeding. Rust topology diagnostics now use
  the same stable transition id shapes as Go for peer changes, range splits,
  and range merges.
- `nokv-raftstore-server` exposes compatible tonic `StoreKV` and `RaftAdmin`
  services, including `WatchApply`, apply status, and a single-region admission
  gate for context, epoch, store, leader, and key-range errors. `StoreKV`
  handlers now execute through the same `RaftCmdRequest` boundary that OpenRaft
  proposals will carry. The tagged Go endpoint harness now covers atomic
  mutate/get/watch, Holt apply-status restart, 2PC, `BatchGet`, `Scan`, and
  `InstallPreparedMVCCEntries` against the Rust server. Rust `Scan` now also
  matches the Go service boundary by rejecting reverse scans as unimplemented.
  Rust `MVCCMaintenance` now validates the whole tombstone batch before
  applying it and reports requested tombstones rather than only keys that
  existed locally, matching the Go raft apply boundary.
  `RegionRuntimeStatus` now rejects missing region ids like the Go admin
  service, and `ExecutionStatus` returns the last `StoreKV` admission decision
  plus restart counts for the hosted single-region runtime.
  StoreKV and RaftAdmin now share one mutable admission state, so successful
  membership changes immediately advance the StoreKV epoch/peer gate instead
  of leaving reads and writes on a stale startup descriptor.
  The server crate now also has multi-region StoreKV/RaftAdmin routers that
  preserve the existing protobuf contract while selecting a hosted region by
  `Context.region_id` or admin `region_id`. Region-scoped requests still pass
  through the per-region admission and runtime-status gates; process-scoped
  `ExecutionStatus` aggregates hosted-region readiness and topology diagnostics.
  `WatchApply` now mirrors Go's prefix projection more closely: buffer 0 maps
  to the default watch buffer, emitted events contain only matching keys, and
  large key sets are split into bounded messages.
  `StoreKV` admission now derives the leader/follower decision from the live
  runtime status instead of trusting the service bootstrap flag, so stale
  leader endpoints reject writes with `NotLeader` and follower-prefer reads keep
  the Go client's `StaleCommand` fallback path.
  Read admission now keeps the follower-prefer fallback contract: when the
  local Rust service is not the leader and follower serving is not wired yet,
  follower-prefer reads return `StaleCommand` so Go clients retry the leader,
  while writes remain leader-only and continue to return `NotLeader`.
  The server/adapter testkit also covers a Holt-backed peer that installs a
  leader snapshot, persists its apply-state and MVCC snapshot, restarts from
  the same Holt/log directories, and applies a later leader commit.
- The tagged Go integration harness now runs the fsmeta contract executor
  through `fsmeta/runtime/raftstore.Runner` against a Rust StoreKV endpoint,
  proving the upper fsmeta semantic path can use the Rust data plane without
  changing fsmeta execution code. The same fsmeta contract now runs against
  both the in-memory Rust store and the Holt-backed Rust store. A Holt restart
  regression now creates a namespace entry through fsmeta, stops the Rust
  process, restarts the same Holt-backed store, and verifies `Lookup` plus
  `ReadDirPlus` still observe the persisted dentry and inode attributes. The
  integration harness also opens the production `fsmeta/runtime/raftstore`
  assembly through a real Go coordinator while the Rust store publishes its
  store/region metadata by heartbeat, then runs fsmeta contract operations
  through coordinator-backed routing. It also covers the same fsmeta runtime
  path after adding a second Rust peer through the existing `RaftAdmin`
  contract: the Go coordinator observes the updated rooted descriptor, fsmeta
  writes route through the coordinator-backed client, and the added peer catches
  up to the leader's apply index.
- The tagged Go client harness now also starts a real Go coordinator service,
  publishes rooted store and region metadata, lets the Rust store populate
  `StoreInfo` through heartbeat, and routes `StoreKV` traffic through the
  coordinator-backed resolver instead of a static in-test resolver.
- The tagged Go raftstore client harness now starts three standalone Rust
  server processes, uses the existing Go `RaftAdmin` client to add and remove
  non-bootstrap peers, writes through the existing Go `StoreKV` client before
  and after membership removal, and verifies remaining follower apply progress
  through admin status. This covers the public wire contract for multi-process
  AddPeer/RemovePeer plus tonic replication rather than only Rust in-process
  tests. Service admission now refreshes from the region descriptor already
  applied by the OpenRaft state machine before layering on the local
  leader/hosted runtime status, so a follower that later becomes leader uses
  the replicated epoch and peer list instead of a stale startup descriptor.
  The Go harness transfers leadership to an added peer after membership
  changes and writes through the existing client to verify that boundary.
- The tagged Go coordinator harness now also verifies the reverse control-plane
  path: a Rust `RaftAdmin AddPeer` call publishes the updated region descriptor
  through the existing Go coordinator root-event service, and subsequent
  coordinator routing observes the new epoch and peer list.
- The tagged Go coordinator harness now also covers coordinator-driven
  scheduler execution for leader transfer: the Go coordinator test double
  returns a `LeaderTransfer` operation in `StoreHeartbeat`, the Rust store
  executes it through its local `RaftAdmin` boundary, peer 2 becomes leader,
  and the existing Go `StoreKV` client continues writing through the updated
  leader.
- The tagged Go coordinator harness also covers the durable retry path: Rust
  persists a pending rooted peer-change event in Holt when the coordinator is
  initially unavailable, returns the successful data-plane membership change,
  and later publishes the descriptor once the coordinator endpoint comes back.
- A tagged Go Holt restart harness now starts two standalone Rust processes,
  adds a non-bootstrap peer, writes through the existing Go client, restarts
  both processes from their Holt and raftlog directories, verifies the persisted
  region descriptor still exposes the two-peer epoch, and writes again after
  the restarted quorum elects a leader.
- The Go CLI now exposes `nokv raft-admin add-peer`, `remove-peer`,
  `transfer-leader`, and `region-status` over the existing `RaftAdmin` service.
  The Docker image also packages `serve-rust-store.sh` and
  `join-rust-raftstore-peers.sh` so one-region Rust parity runs can derive
  identity, peer endpoints, and AddPeer calls from the normal NoKV config
  instead of hand-written environment variables.
- Rust coordinator publication and heartbeat now accept comma-separated
  endpoint lists like the Go deployment config and try endpoints in order,
  keeping `InvalidArgument`/permanent root validation failures terminal while
  allowing transient leader/unavailability failures to fall through to another
  coordinator endpoint.

Known gaps:

- OpenRaft proposal/apply now has in-process three-node replication coverage,
  raftnode-level voter add/remove helpers, and an internal prost codec for
  OpenRaft RPC payloads. The encoded test network now exercises that codec at
  the `RaftNetwork` boundary, and the first tonic transport service/client can
  replicate between local servers. The standalone Rust server now mounts the
  transport beside StoreKV/RaftAdmin and has server-level replication coverage.
  The Go tagged harness also covers three standalone Rust processes joining and
  leaving through `RaftAdmin AddPeer`/`RemovePeer`, then replicating `StoreKV`
  writes across the active membership. The standalone binary can now report
  StoreHeartbeat telemetry to the coordinator, execute coordinator-returned
  leader-transfer operations through the local admin service, and publish
  terminal peer-add/remove descriptors back to the coordinator after successful
  Rust admin membership changes, with a Holt-backed pending root-event queue
  for retry when the coordinator is temporarily unavailable. Permanent
  coordinator validation failures now move the event into a durable
  Holt-backed blocked-event catalog and surface pending/blocked counts through
  `ExecutionStatus.Restart`. Production route integration now has tagged
  coverage for coordinator-backed store discovery, key routing, admin
  descriptor publication, pending descriptor retry, blocked descriptor
  recovery after Rust process restart, and startup root publication for
  coordinator-backed routing; coordinator-owned process lifecycle, operator
  resolution for blocked events, split/merge scheduler operation execution,
  and remaining `RaftAdmin` RPC wiring are still being built out. Unsupported
  split/merge scheduler operations are no longer silently treated as consumed:
  the server validates their required fields and logs an explicit unsupported
  outcome with operation identifiers. Holt-backed Rust stores also retain those
  unsupported scheduler operations in a pending catalog keyed by the full
  operation identity, so different split keys or merge sources do not overwrite
  each other. Scheduler operations that should be executable but currently fail
  at the local admin boundary, such as a transient leader-transfer RPC failure,
  are also persisted into the same pending catalog instead of being dropped
  after one heartbeat attempt. The Rust pending topology retry loop now also
  replays pending scheduler operations through the local admin endpoint and
  removes them from Holt after an applied or permanently invalid result. The
  Go tagged harness covers the successful coordinator-returned leader-transfer
  operation across two standalone Rust processes. The pending count is surfaced
  through `ExecutionStatus.Restart.PendingSchedulerOperationCount`, and each
  pending operation also appears in `ExecutionStatus.Topology` with a stable
  transition id. Rust store heartbeat now also matches the Go coordinator
  client policy:
  it contacts every configured coordinator endpoint and prefers the first
  response that carries scheduler operations over an earlier successful
  no-operation response. Pending root events are also projected into
  `ExecutionStatus.Topology` with `TerminalPending` publish state, while
  blocked events remain terminal and take precedence for the same transition id.
- The default server startup is mounted behind a single-node OpenRaft node;
  additional peers can now start in non-bootstrap mode and join through
  `RaftAdmin AddPeer`. When a coordinator endpoint is configured, startup
  publishes the store membership and bootstrap region descriptor needed for
  coordinator-backed routing. Config-driven one-region process launch and
  AddPeer joining now have ops scripts. The server crate now has a
  multi-region service routing boundary and a config-driven multi-region
  startup path that opens several local OpenRaft groups in one process, isolates
  each region's raftlog directory, publishes store membership once, publishes
  one bootstrap descriptor per bootstrapped region, and sends a single
  aggregate store heartbeat for the process. Multi-region bootstrap now
  requires explicit non-overlapping key ranges before publishing those startup
  descriptors, and the Go tagged harness covers routing through a real
  coordinator using the published descriptors. Coordinator-owned lifecycle,
  split/merge execution, and default compose cutover are still pending.
- Region metadata has a Holt persistence point for descriptors and apply-state
  records, and Holt server mode persists apply status after successful write
  commands. Non-bootstrap Rust peers no longer persist a default descriptor,
  report a hosted region, or expose ready restart counts before OpenRaft
  membership exists; StoreKV admission also rejects those unhosted runtimes as
  not hosted instead of treating them as ordinary followers. Bootstrap peers
  still seed the initial descriptor locally, and full coordinator-provided
  topology bootstrap is still pending.
- Admin `AddPeer`/`RemovePeer` RPCs are wired for `OpenRaftRegion`.
  `TransferLeader` is wired for current-leader no-op and target-peer
  self-election. When the current leader has a configured target peer endpoint,
  the admin service now routes directed transfer to that target peer and waits
  for the target-side election boundary. Missing or unreachable target
  endpoints still return `FailedPrecondition`.
  Terminal AddPeer/RemovePeer descriptors can be published to the coordinator
  when `NOKV_RUST_RAFTSTORE_COORDINATOR_ADDR` is configured. Holt mode now
  persists pending topology events before publication and retries them in the
  background; permanent validation failures are moved from pending into a
  durable blocked-event catalog so they stop retrying silently and remain
  visible in admin diagnostics.
- Restart recovery now covers single-node Holt restart, write-after-restart,
  durable vote/committed metadata, and an in-process multi-node restart after a
  membership change. The Go tagged harness also covers Holt-backed
  multi-process restart after AddPeer and verifies writes can continue after
  the restarted quorum elects any voter as leader. Production completeness
  still needs coordinator-managed lifecycle coverage.
- Snapshot catch-up has both state-machine snapshot coverage and an in-process
  OpenRaft peer catch-up test where a joining peer installs a snapshot after the
  leader purges covered logs. Holt-backed adapter coverage now restarts a
  snapshot-installed peer from persistent apply-state/MVCC state and verifies it
  applies a later leader commit. The tonic transport testkit now also covers a
  joining peer installing a snapshot after the leader triggers snapshot creation
  and purges the covered log prefix, so snapshot catch-up is exercised through
  the real Rust transport boundary. Corrupt snapshot payloads are rejected
  before state-machine mutation in the current unit coverage.
- Go fsmeta and raftstore client Rust-endpoint tests remain behind the
  `rust_raftstore` build tag until the Rust data plane is the default runtime.
- The Docker image can carry the Rust server binary and one-region Rust
  launcher/join scripts, but compose default cutover is still pending
  coordinator-managed multi-region lifecycle wiring and benchmark validation.
- Rust follower reads are intentionally not served locally yet. The service
  preserves the Go client fallback shape for follower-prefer reads, but safe
  follower ReadIndex and bounded-stale serving still require the multi-node
  transport and freshness budget to be wired through raftnode.

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
