# RaftStore Deep Dive

`raftstore` powers NoKV’s distributed mode by layering multi-Raft replication on top of the embedded storage engine. This note explains the major packages, the boot and command paths, how transport and storage interact, and the supporting tooling for observability and testing.

---

## 1. Package Structure

| Package | Responsibility |
| --- | --- |
| [`store`](../raftstore/store) | Orchestrates peer set, command pipeline, region manager, scheduler/heartbeat loops; exposes helpers such as `StartPeer`, `ProposeCommand`, `SplitRegion`. |
| [`peer`](../raftstore/peer) | Wraps etcd/raft `RawNode`, drives Ready processing (persist to WAL, send messages, apply entries), tracks snapshot resend/backlog. |
| [`engine`](../raftstore/engine) | WALStorage/DiskStorage/MemoryStorage across all Raft groups, leveraging the NoKV WAL while keeping manifest metadata in sync. |
| [`transport`](../raftstore/transport) | gRPC transport with retry/TLS/backpressure; exposes the raft Step RPC and can host additional services (TinyKv). |
| [`kv`](../raftstore/kv) | TinyKv RPC implementation, bridging Raft commands to MVCC operations via `kv.Apply`. |
| [`server`](../raftstore/server) | `ServerConfig` + `New` that bind DB, Store, transport, and TinyKv server into a reusable node primitive. |

---

## 2. Boot Sequence

1. **Construct Server**
   ```go
   srv, _ := raftstore.NewServer(raftstore.ServerConfig{
       DB: db,
       Store: raftstore.StoreConfig{StoreID: 1},
       Raft: myraft.Config{ElectionTick: 10, HeartbeatTick: 2, PreVote: true},
       TransportAddr: "127.0.0.1:20160",
   })
   ```
   - A gRPC transport is created, the TinyKv service is registered, and `transport.SetHandler(store.Step)` wires raft Step handling.
   - `store.Store` loads `manifest.RegionSnapshot()` to rebuild the Region catalog (router + metrics).

2. **Start local peers**
   - CLI (`nokv serve`) iterates the manifest snapshot and calls `Store.StartPeer` for every region that includes the local store.
   - Each `peer.Config` carries raft parameters, the transport reference, `kv.NewEntryApplier`, WAL/manifest handles, and Region metadata.
   - `StartPeer` registers the peer through the peer-set/routing layer and may bootstrap or campaign for leadership.

3. **Peer connectivity**
   - `transport.SetPeer(storeID, addr)` defines outbound raft connections; the CLI exposes it via `--peer storeID=addr`.
   - Additional services can reuse the same gRPC server through `transport.WithServerRegistrar`.

---

## 3. Command Execution

### Read (strong leader read)
1. `kv.Service.KvGet` builds `pb.RaftCmdRequest` and invokes `Store.ReadCommand`.
2. `validateCommand` ensures the region exists, epoch matches, and the local peer is leader; a RegionError is returned otherwise.
3. `peer.Flush()` drains pending Ready, guaranteeing the latest committed log is applied.
4. `commandApplier` (i.e. `kv.Apply`) runs GET/SCAN directly against the DB, using MVCC readers to honour locks and version visibility.

### Write (via Propose)
1. Write RPCs (Prewrite/Commit/…) call `Store.ProposeCommand`, encoding the command and routing to the leader peer.
2. The leader appends the encoded request to raft, replicates, and once committed the command pipeline hands data to `kv.Apply`, which maps Prewrite/Commit/ResolveLock to the `percolator` package.
3. `engine.WALStorage` persists raft entries/state snapshots and updates manifest raft pointers. This keeps WAL GC and raft truncation aligned.
4. Raft apply only accepts command-encoded payloads (`RaftCmdRequest`). Legacy raw KV payloads are rejected as unsupported.

---

## 4. Transport

- gRPC transport listens on `TransportAddr`, serving both raft Step RPC and TinyKv RPC.
- `SetPeer` updates the mapping of remote store IDs to addresses; `BlockPeer` can be used by tests or chaos tooling.
- Configurable retry/backoff/timeout options mirror production requirements. Tests cover message loss, blocked peers, and partitions.

---

## 5. Storage Backend (engine)

- `WALStorage` piggybacks on the embedded WAL: each Raft group writes typed entries, HardState, and snapshots into the shared log.
- `LogRaftPointer` and `LogRaftTruncate` edit manifest metadata so WAL GC knows how far it can compact per group.
- Alternative storage backends (`DiskStorage`, `MemoryStorage`) are available for tests and special scenarios.

---

## 6. TinyKv RPC Integration

| RPC | Execution Path | Notes |
| --- | --- | --- |
| `KvGet` / `KvScan` | `ReadCommand` → `kv.Apply` (read mode) | No raft round-trip; leader-only.
| `KvPrewrite` / `KvCommit` / `KvBatchRollback` / `KvResolveLock` / `KvCheckTxnStatus` | `ProposeCommand` → command pipeline → raft log → `kv.Apply` | Pipeline matches proposals with apply results; MVCC latch manager prevents write conflicts.

The `cmd/nokv serve` command uses `raftstore.Server` internally and prints a manifest summary (key ranges, peers) so operators can verify the node’s view at startup.

---

## 7. Client Interaction (`raftstore/client`)

- Region-aware routing with NotLeader/EpochNotMatch retry.
- `Mutate` splits mutations by region and performs two-phase commit (primary first). `Put` / `Delete` are convenience wrappers.
- `Scan` transparently walks region boundaries.
- End-to-end coverage lives in `raftstore/server/server_client_integration_test.go`, which launches real servers, uses the client to write and delete keys, and verifies the results.

---

## 8. Control Plane & Region Operations

### 8.1 Topology & Routing
- Topology is sourced from `raft_config.example.json` (via `config.LoadFile`) and
  reused by scripts, Docker Compose, and the Redis gateway.
- The client builds a static region map (`[]RegionConfig`) and store endpoints
  from the same file; there is no dynamic PD-style reconfiguration today.
- The built-in scheduler currently emits leader-transfer operations only
  (see `raftstore/scheduler`), acting as a minimal control plane.

### 8.2 Split / Merge
- **Split**: leaders call `Store.ProposeSplit`, which writes a split
  `AdminCommand` into the parent region's raft log. On apply,
  `Store.SplitRegion` updates the parent range/epoch and starts the child peer.
- **Merge**: leaders call `Store.ProposeMerge`, writing a merge `AdminCommand`.
  On apply, the target region range/epoch is expanded and the source peer is
  stopped/removed from the manifest.
- These operations are explicit and are not auto-triggered by size/traffic
  heuristics; a higher-level controller could call the same APIs.

---

## 9. Observability

- `store.RegionMetrics()` feeds into `StatsSnapshot`, making region counts and backlog visible via expvar and `nokv stats`.
- `nokv regions` shows manifest-backed regions: ID, range, peers, state.
- `scripts/transport_chaos.sh` exercises transport metrics under faults; `scripts/run_local_cluster.sh` spins up multi-node clusters for manual inspection.

### Store internals at a glance

| Component | File | Responsibility |
| --- | --- | --- |
| Store facade | [`store.go`](../raftstore/store/store.go) | Store construction/wiring and shared component ownership (router, region manager, command pipeline, scheduler runtime). |
| Peer lifecycle | [`peer_lifecycle.go`](../raftstore/store/peer_lifecycle.go) | Start/stop peers, router registration, lifecycle hooks, and store shutdown sequencing. |
| Command service | [`command_service.go`](../raftstore/store/command_service.go) | Region/epoch/key-range validation and read/propose request handling. |
| Admin service | [`admin_service.go`](../raftstore/store/admin_service.go) | Split/merge proposal handling and applied admin command side effects. |
| Membership service | [`membership_service.go`](../raftstore/store/membership_service.go) | Conf-change proposal helpers and manifest metadata updates after membership changes. |
| Region catalog | [`region_catalog.go`](../raftstore/store/region_catalog.go) | Public region catalog accessors and region metadata lifecycle operations. |
| Scheduler runtime | [`scheduler_runtime.go`](../raftstore/store/scheduler_runtime.go) | Scheduler snapshot generation, store stats, operation application, and apply-entry dispatch. |
| Peer set | [`peer_set.go`](../raftstore/store/peer_set.go) | Tracks active peers and exposes thread-safe lookups/iteration snapshots. |
| Command pipeline | [`command_pipeline.go`](../raftstore/store/command_pipeline.go) | Assigns request IDs, records proposals, matches apply results, returns responses/errors to callers. |
| Region manager | [`region_manager.go`](../raftstore/store/region_manager.go) | Validates state transitions, writes manifest edits, updates peer metadata, triggers region hooks. |
| Operation scheduler | [`operation_scheduler.go`](../raftstore/store/operation_scheduler.go) | Buffers planner output, enforces cooldown & burst limits, dispatches leader transfers or other operations. |
| Heartbeat loop | [`heartbeat_loop.go`](../raftstore/store/heartbeat_loop.go) | Periodically publishes region/store heartbeats and re-runs the planner to produce scheduling actions. |

---

## 10. Extending raftstore

- **Adding peers**: update the manifest with new Region metadata, then call `Store.StartPeer` on the target node.
- **Follower or lease reads**: extend `ReadCommand` to include ReadIndex or leader lease checks; current design only serves leader reads.
- **Scheduler integration**: pair `RegionSnapshot()` and `RegionMetrics()` with an external scheduler (PD-like) for dynamic balancing.

This layering keeps the embedded storage engine intact while providing a production-ready replication path, robust observability, and straightforward integration in both CLI and programmatic contexts.
