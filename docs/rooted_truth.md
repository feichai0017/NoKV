# Rooted Truth — `meta/root`

The `meta/root/` tree implements NoKV's **rooted truth kernel**: a typed, append-only event log whose committed tail is the single source of truth for cluster-level metadata (coordinator leases, allocator fences, region lifecycle, pending peer/range changes).

> If the distributed system has a "brain", it does **not** live in the coordinator. It lives here. The coordinator is a service+view on top of this log.

---

## 1. Why a separate truth layer

In a typical multi-raft system, the metadata used by the control plane (routes, TSO, leases, scheduling decisions) is either:

1. Stored inside one of the raft groups (mixed with user data)
2. Owned by a single coordinator node (coordinator becomes the bottleneck)
3. Split across ad-hoc persistence files

NoKV makes it **explicit**: there is a small, typed metadata log with its own durability and replication shape, and everything control-plane-related goes through it. This matches the "virtual consensus" pattern (Delos-lite): the log is the truth; services above are views that can be rebuilt.

The benefits are concrete:

- **Coordinator is stateless at restart** — the only persistent thing about a coordinator is its configured holder ID; everything else is rebuilt from `meta/root` on boot
- **The log can be swapped** between local (single-node) and replicated (embedded-raft) backends without changing coordinator code
- **Authority handoff is auditable** — every lease grant / seal / closure event is a committed log record with a cursor

---

## 2. Package layout

```
meta/
├── root/
│   ├── protocol/       # Pure protocol types (Cursor, Frontiers, Handoff, Witness, ...)
│   ├── event/          # Typed events (KindStoreJoined, KindCoordinatorLease, ...)
│   ├── state/          # Compact applied state (State, Snapshot, ApplyEventToSnapshot)
│   ├── materialize/    # Helpers that build Snapshot from raw events
│   ├── storage/        # Virtual log file layout + checkpoint format
│   │   └── file/       # Actual on-disk file operations
│   ├── backend/
│   │   ├── local/      # Single-node file-backed log
│   │   └── replicated/ # Embedded raft-backed log (quorum durability)
│   └── remote/         # gRPC service + client for remote rooted access
└── wire/               # proto <-> Go conversions (Event, Snapshot, Cursor)
```

---

## 3. What lives in rooted state

[`meta/root/state/state.go`](../meta/root/state/state.go) defines `State`, the applied snapshot. Everything the control plane cares about is here:

```go
type State struct {
    ClusterEpoch       uint64         // bumped on topology event
    MembershipEpoch    uint64         // bumped on store join/leave
    LastCommitted      Cursor         // highest committed (term, index)
    IDFence            uint64         // globally fenced ID allocator floor
    TSOFence           uint64         // globally fenced TSO allocator floor
    CoordinatorLease   CoordinatorLease
    CoordinatorSeal    CoordinatorSeal
    CoordinatorClosure CoordinatorClosure
}
```

`Snapshot` wraps `State` together with descriptors and pending peer/range changes:

```go
type Snapshot struct {
    State               State
    Descriptors         map[uint64]descriptor.Descriptor
    PendingPeerChanges  map[uint64]PendingPeerChange
    PendingRangeChanges map[uint64]PendingRangeChange
}
```

Every event kind has a deterministic effect on `Snapshot`. See [`meta/root/state/snapshot_apply.go`](../meta/root/state/snapshot_apply.go) and [`meta/root/state/state.go:ApplyEventToState`](../meta/root/state/state.go).

---

## 4. The Append protocol

The core interface any backend must satisfy:

```go
type Backend interface {
    Snapshot() (rootstate.Snapshot, error)
    Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error)
    FenceAllocator(ctx context.Context, kind AllocatorKind, min uint64) (uint64, error)
}
```

`Append` does five things atomically:

1. Validate events against the current `Snapshot` (reject duplicate region IDs, invalid transitions, stale epochs)
2. Assign each event a committed cursor `(Term, Index)`
3. Persist the batch to the backing log
4. Persist an updated compact `Checkpoint`
5. Advance in-memory `State` + `Descriptors` + pending maps

After `Append` returns successfully, callers can observe the new state via `Snapshot()`. The `CommitInfo.Cursor` they get back is the globally ordered cursor for the **last** event in the batch.

`FenceAllocator` is separate because it's an **authoritative minimum** — backends may promote the fence further (e.g., to account for outstanding windows) but must never return a value below `min`.

---

## 5. Single backend: replicated

NoKV ships one meta-root backend: the 3-peer raft-replicated cluster.
Historical single-process "local" backend has been removed.

[`meta/root/replicated/store.go`](../meta/root/replicated/store.go) — embedded raft library, quorum-durable commits.

- exactly 3 replicas, one leader
- `Append` proposes a raft log entry; returns after it's committed to quorum
- Non-leader nodes reject `Append` with `codes.FailedPrecondition`
- Leader changes trigger `IsLeader()` / `LeaderID()` state updates that coordinator consumes
- On-disk state per peer: `root.events.wal`, `root.checkpoint.binpb`, `root.raft.bin`
  (raft hard state + snapshot + retained entries)

---

## 6. Coordinator commands — how lease/seal/closure flow in

In addition to "raw" events, backends expose command APIs for control-plane-specific operations:

```go
ApplyCoordinatorLease(ctx, cmd CoordinatorLeaseCommand)
    (CoordinatorProtocolState, error)

ApplyCoordinatorClosure(ctx, cmd CoordinatorClosureCommand)
    (CoordinatorProtocolState, error)
```

These are **validated, typed writes** that internally:

1. Validate the command against current state (e.g., seal requires active lease, confirm requires prior seal)
2. Emit the appropriate `KindCoordinatorLease` / `KindCoordinatorSeal` / `KindCoordinatorClosure` event
3. Append through the normal log path
4. Return the new `CoordinatorProtocolState = { Lease, Seal, Closure }`

Command-level validation lives in [`meta/root/state/coordinator_lease.go`](../meta/root/state/coordinator_lease.go).

---

## 7. Tail subscription — how coordinator consumes

Coordinators don't poll `Snapshot()` — they subscribe:

```go
sub := rootstorage.NewTailSubscription(afterToken, waitFn)
advance, err := sub.Next(ctx, fallback)
if advance.Action == rootstorage.TailCatchUpAction_Reload {
    // backend advanced far; reload snapshot
} else if advance.Action == rootstorage.TailCatchUpAction_Bootstrap {
    // backend advanced past our retention window; install from compact state
}
sub.Acknowledge(advance)
```

[`meta/root/storage/virtual_log.go`](../meta/root/storage/virtual_log.go) defines:

- `TailToken` — opaque position in the log
- `TailAdvance` — either new events, a reload signal, or a bootstrap install
- `TailSubscription` — stateful iterator that survives across reloads

This is what lets `coordinator/` run as a thin service without duplicating rooted storage.

---

## 8. Recovery model

On coordinator boot:

1. Open the replicated backend (each peer via `rootreplicated.Open`, or connect through `coordinator/rootview` for the client side)
2. Call `Snapshot()` — backend replays/bootstraps internally
3. Build a `TailSubscription` from the snapshot's `LastCommitted`
4. Start the lease campaign loop, which will eventually `ApplyCoordinatorLease(Issue)` when it's leader

If the backend file is corrupted, the coordinator fails fast — it does **not** try to reconstruct rooted state from raftstore local metadata. The two are deliberately partitioned.

---

## 9. Remote access

`meta/root/remote/` provides a gRPC service + client. This exists so a raftstore store can read rooted state without being colocated with the replicated backend:

- `RemoteRootService` serves `Snapshot`, `Append`, `WaitForTail`, `ObserveTail`, etc.
- `RemoteRootClient` implements the same `rootBackend` interface by calling over gRPC
- Leader redirect is automatic: if the target returns `NotLeader`, client re-dials the returned leader

This is what keeps `coordinator/` deployable separately from the rooted log, if you ever want to.

---

## 10. Source map

| File | Responsibility |
|---|---|
| [`meta/root/protocol/types.go`](../meta/root/protocol/types.go) | Pure protocol types (no persistence logic) |
| [`meta/root/event/types.go`](../meta/root/event/types.go) | Typed event constructors |
| [`meta/root/state/state.go`](../meta/root/state/state.go) | `State`, `Snapshot`, `ApplyEventToSnapshot` |
| [`meta/root/state/coordinator_lease.go`](../meta/root/state/coordinator_lease.go) | Lease/Seal/Closure validation + digest |
| [`meta/root/state/transition.go`](../meta/root/state/transition.go) | Cross-event transition rules |
| [`meta/root/storage/virtual_log.go`](../meta/root/storage/virtual_log.go) | Tail subscription + checkpoint primitives |
| [`meta/root/replicated/store.go`](../meta/root/replicated/store.go) | The only backend: 3-peer raft-replicated meta-root |
| [`meta/root/remote/service.go`](../meta/root/remote/service.go) | gRPC service + client for meta-root |
| [`meta/wire/root.go`](../meta/wire/root.go) | proto ↔ Go conversions |

Related docs:

- [Coordinator](coordinator.md) — how the control plane consumes rooted state
- [Control and Execution Plane Protocols](control_and_execution_protocols.md) — the full contract between `meta/root`, `coordinator/`, and `raftstore/`
- [Migration](migration.md) — how the seeded→distributed flow bootstraps rooted state
