# RaftStore Overview

NoKV's replication layer lives under `raftstore/` and mirrors TinyKV's modular layout. The code is organised into four subpackages, each with a single responsibility:

| Subpackage | Responsibility |
| --- | --- |
| [`store/`](../raftstore/store) | Coordinates Region and peer lifecycles, maintains the region catalog, exposes lifecycle hooks/metrics, and persists Region metadata via the manifest. |
| [`peer/`](../raftstore/peer) | Wraps etcd/raft's `RawNode`, handling Ready processing, log persistence, snapshot application, and Region metadata updates. |
| [`engine/`](../raftstore/engine) | Provides `PeerStorage` implementations (shared WAL, disk, in-memory) and snapshot import/export helpers. |
| [`transport/`](../raftstore/transport) | Implements the gRPC transport with retry/TLS/backpressure support, plus utilities used by integration tests. |

## 1. Store module

`store.Store` is the primary entry point for multi-Raft orchestration:

- **Region catalog** – An internal `regionManager` keeps track of Region metadata and associated peers. At startup it loads `manifest.RegionSnapshot()` to rebuild the catalog.
- **RegionHooks** – Callers can supply `OnRegionUpdate`/`OnRegionRemove` callbacks. The store automatically chains in the built-in `RegionMetrics` recorder so state transitions (`Running → Removing → Tombstone`) are always tracked.
- **Region snapshot & metrics** – `RegionSnapshot()` and `RegionMetrics()` expose the current catalog and aggregated counts. Helpers like `SplitRegion` make future split/merge logic easier to wire.
- **Global registry** – `RegisterStore` records live stores so tools (e.g. CLI) can find them; `Stores()` returns the current list.

## 2. Peer module

`peer.Peer` is a thin wrapper around etcd/raft:

- **Ready pipeline** – Persists entries via the configured `PeerStorage` (typically `engine.WALStorage`), sends outgoing messages, and applies committed entries through the user-provided `ApplyFunc`.
- **Region metadata** – Uses `manifest.CloneRegionMetaPtr` to deep copy Region info whenever it changes, preventing aliasing between components.
- **Snapshot/resend** – Integrates the snapshot resend queue and `RaftLogTracker`, persisting truncation metadata to the manifest.
- **Backpressure** – Works with `utils.WaterMark` to implement `Peer.WaitApplied` and apply-side throttling.

## 3. Engine module

`engine.WALStorage` leverages the DB's WAL manager and manifest so raft groups share the same durability path:

- Typed WAL records (entries/state/snapshot) rebuild `MemoryStorage` on replay and update `manifest.RaftLogPointer`.
- Truncation helpers (`LogRaftTruncate`, `MaybeCompact`, snapshot export/import) keep manifest metadata consistent.
- Tests cover slow followers, snapshot round trip, and injected failures so WAL GC never removes segments prematurely.

Disk-based (`DiskStorage`) and in-memory (`MemoryStorage`) implementations exist for specialised scenarios (testing, single-node workflows).

## 4. Transport module

`transport.GRPCTransport` handles cross-node communication:

- Supports configurable retry, TLS, dial/send timeouts, and peer blocking/unblocking (used by chaos tests).
- Unit and integration tests cover replication, network partitions, and backpressure propagation.

## 5. Region metrics & CLI

- `StatsSnapshot`, expvar, and `nokv stats` now include Region state counts (`region_total/new/running/removing/tombstone/other`).
- `nokv regions --workdir <dir> [--json]` dumps the manifest-backed Region catalog (ID, state, key range, peers).
- `nokv stats --no-region-metrics` disables the live metrics attachment and reports manifest-only figures.

## 6. Roadmap

- **Split/Merge flow** – Use `SplitRegion`, `RegionHooks`, and `RegionMetrics` to tie parent/child metadata updates into real split/conf-change code paths.
- **Scheduler integration** – Feed `RegionSnapshot()` and metrics into a higher-level scheduler or monitoring stack.
- **CLI extensions** – Add filtering/inspection (by state or key range) and richer Region summaries aligned with scheduler needs.

For additional context, see:

- [`docs/architecture.md`](architecture.md) for the high-level system overview.  
- [`docs/manifest.md`](manifest.md) for Region metadata persistence details.  
- [`docs/stats.md`](stats.md) for the observability pipeline and CLI output.
