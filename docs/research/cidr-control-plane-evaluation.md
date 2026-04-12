# CIDR Control-Plane Evaluation Plan

> Status: working evaluation plan for a CIDR-style systems paper. The goal is
> not to claim a new consensus algorithm. The goal is to defend a concrete
> systems architecture claim: separating durable metadata truth from a
> rebuildable Coordinator view can reduce failure-domain coupling while keeping
> allocator and routing semantics explicit.

## CIDR Bar

CIDR values systems architecture, experience-based insight, resourceful
experiments, and clear vision. For NoKV, that means every claim below must map
to a runnable test or benchmark in the repository.

Paper constraints to keep in mind:

- 6-page submitted paper, including references and appendix.
- The contribution must be a sharp systems argument, not a feature inventory.
- Evaluation should be small but decisive.

## Core Claim

NoKV uses a Delos-inspired metadata-root log as durable truth and keeps
Coordinator as a rebuildable service/view layer. This enables both co-located
and separated control-plane deployments under one `RootStore` contract while
making freshness, lease ownership, and allocator fences explicit.

## Claims And Required Evidence

| Claim | Evidence | Status |
| --- | --- | --- |
| Coordinator crash does not lose allocator progress in separated mode. | `TestSeparatedModeCoordinatorCrashAndRecoveryPreservesAllocatorFence` | implemented |
| Root leader change does not roll back `CoordinatorLease` or allocator fences. | `TestReplicatedStoreCoordinatorLeaseFenceSurvivesLeaderChange` | implemented |
| Remote root adds RPC overhead, but allocator windows keep it off the hot path. | `BenchmarkControlPlaneAllocID{Local,Remote}Window{Default,One}` | implemented benchmark entrypoint; numbers pending |
| Freshness is explicit rather than inferred from write errors. | protocol/unit/integration tests around `Freshness`, `RootToken`, `CatchUpState`, `DegradedMode` | partially implemented |
| Separated deployment is operationally distinct but shares the same rooted truth contract. | `scripts/dev/separated-cluster.sh`, `scripts/ops/serve-meta-root.sh`, docs | implemented, experimental |

## Benchmark Set

### B1: AllocID Cost Across Access Modes

Command:

```bash
go test ./coordinator/server \
  -run '^$' \
  -bench 'BenchmarkControlPlaneAllocID(Local|Remote)WindowDefault' \
  -benchmem \
  -count 5
```

Purpose:

- Compare in-process local root access with remote gRPC root access.
- Measure the direct service overhead of separated mode when allocator windows
  are enabled.

Expected interpretation:

- Remote should be slower than local for refill operations.
- Windowed steady-state allocation should remain mostly in-memory.

### B2: Allocator Window Write Amplification

Command:

```bash
go test ./coordinator/server \
  -run '^$' \
  -bench 'BenchmarkControlPlaneAllocID(Local|Remote)Window(Default|One)' \
  -benchmem \
  -count 5
```

Purpose:

- `WindowDefault` uses the production default window.
- `WindowOne` intentionally degenerates into one rooted fence write per
  allocation.
- The difference quantifies why allocator windows are required before remote
  `meta/root` can be used seriously.

Expected interpretation:

- `WindowOne` should show much higher latency and allocation/write overhead,
  especially in remote mode.
- The exact numbers are less important than the order-of-magnitude gap and the
  causal explanation.

### B3: Coordinator Crash Recovery

Current runnable test:

```bash
go test ./coordinator/integration \
  -run TestSeparatedModeCoordinatorCrashAndRecoveryPreservesAllocatorFence \
  -count 10
```

Next step:

- Convert this into a timed recovery measurement:
  - old coordinator serves `AllocID`
  - old coordinator crashes without release
  - new coordinator connects
  - first successful `AllocID` latency is recorded

The paper should report this as recovery latency, not as throughput.

## Minimal Paper Shape

1. Problem: control-plane truth, service view, and executor progress are often
   coupled in one operational unit.
2. Design: metadata-root VirtualLog, Coordinator rooted view, freshness
   contract, CoordinatorLease, allocator windows.
3. Implementation: local / replicated / remote root under one `RootStore`
   contract.
4. Evaluation: B1, B2, B3 plus one failure-domain demonstration.
5. Related work: Delos, TiKV/PD, FoundationDB, KRaft.

## What Not To Claim

- Do not claim a new consensus protocol.
- Do not claim stronger performance than TiKV/PD without comparable
  large-scale measurements.
- Do not claim separated mode is production default.
- Do not claim `remote` is a third root backend; it is an access layer over
  `local` or `replicated` authority.

