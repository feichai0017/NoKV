# TLA Specifications

This directory holds bounded TLA+/TLC models for protocol-level correctness
claims in NoKV. The goal is not function-level coverage. The specs cover
authority boundaries, crash windows, replay rules, and metadata invariants that
ordinary unit tests cannot exhaustively interleave.

## Install

```bash
make install-tla-tools
```

This downloads the pinned `tla2tools.jar` under `third_party/tla/`.

## Run

```bash
make test-tla-smoke
make test-tla-nightly
make record-formal-artifacts
```

`test-tla-smoke` is the CI-sized positive model set. `test-tla-nightly` also
runs the larger contrast matrix whose specs are expected to produce
counterexamples.

## Coverage Matrix

| Spec | Coverage boundary | Main invariants | Related implementation tests |
| --- | --- | --- | --- |
| `Eunomia.tla` | Repeated rooted handoff for the control-plane authority lineage. | Inheritance, Primacy, Silence, Finality. | `coordinator/integration/*`, `meta/root/state/*`. |
| `EunomiaMultiDim.tla` | Lease-start coverage model for served-read handoff. | No write behind a served read after successor issue. | `coordinator/integration/root_model_test.go`. |
| `MountLifecycle.tla` | Rooted fsmeta mount registration and retirement. | No implicit mount, terminal retirement, stable mount identity. | `fsmeta/exec/mount_test.go`, `fsmeta/server/service_test.go`. |
| `SubtreeAuthority.tla` | Namespace subtree authority handoff consumed by subtree rename. | One active authority, successor frontier coverage, sealed-reply rejection, predecessor finality. | `fsmeta/integration/namespace_chaos_test.go`, `fsmeta/contract/*`. |
| `Percolator2PC.tla` | 2PC primary/secondary decision authority and lock lifecycle. | Secondary follows primary, min commit ts, rollback marker exclusion, commit before TTL expiry, GC below live locks. | `percolator/txn_model_test.go`, `percolator/crash_matrix_test.go`, `raftstore/integration/twopc_fault_model_test.go`. |
| `MVCCGC.tla` | MVCC safepoint admission and destructive cleanup. | Safepoint below active snapshots/locks, visible version retained, rollback/default tombstone/version removal only below safepoint. | `raftstore/mvcc/*_test.go`, `percolator/mvcc/gc_test.go`. |
| `RaftstoreApplyPublish.tla` | Raft Ready apply/publish/advance/send and snapshot publish boundary. | Publish requires apply, send requires advance, snapshot publish requires completed install, aborted install is never published. | `raftstore/peer/peer_test.go`, `raftstore/integration/snapshot_interruption_test.go`. |
| `RootReplayWatch.tla` | Root epoch, snapshot replay, follower catch-up, and watch gap reconciliation. | Snapshot/follower/watch cursor never ahead of root, no silent watch gap, replay lag is explicit through pending replay or reconcile. | `coordinator/integration/root_model_test.go`, `meta/root/state/*_test.go`, `fsmeta/exec/watch/*_test.go`. |
| `FSMetaNamespace.tla` | Small root-directory namespace model for create/link/unlink/rename/snapshot/session. | Dentries point to live inodes, link counts match dentries, sessions target live non-expired inodes, snapshots only reference known inodes. | `fsmeta/contract/*`, `fsmeta/integration/history_contract_test.go`. |

## Contrast Models

These specs intentionally omit one safety boundary and should fail under TLC:

- `LeaseOnly.tla`: no reply-side guard and no rooted handover record; expected
  to violate `NoOldReplyAfterSuccessor`.
- `LeaseStartOnly.tla`: no lease-start coverage check on predecessor served-read
  summaries; expected to violate `NoWriteBehindServedRead`.
- `TokenOnly.tla`: bounded-freshness token only; still expected to violate
  `NoOldReplyAfterSuccessor`.
- `ChubbyFencedLease.tla`: per-reply sequencer fencing; expected to preserve
  stale-reply rejection but violate successor coverage.
- `SubtreeWithoutFrontierCoverage.tla`: subtree handoff without successor
  frontier coverage; expected to violate `Inheritance`.
- `SubtreeWithoutSeal.tla`: subtree handoff that leaves the predecessor active;
  expected to violate `Primacy`.

## Policy

New protocol or metadata lifecycle work should update this matrix in the same
patch when it changes one of these boundaries. If the change is implementation
only, update the related Go history/simulation test instead and say why the TLA
state machine is unchanged.

`record-formal-artifacts` stores sanitized TLC outputs under `artifacts/`.
These are bounded model checks, not a TLAPS / Coq / Dafny proof.
