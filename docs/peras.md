# Peras Semantic Commit Path

## TL;DR

- Peras is fsmeta's semantic visible-commit path, not a replacement for Raft,
  Percolator, or the local storage engine.
- The compiler lowers one fsmeta operation into a conservative semantic IR. It
  does not execute reads, allocate timestamps, or weaken executor validation.
- The executor materializes that IR with concrete effects, predicate proofs, and
  guard proofs before holder admission.
- A visible Peras ack means the active holder accepted the operation and exposed
  it through its overlay. Durable publication still requires segment witness,
  raftstore install, and, when configured, a rooted seal.
- Peras authority is a fsmeta-specific root-issued grant object. It is separate
  from Eunomia's current production coordinator duties (`alloc_id`, `tso`, and
  `region_lookup`), although both address authority-continuation gaps.

## 1. Where Peras Sits

The normal fsmeta execution chain is:

```text
fsmeta/client
  -> fsmeta/server
  -> fsmeta/exec.Executor
  -> fsmeta/runtime/raftstore
  -> raftstore/client
  -> raftstore/kv
  -> txn/percolator or Peras install
  -> local.DB.ApplyInternalEntries
  -> WAL + memtable + SST
```

Peras is the alternative write path inside `fsmeta/exec` and
`fsmeta/runtime/*` for operations that the compiler can prove safe for
visible commit. The ordinary Percolator/AtomicMutate path remains the fallback
for range reads, durability barriers, cross-bucket moves, shared quota paths,
dynamic write sets, and any operation whose predicates or guards cannot be
proved.

At the distributed layer, Peras still enters raftstore as a normal replicated
command: `CMD_PERAS_INSTALL_SEGMENT`. The storage engine never learns fsmeta
semantics; it persists internal entries through the same WAL/LSM pipeline as
other MVCC writes.

## 2. Compiler Boundary

The compiler lives in `fsmeta/exec/compile`.

`SemanticDelta` is the executor-facing mutation contract for one fsmeta
operation. It records:

- static read predicates;
- static or derived write effects;
- authority scope;
- runtime guards;
- slow-path reason;
- watch and durability requirements.

`CompiledOp` is the segment-installable descriptor derived from the delta. It
adds key footprint, placement plan, predicate obligations, guard obligations,
effect plan, atomicity group, durability class, watch projection, completion
plan, and segment plan.

`MaterializedOp` is the closed IR admitted by Peras. It must carry concrete
effects and any required predicate/guard proofs. `ValidateForAdmission` is the
key correctness gate: it rejects non-canonical descriptors, prefix reads,
dynamic effects, uncovered keys, missing observed-value proofs, and missing or
mismatched guard proofs.

The compiler is intentionally conservative. If a future change makes a request
look eligible without enough static key coverage, the right fix is to mark it
slow-path or require materialization, not to relax validation.

## 3. Visible Commit Flow

For an eligible operation such as a simple `Create`:

1. The executor resolves mount identity and any required external authority
   inputs, such as inode allocation.
2. The compiler emits a `SemanticDelta`.
3. If all effects are already concrete, the executor builds a `MaterializedOp`
   directly. If the write set depends on current state, `perasReadView` reads
   the merged overlay/base view, records observed predicates, and materializes
   concrete effects.
4. `tryPerasVisibleCommit` calls `SubmitVisible`.
5. Runtime acquires a root-issued Peras authority grant for the fsmeta scope.
6. The admission callback rechecks predicates against overlay plus base storage
   and returns guard proofs.
7. The holder records the operation under a stable `OperationID`, checks pending
   conflicts, and returns a visible ack.
8. The runtime adds concrete effects to the overlay and publishes visible watch
   events when the operation requested visible emission.

At this point the operation is visible to this holder's Peras-aware read path,
but it is not yet durable in raftstore.

If admission is rejected before the overlay changes, the executor falls back to
ordinary transaction execution. If an unexpected error occurs after Peras may
have changed holder state, the executor returns the error instead of retrying on
the ordinary path, avoiding double-apply ambiguity.

## 4. Segment Flush And Install

Flush is ordered:

1. freeze each holder's pending replay plan;
2. split plans by compiler segment budget and merge key;
3. build a `PerasSegment`;
4. append witness records to the configured witness quorum;
5. install the segment through raftstore;
6. optionally publish a root seal;
7. mark the holder replay plan applied and remove overlay state covered by the
   installed segment.

A Peras segment is not just a KV batch. It contains coalesced final key state
plus completion records for every operation. Each completion carries the
descriptor digest, predicate-proof digest, and execution-plan digest. Coalescing
therefore reduces installed key entries without erasing per-operation completion
evidence.

The raftstore adapter installs a segment by routing one or more
`PerasInstallSegment` commands. Catalog-only install writes segment object and
index records. Materialized install expands segment entries into MVCC internal
entries. Both modes are applied through raftstore and ultimately through
`local.DB.ApplyInternalEntries`.

`InstallVersion` is currently reserved inside the raftstore Peras installer with
a process-local atomic counter. Treat that as an implementation detail to
revisit before extending materialized MVCC installs across restart or multiple
holder-version domains.

## 5. Authority, Fence, And Recovery

Peras authority is rooted in `meta/root/protocol.PerasAuthorityGrant`, which is
explicitly separate from the coordinator service `AuthorityGrant` used by the
Eunomia duties.

The authority manager talks to coordinator, while coordinator applies the
root-backed Peras command:

- `Acquire` creates or returns an active grant, enforcing primacy by overlapping
  mount/bucket scope.
- `Retire` removes an owned active grant.
- `Seal` records segment root, payload digest, operation count, entry count, and
  raftstore install cursor.

The root seal is evidence, not the data body. Segment bytes stay in raftstore's
catalog.

While a Peras grant covers a key, raftstore apply uses the Peras authority fence
to reject ordinary write commands touching that key. `GET`, `SCAN`, and
`CMD_PERAS_INSTALL_SEGMENT` are not fenced. This prevents fsmeta mutations from
being split between visible Peras state and ordinary Percolator state for the
same authority scope.

On successor holder startup, runtime first loads installed/root-sealed segments.
If a grant has a predecessor, it can probe witnesses for missing predecessor
segments, verify witness records and payload digests, and reinstall valid
segments before accepting new holder state.

## 6. Relationship To Eunomia

Eunomia addresses a broader detached-control-plane problem: root truth and
serving logic are separated, so authority handoff can leave old replies,
unsealed frontiers, successor inheritance, client verifier floors, and
execution-plane epochs in transition.

Current production Eunomia duties are only:

- `alloc_id`;
- `tso`;
- `region_lookup`.

Peras authority is not one of those registered production duties. It is a
fsmeta-specific root object that uses the same architectural idea: root-issued
bounded authority, holder-local serving, explicit retirement/seal evidence, and
successor recovery from rooted or witnessed frontier.

Do not describe Peras as "Eunomia already generalized to fsmeta namespace duty"
unless the duty registry, client verifier rules, audit rules, and bounded
handoff contract are actually implemented for that duty.

## 7. Common Correctness Misreads

1. **"Peras replaces Raft."** Incorrect. Raft still orders and replicates segment
   install commands inside raftstore.
2. **"The compiler is an interpreter."** Incorrect. It builds conservative IR;
   executor materialization performs runtime reads and proof construction.
3. **"Visible ack means durable commit."** Incorrect. It means holder admission
   plus overlay visibility. Durability requires witness and raftstore install.
4. **"Witness quorum is Raft quorum."** Incorrect. Witnesses provide segment
   evidence and recovery material; raftstore consensus still owns replicated
   install.
5. **"Root seal stores segment data."** Incorrect. Root stores digest frontier
   and install cursor; segment payload remains in raftstore catalog.
6. **"Segment coalescing loses operation history."** Incorrect. Final key state
   is coalesced, but completion records retain per-operation digests.
7. **"Derived caches are truth."** Incorrect. negative-cache and dirpage cache are
   derived state and can be rebuilt. They must not be used as authority evidence.
8. **"Peras fast path covers all fsmeta writes."** Incorrect. The compiler keeps
   many operations on slow path by design.

## 8. Code Map

| Area | Files |
| --- | --- |
| Compiler IR and validation | `fsmeta/exec/compile/*` |
| Executor Peras admission/read view | `fsmeta/exec/peras_admission.go`, `fsmeta/exec/peras_read_view.go` |
| Holder, conflict detector, overlay, segment format | `fsmeta/exec/peras/*` |
| Runtime authority, flush, recovery | `fsmeta/runtime/peras/*` |
| Raftstore adapter, segment installer, witnesses | `fsmeta/runtime/raftstore/peras_*.go` |
| Raftstore command/apply support | `raftstore/kv/peras_*.go`, `raftstore/peras/*`, `raftstore/store/command_apply_plan.go` |
| Root Peras authority | `meta/root/protocol/peras.go`, `meta/root/state/peras.go`, `meta/root/replicated/store.go` |
| Coordinator Peras authority RPC | `coordinator/server/service_peras.go`, `coordinator/rootview/root.go` |

## 9. Focused Test Map

When changing this area, start with focused tests before broader suites:

```bash
go test ./fsmeta/exec/compile ./fsmeta/exec/peras ./fsmeta/runtime/peras ./fsmeta/runtime/raftstore ./raftstore/peras ./raftstore/kv ./meta/root/protocol ./meta/root/state ./meta/root/replicated ./coordinator/server -run 'Test.*(Peras|Segment|Authority|Witness|Compile)' -count=1
go test ./fsmeta ./fsmeta/exec ./fsmeta/runtime/raftstore ./fsmeta/client ./fsmeta/server -run 'Test.*(Create|ReadDir|Snapshot|Rename|Watch|Quota|Session|Peras)' -count=1
go test ./raftstore/kv ./raftstore/store ./raftstore/client ./raftstore/peer -run 'Test.*(Peras|CommandApply|ApplyBatch|Install|Fence|Route|Peer|Read|Propose)' -count=1
go test ./local ./engine/wal ./engine/lsm -run 'Test.*(ApplyInternalEntries|WAL|MemTable|Manifest|Recovery|SetBatch|Replay)' -count=1
```
