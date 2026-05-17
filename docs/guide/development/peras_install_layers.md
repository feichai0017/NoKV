<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Peras Install — Layer Composition Design

Status: staged refactor. Phases 1, 1.5, 1.6, 1.7, 2, 3, 4, and the
encoding-skip part of Phase 6 are implemented on the
`peras-rooted-snapshot-refs` branch. The remaining work is witness-layer
extraction, configuration cleanup, and the TLA model. The target shape is
unchanged: single-node and distributed runtimes should pay only for the
install responsibilities they actually consume.

## Why

Today `Runtime.installSegment` and `Config.Installer` together perform four
distinct responsibilities in a single welded path:

1. Apply the segment's entries to base MVCC.
2. Add the segment to the in-memory `sealedSegments` slice and `sealed`
   `OverlayView` so subsequent reads can merge it.
3. Write the encoded segment payload + index keys into the catalog records.
4. Send the segment digest to witnesses for quorum signing.

Distributed Peras needs all four. Local Peras needs only #1. The current
shape forces local to pay every cost that exists for witness/handoff
correctness even though there is no witness and no handoff.

The observable effects of running the full path on a single-node deployment:

- ~6 GB cumulative alloc from segment encoding (snappy + predicate proofs +
  completion encoding) — see profile in `peras-rooted-snapshot-refs` branch.
- `sealedSegments` slice grows unbounded; long-running processes
  eventually exhaust RAM. There is no retire / drop path.
- Catalog `peras` keys accumulate in base MVCC forever; the catalog has no
  `Delete` operation in the install protocol.
- Recovery `LoadInstalledSegments` rebuilds the entire `sealedSegments`
  slice from the catalog on every restart, even though for single-node the
  source of truth is the visible WAL replay.

This design replaces the welded path with a chain of composable layers.

## Boundary

The layer-composition part of this change is bounded to:

- `fsmeta/runtime/peras` — internal refactor; the public `PerasCommitter`
  surface stays the same.
- `fsmeta/runtime/local` — wire the chain to local-only install layers.
- `fsmeta/runtime/raftstore` — wire the chain to the full four-layer chain.

The wider branch also contains local runtime, snapshot-token, LookupPlus, and
benchmark-support work. The install-layer refactor must not reinterpret
compiler semantics, and distributed runtime behavior must stay byte-identical
unless a later phase explicitly changes the distributed contract.

## Runtime lifecycle

Peras segment flush has three separate boundaries. The refactor must keep
them separate; otherwise local cleanup can accidentally weaken distributed
read safety.

1. **Pre-publish durable install.** Write durable material that can survive a
   crash: base MVCC entries, catalog payload/index records, and witness quorum
   evidence. This stage may run before root seal because its effects are not
   necessarily exposed to the read view yet.
2. **Publish/seal boundary.** Root seal is the authority boundary for
   distributed Peras. A segment is not globally published until this step
   succeeds. This is a runtime transition, not an install layer.
3. **Post-publish finalize.** Make the segment visible to runtime reads and
   retry dedup: sealed overlay, sealed segment tracking, completion index,
   metrics, and visible WAL applied marker. This stage must run only after
   the segment's read source is valid: root-sealed catalog/witness for
   distributed, or base MVCC materialization for local.

Current code keeps this split explicit. Distributed installs still use
catalog/witness/root-seal evidence before finalizing read state. Local installs
now materialize directly into base MVCC, skip catalog records, and recover
retry deduplication from visible WAL applied state.

## Layer Interfaces

```go
// fsmeta/runtime/peras/ports.go and finalize_chain.go

// SegmentInstallLayer is one step in the pre-publish durable install
// pipeline. It is currently an alias for SegmentInstaller so existing
// runtime wiring keeps working.
type SegmentInstallLayer = SegmentInstaller

type SegmentInstallRequest struct {
    Scope           compile.AuthorityScope
    Segment         fsperas.PerasSegment
    Payload         []byte
    PayloadDigest   [32]byte
    Install         compile.InstallPlan
    MaterializeMVCC bool
}

type SegmentInstaller interface {
    InstallSegment(context.Context, SegmentInstallRequest) (InstallCursor, error)
}

// SegmentFinalizeLayer is one step in the post-publish runtime finalize
// pipeline. It must not create new durability evidence. It only updates the
// local runtime read/dedup state after the segment is safe to observe.
type SegmentFinalizeLayer = SegmentFinalizer

type SegmentFinalizeRequest struct {
    Scope         compile.AuthorityScope
    Plan          fsperas.ReplayPlan
    Segment       fsperas.PerasSegment
    InstallCursor InstallCursor
    MaterializeMVCC bool
}

type SegmentFinalizer interface {
    FinalizeSegment(context.Context, SegmentFinalizeRequest) error
}
```

### Concrete layers

```
MVCCEntryLayer
  Stage:          pre-publish durable install.
  Responsibility: write segment entries as MVCC mutations to local.DB.
  Inputs needed:  Segment.entries (raw, not encoded), Scope (for primary key).
  Side effects:   runner.MutateAtCommit; assigns InstallCursor with
                  commit version on success.
  Failure mode:   safe — no other layer has run yet; install retries cleanly.

CatalogLayer
  Stage:          pre-publish durable install.
  Responsibility: write segment payload + catalog index keys to base MVCC.
  Inputs needed:  Payload, PayloadDigest, Install.CanonicalObjectKey.
  Pre-condition:  Payload must be populated; chain inserts SegmentEncodeLayer
                  before CatalogLayer when present.
  Failure mode:   safe if MVCCEntryLayer already ran — entries are durable;
                  catalog record is missing but visibleLog replay can recover.
  Notes:          distributed-only.

SealedTrackingLayer
  Stage:          post-publish finalize.
  Responsibility: add segment to c.read.sealed OverlayView and to
                  c.read.sealedSegments slice.
  Inputs needed:  Segment.
  Failure mode:   should not fail in practice; in-memory ops only.
  Notes:          distributed-only after local materializes to base MVCC.
                  Skipping this layer in local before materialize is enabled
                  would make flushed catalog-only segments invisible to reads.
                  This layer must run after publish/seal, not in the current
                  pre-publish installBatch stage.
                  Skipping this layer in local means
                  reads never consult sealedSegments, which is fine because
                  MVCCEntryLayer already put the data in base MVCC.

WitnessSignLayer
  Stage:          pre-publish durable evidence.
  Responsibility: send segment witness records to configured witnesses;
                  collect signatures up to quorum.
  Inputs needed:  Segment.Root, PayloadDigest, EpochID, witness list.
  Pre-condition:  CatalogLayer has run (witnesses identify segments via
                  catalog references).
  Failure mode:   abort install — without quorum the segment is not durable
                  by Peras' rules; visible commit must be marked failed.
  Notes:          distributed-only. SegmentWitnessMode=Bypass means this
                  layer is not in the chain.

CompletionIndexLayer
  Stage:          post-publish finalize.
  Responsibility: update c.read.completed map for op deduplication.
  Inputs needed:  Segment.Completions.
  Failure mode:   in-memory only; failure is a programming error.
  Notes:          both runtimes use this — op-id-based deduplication. The
                  layer runs in finalize, after the segment's read source is
                  valid.
```

### Encoding as a derived input

`Payload` and `PayloadDigest` are only needed if an install layer or witness
path needs the encoded segment. The Runtime computes them lazily:

```go
needsEncoding := runtime.flushNeedsSegmentPayload()
if needsEncoding {
    payload, digest := fsperas.EncodePerasSegment(segment)
    op.Payload = payload
    op.PayloadDigest = digest
}
```

`SegmentPayloadRequirement` is the capability that lets an installer declare
whether it needs the encoded segment payload. Unknown installers default to
`true` for safety. Local's chain is a single `MVCCEntryLayer`, which returns
false, so local flushes skip segment encoding entirely. Distributed keeps
encoding because catalog and witness evidence still need payload identity.

## Runtime composition

```go
// fsmeta/runtime/local/runtime.go
install := peras.NewInstallChain(
    local.MVCCEntryLayer(runner),
)
materializeSegments := true

// fsmeta/runtime/raftstore/open.go (unchanged behavior)
install := peras.NewInstallChain(
    peras.MVCCEntryLayer(runner),
    peras.CatalogLayer(runner),
    peras.WitnessSignLayer(witnesses),
)
finalize := peras.NewFinalizeChain(
    peras.SealedTrackingLayer(runtime),
    peras.CompletionIndexLayer(),
)
```

The order matters for failure-mode reasoning:

- MVCCEntryLayer first: local materialization must be the first durable
  candidate because it owns the base-MVCC cursor when enabled.
- CatalogLayer before WitnessSignLayer: witnesses sign a segment whose payload
  and catalog identity are already known.
- Root publish/seal after install evidence: distributed read visibility is not
  valid until authority accepts the seal.
- SealedTrackingLayer after publish/seal: adding a segment to the sealed
  overlay exposes it to reads, so it cannot run in pre-publish `installBatch`.
- CompletionIndexLayer after the segment is readable: retry dedup must not
  report an op complete before the segment's read source exists.
- Visible WAL applied marker last: once this marker is durable, recovery may
  skip replaying the visible record, so all earlier finalize work must have
  succeeded.

## Recovery

`LoadInstalledSegments` becomes a method on CatalogLayer specifically:

```go
type recoverableLayer interface {
    LoadInstalled(ctx context.Context, scope compile.AuthorityScope) error
}
```

`Runtime.Open` iterates the chain and calls `LoadInstalled` on each layer
that implements it. Local's chain has no CatalogLayer, so startup does not
scan Peras catalog keys. Local recovery uses the visible WAL instead:

- pending visible records are restored into holder/overlay state;
- applied visible records rebuild the completion index, so retrying an
  already-applied operation after restart returns the original ack instead of
  re-executing it;
- base MVCC remains the read source for materialized entries.

The local WAL-backed visible log therefore must support both applied-marker
writes and state replay, and it retains visible operation records after writing
applied markers. That is intentional: without catalog records, those retained
records are the durable completion evidence. Future visible-WAL GC must first
have another durable completion-retention boundary before it can compact them.

Distributed recovery remains catalog/root/witness based. Root-sealed witness
recovery still treats catalog payload identity as distributed evidence and is
not replaced by the local visible-WAL shortcut.

## Target configuration cleanup

The refactor makes these `Config` fields derivable from chain composition, but
the fields are not all deleted yet:

- `MaterializeSegments` — implied by whether the runtime uses a base-MVCC
  install layer plus local finalize, or catalog/witness install plus
  distributed finalize.
- `CatalogOnlyAuthorityDrain` — implied by chain composition.
- `SegmentWitnessMode` enum — replaced by "is WitnessSignLayer in the chain".

`SegmentInstaller` remains the exported runtime boundary. The canonical wiring
is chain composition, and unknown installers are treated conservatively as
payload-requiring until they declare otherwise.

## Migration plan

| Phase | Scope | Lines (est.) | Risk | Status |
|---|---|---|---|---|
| 1 | Introduce `SegmentInstallLayer` interface and `NewInstallChain` constructor. Make `NewRuntime` wrap `cfg.Installer` so future phases can compose multiple layers without further changes. | ~150 | low — no behavior change | **landed** (`3885377e`) |
| 1.5 | Compiler unblock for cross-bucket materialize. Add `Config.MaterializeMaxReplayMutations` so callers can override the distributed-routing 20-mutation cap. Distributed defaults preserve current behavior. | ~190 | low | **landed** (`6ed3d704`) |
| 1.6 | Cross-shard install path: `runner.InstallMutationsAtCommit` + `applyInstallMutationGroup` bypass the percolator atomicity guard. Auto-chunk under the local DB's `MaxBatchCount` / `MaxBatchSize` budget. Percolator commits keep the guard. `LSMShardCount=4` default preserved. | ~218 | medium | **landed** (`fcbb0f8b`) |
| 1.7 | Catalog-skip in `localPerasSegmentInstaller` when `MaterializeMVCC=true` — when data lives in base MVCC the catalog record is redundant for local-only recovery. Catalog-mode (materialize=false) unchanged. **Default stays MaterializeSegments=false** until Phase 4 detaches install from the catalog pipeline entirely. | ~60 | low | **landed** (`fb685424`) |
| 2 | Extract MVCCEntryLayer (local) + CompletionIndexLayer (both runtimes). Chain semantics relaxed to "first valid cursor wins" so the cursor producer switches on `MaterializeMVCC`. Single `readState.mergeCompletions` helper owns the completion index — chain layer uses it on the live path, recovery's `LoadInstalledSegments` (which bypasses the chain because the catalog already exists) calls it explicitly. | ~270 | low — split, no behavior change on default settings | **landed** |
| 3 | Split pre-publish install from post-publish finalize. Move SealedTrackingLayer and CompletionIndexLayer out of pre-publish `installBatch`; run them after root seal for distributed and after base-MVCC materialization for local. | ~220 | medium — moving this boundary too early exposes unsealed segments to reads; moving completion too early can make retry dedup lie | **landed** |
| 4 | Local opts out of catalog install/scanner and defaults to base-MVCC materialization. WAL-backed visible logs retain applied records and replay both pending and applied state so restart keeps retry deduplication without catalog records. Distributed catalog/root recovery is unchanged. | ~260 | medium-high — recovery contract changes for local | **landed** |
| 5 | Extract WitnessSignLayer. Wire raftstore. Witness mode enum deleted. | ~150 | low — already gated by Bypass mode | pending |
| 6a | Skip segment payload encoding when the local install chain does not require it. Unknown/distributed installers still encode by default. | ~80 | low | **landed** |
| 6b | Delete `MaterializeSegments` / `CatalogOnlyAuthorityDrain` flags after raftstore and tests no longer rely on them directly. | ~40 | low | pending |
| 7 | TLA spec — verify safety is not order-dependent across legitimate chain compositions. | ~100 spec | medium | pending |

Phases 1, 2, 3, 5, and 6 are intended to be byte-identical for distributed.
Phase 3 changes runtime staging but keeps distributed visibility identical:
durable evidence first, root seal next, read-view finalize last. Phase 4
changes local's behavior (no sealedSegments, no catalog) while keeping
raftstore on the catalog/witness/root-seal path. Phase 7 verifies that the
safety argument is not order-dependent across legitimate compositions.

Total remaining: witness-layer extraction, config deletion, and the TLA/crash
matrix. The local materialization boundary is no longer the blocker.

## What Phase 4 changed

A measurement during foundation work confirms the layering matters more
than the flag. With Phases 1.5 + 1.6 + 1.7 landed, flipping
`MaterializeSegments=true` on local makes the test
`TestLocalRuntimePerasVisibleCommitRecoversInstalledCatalog` pass
correctly — every multi-bucket op materializes into base MVCC, the
catalog record is skipped, and visibleLog replay covers recovery. But
under sustained varmail+ai-checkpoint load:

- `peras_committer.flush_latency_avg_ms` rose to **694ms** (vs.
  ~15–50ms with the catalog-only default).
- `peras_committer.admission_waiting` held at 16 with
  `admission_wait_total=192`, meaning every visible-commit submitter
  routinely blocked waiting for the install pipeline to retire pending
  ops.
- `peras_committer.error_total` accumulated 287 install errors with
  `last_error = "install peras segment: context canceled"` —
  submitter contexts timed out while their ops sat in `holder.pending`.
- bench wall time stretched from ~80s (median scale) to over 12
  minutes with 7.8GB disk usage before manual abort.

The root cause is structural, not a tuning miss: the segment install
pipeline was built to write one small catalog record per segment. Every
segment retires its ops from `holder.pending` only once that single
install completes. With `MaterializeSegments=true` each install now
drives N MVCC commit chunks (one per segment entry) through the
pipeline, and the pipeline serializes per-holder. Pending grows faster
than installs complete and admission backpressure pins the workload.

Tuning knobs (lower `MaterializeMaxReplayMutations`, smaller batches,
parallel chunks) all push the wall to slightly different spots; the real issue
was that local still paid for catalog-mode recovery and segment payload work.
Phase 4 changes the local boundary:

- Local pre-publish install becomes `MVCCEntryLayer` only. Local finalize
  becomes `CompletionIndexLayer` plus the visible-WAL applied marker. There is
  no SealedTrackingLayer (data already lives in base MVCC), no CatalogLayer,
  and no WitnessSignLayer.
- Visible commit writes to overlay as today. The flush path no longer
  writes catalog payload/index records for local; it drains the overlay into
  base MVCC via `MVCCEntryLayer`, records completion state, and bumps the
  applied marker on the visible WAL.
- Applied visible records are retained so restart can rebuild completion
  state without a catalog scanner.
- Segment payload encoding is skipped for local because no local layer needs
  payload bytes or payload digest.

## Bench evidence the foundation already pays

End-to-end bench (median scale, all five workloads, M3 Pro):

```
                       baseline (pre-branch)  branch tip       Δ
ai-checkpoint-agent              7,087/s        11,988/s     +69.2%
filebench-varmail               21,692/s        31,351/s     +44.5%
mimesis-namespace               22,517/s        26,400/s     +17.2%
mdtest-easy                     41,067/s        40,930/s      -0.3%
mdtest-hard                     42,778/s        33,478/s     -21.7%
─────────────────────────────────────────────────────────────────
AGGREGATE                       20,252/s        26,563/s     +31.2%

p99 tails:
  varmail_unlink                 22.3ms →  0.7ms    -97%
  ai_checkpoint_create_artifact  12.6ms →  5.0ms    -61%
  ai_checkpoint_open_session     12.1ms →  4.0ms    -67%
  ai_checkpoint_snapshot_readdir 35.3ms → 14.5ms    -59%
```

Those wins come from the snapshot-flush removal + snapshot scan
optimization + cache wiring + foundation refactor. The install path
restructure and the local materialize default build on that floor; they are
not a replacement for it.

The single sustained regression is `mdtest-hard -21.7%` — removing
the opportunistic snapshot flush also removes one of the few natural
moments when very large segments drain. mdtest-hard hits a shared
directory hot enough that the periodic-only flush schedule can fall
behind; the trade-off was disclosed in the snapshot-flush removal
commit message and is acceptable because the absolute latency stays
well under any human-perceptible bound.

## What this is not

- Not a rewrite of Peras' protocol — `PerasVisibleCommit.tla` semantics
  (visible/durable two-stage) are preserved.
- Not a new runtime package — `fsmeta/runtime/peras` retains its place;
  the change is internal composition.
- Not a wire-format change — `pb/fsmeta` and `SnapshotSubtreeToken` are
  unchanged.
- Not a distributed-side optimization — distributed retains all four
  layers; the catalog-retire / sealed-truncation work for long-running
  distributed clusters is a separate roadmap item.

## Open questions

1. WitnessSignLayer is still represented through the existing witness mode
   instead of a first-class install layer.
2. `MaterializeSegments` and `CatalogOnlyAuthorityDrain` are still config
   switches. They should disappear once composition alone expresses the mode.
3. Local visible WAL now retains applied records as completion evidence.
   A future GC path needs a durable completion-retention boundary before those
   records can be compacted safely.

## Resolved prerequisite: compiler supports multi-bucket materialize

A field experiment in this branch originally confirmed that simply flipping
`MaterializeSegments=true` on the local runtime breaks the existing
`TestLocalRuntimePerasVisibleCommitRecoversInstalledCatalog` test with
`split peras replay plan by install budget: invalid peras segment`. The
root cause is at the compiler layer, not the runtime:

- `compile/rename.peras.go` (and the parallel `link / inode / lookup`
  files) only set `CanMaterialize=true` for two cases: `SegmentInstallSingleBucket`,
  and `SegmentInstallCatalog` with `SingleBucket==true && len(Buckets)==1`.
- Any op whose effects span multiple affinity buckets falls outside both
  cases, leaves `CanMaterialize=false`, and `SegmentPlanForInstall(plan, true)`
  returns `ok=false`, which `splitReplayPlanByCompilerBudget` reports as
  `ErrInvalidPerasSegment`.
- The single-parent Rename in the failing test crosses buckets because
  the renamed inode's bucket is computed independently from its parent's
  bucket; the cross-bucket case is structural, not a workload outlier.

This is conservative correctness behavior at the compiler — it refuses to
emit a materialize plan it isn't sure base MVCC can satisfy atomically.
In reality `runner.MutateAtCommit` handles arbitrary-bucket mutations in a
single percolator group, so multi-bucket materialize is safe.

Phase 1.5 landed the compiler/runtime budget unlock for this class of plans.
The historical shape of the fix was:

Extend the compiler so cross-bucket ops also produce a valid materialize
plan:

```go
// compile/rename.peras.go, around line 340
switch {
case placement.Install == SegmentInstallSingleBucket:
    segment.CanMaterialize = placement.CanSegment
    segment.MaterializeInstall = SegmentInstallSingleBucket
    segment.MaterializeMergeKey = placement.MergeKey
case placement.Install == SegmentInstallCatalog && placement.SingleBucket && len(placement.Buckets) == 1:
    segment.CanMaterialize = placement.CanSegment
    segment.MaterializeInstall = SegmentInstallSingleBucket
    segment.MaterializeMergeKey = SegmentMergeKey{...single-bucket form...}
case placement.Install == SegmentInstallCatalog && placement.CanSegment: // NEW
    // Multi-bucket: materialize is still safe because runner handles
    // arbitrary keys in one percolator commit. Merge key carries the
    // bucket set rather than a single PrimaryBucket.
    segment.CanMaterialize = true
    segment.MaterializeInstall = SegmentInstallMultiBucket // NEW enum value
    segment.MaterializeMergeKey = SegmentMergeKey{
        MountKeyID: placement.MountKeyID,
        // No PrimaryBucket; consumers must read from Buckets list.
        Install:    SegmentInstallMultiBucket,
        Durability: placement.MergeKey.Durability,
    }
}
```

The new `SegmentInstallMultiBucket` enum value semantically means "install
target is `runner.MutateAtCommit` directly, multi-bucket entries are fine".
`installer.MaterializeMVCC` already iterates the segment's entries and
emits one mutation per entry regardless of bucket; the only thing the
existing code does not do is *plan* for that case.

### Remaining dependency on this design

- Phase 3 has moved read/dedup finalize after publish or local materialization.
- Phase 4 has removed local's catalog dependency and added WAL-state recovery
  for applied visible records.
- The next implementation PR should extract WitnessSignLayer and remove the
  now-derivable mode flags.
