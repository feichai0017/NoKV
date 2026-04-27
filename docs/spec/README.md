# TLA Setup

This directory holds the first model-checking skeleton for the control-plane paper.

## Install tools

```bash
make install-tla-tools
```

This downloads a pinned version of:

- `tla2tools.jar` for TLC

It is installed locally under `third_party/tla/`.

## Run TLC

```bash
make tlc-eunomia
make tlc-eunomiamultidim
make tlc-mountlifecycle
make tlc-subtreeauthority
make tlc-leaseonly-counterexample
make tlc-leasestart-counterexample
make tlc-tokenonly-counterexample
make tlc-chubbyfenced-counterexample
make tlc-subtreewithoutfrontiercoverage-counterexample
make tlc-subtreewithoutseal-counterexample
make record-formal-artifacts
```

The Eunomia positive model lives at `Eunomia.tla` and should satisfy its
configured invariants.

Current contrast models:

- `LeaseOnly.tla`: no reply-side guard and no rooted handover record; expected to violate `NoOldReplyAfterSuccessor`
- `EunomiaMultiDim.tla`: positive lease-start coverage model for the CRDB `#66562` analog
- `MountLifecycle.tla`: positive rooted mount lifecycle model; checks that registered mounts cannot be implicitly created or reactivated after retirement
- `SubtreeAuthority.tla`: positive namespace authority handoff model; projects Eunomia's Primacy / Inheritance / Silence / Finality onto subtree authority records
- `LeaseStartOnly.tla`: no lease-start coverage check on predecessor served-read summaries; expected to violate `NoWriteBehindServedRead`
- `TokenOnly.tla`: bounded-freshness token only; still expected to violate `NoOldReplyAfterSuccessor`
- `ChubbyFencedLease.tla`: per-reply sequencer fencing; expected to preserve stale-reply rejection but violate successor coverage
- `SubtreeWithoutFrontierCoverage.tla`: subtree handoff without successor frontier coverage; expected to violate `Inheritance`
- `SubtreeWithoutSeal.tla`: subtree handoff that leaves the predecessor active; expected to violate `Primacy`

The current models now distinguish:

- `inflight` replies still in the network / service boundary
- `delivered` reply currently being admitted by the caller

This means `Seal` no longer retroactively clears outstanding replies. Instead,
the positive model only allows delivery of replies whose era remains
legal under the rooted handover state. The contrast model keeps the same
in-flight structure but removes finality-aware admission.

The Eunomia model in `Eunomia.tla` now models a repeated rooted handoff cycle:

- `Issue -> Active -> Seal -> Issue(successor) -> Cover -> Close -> Reattach -> Active`

The model is still checked with finite constants, but it is no longer limited
to a single finality cycle.

For `Primacy`, the spec now includes a stronger induction-friendly invariant:

- `G2_PrimacyInductive`

This invariant states that every issued era other than the current
`activeEra` has already been sealed. TLC checks this stronger shape directly,
and the spec includes a lemma showing it implies the original `Primacy` claim.
This is still not a full TLAPS proof, but it is a more robust bridge from
bounded checking to an unbounded-by-construction argument for Primacy than the
earlier cardinality-only invariant.

Stage 3 adds two namespace-facing positive models:

- `MountLifecycle.tla` models `MountRegistered` / `MountRetired` as rooted
  lifecycle truth. It checks three mount invariants: retired mounts never
  become active again, active/retired mounts must have been explicitly
  registered, and mount identity fields stay present after registration.
- `SubtreeAuthority.tla` models the authority protocol that `RenameSubtree v1`
  will consume. It excludes dentry mutation and checks only authority
  correctness: one active authority per subtree, successor frontier coverage,
  inadmissibility of sealed-authority replies, and finality of sealed
  predecessors. It also carries `PrimacyInductive`, a pairwise active-record
  uniqueness shape that is stronger than the cardinality-only `Primacy`
  invariant and better suited for a later proof.

`record-formal-artifacts` stores sanitized TLC outputs under `artifacts/`
so the current result shape is checked into the repo. These are bounded model
checks, not a TLAPS / Coq / Dafny proof.
