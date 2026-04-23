# TLA Setup

This directory holds the first model-checking skeleton for the control-plane paper.

## Install tools

```bash
make install-tla-tools
```

This downloads pinned versions of:

- `tla2tools.jar` for TLC
- `Apalache`

Both are installed locally under `third_party/tla/`.

## Run TLC

```bash
make tlc-succession
make tlc-successionmultidim
make tlc-leaseonly-counterexample
make tlc-leasestart-counterexample
make tlc-tokenonly-counterexample
make tlc-chubbyfenced-counterexample
make record-formal-artifacts
```

`Succession.tla` is the positive model and should satisfy its configured invariants.

Current contrast models:

- `LeaseOnly.tla`: no reply-side guard and no rooted closure object; expected to violate `NoOldReplyAfterSuccessor`
- `SuccessionMultiDim.tla`: positive lease-start coverage model for the CRDB `#66562` analog
- `LeaseStartOnly.tla`: no lease-start coverage check on predecessor served-read summaries; expected to violate `NoWriteBehindServedRead`
- `TokenOnly.tla`: bounded-freshness token only; still expected to violate `NoOldReplyAfterSuccessor`
- `ChubbyFencedLease.tla`: per-reply sequencer fencing; expected to preserve stale-reply rejection but violate successor coverage

The current models now distinguish:

- `inflight` replies still in the network / service boundary
- `delivered` reply currently being admitted by the caller

This means `Seal` no longer retroactively clears outstanding replies. Instead,
the positive model only allows delivery of replies whose generation remains
legal under the rooted closure state. The contrast model keeps the same
in-flight structure but removes closure-aware admission.

`Succession.tla` now models a repeated rooted handoff cycle:

- `Issue -> Active -> Seal -> Issue(successor) -> Cover -> Close -> Reattach -> Active`

The model is still checked with finite constants, but it is no longer limited
to a single closure cycle.

For `Primacy`, the spec now includes a stronger induction-friendly invariant:

- `G2_PrimacyInductive`

This invariant states that every issued generation other than the current
`activeGen` has already been sealed. TLC and Apalache both check this stronger
shape directly, and the spec includes a lemma showing it implies the original
`Primacy` claim. This is still not a full TLAPS proof, but it is a
more robust bridge from bounded checking to an unbounded-by-construction
argument for Primacy than the earlier cardinality-only invariant.

## Run Apalache

```bash
make apalache-typecheck
make apalache-check-succession
make apalache-check-successionmultidim
```

`apalache-typecheck` checks that the current specs are well-typed.

`apalache-check-succession` runs a bounded check of `Succession.tla` against:

- `G1_Succession`
- `G2_Primacy`
- `G2_PrimacyInductive`
- `G3_Silence`

`apalache-check-successionmultidim` runs a bounded check of `SuccessionMultiDim.tla`
against:

- `NoWriteBehindServedRead`

`record-formal-artifacts` stores sanitized TLC / Apalache outputs under
`spec/artifacts/` so the current result shape is checked into the repo.
