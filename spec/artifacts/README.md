# Formal Artifact Snapshots

This directory stores sanitized, checked-in outputs from the current TLA+ runs.

- `tlc-succession.out`: positive `Succession.tla` run
- `tlc-successionmultidim.out`: positive `SuccessionMultiDim.tla` run
- `tlc-mountlifecycle.out`: positive `MountLifecycle.tla` run
- `tlc-subtreeauthority.out`: positive `SubtreeAuthority.tla` run
- `tlc-leaseonly.out`: counterexample for `LeaseOnly.tla`
- `tlc-leasestart.out`: counterexample for `LeaseStartOnly.tla`
- `tlc-tokenonly.out`: counterexample for `TokenOnly.tla`
- `tlc-chubbyfenced.out`: counterexample for `ChubbyFencedLease.tla`
- `tlc-subtreewithoutfrontiercoverage.out`: counterexample for `SubtreeWithoutFrontierCoverage.tla`
- `tlc-subtreewithoutseal.out`: counterexample for `SubtreeWithoutSeal.tla`
- `apalache-succession.out`: bounded Apalache check for `Succession.tla`
- `apalache-successionmultidim.out`: bounded Apalache check for `SuccessionMultiDim.tla`

The outputs are intentionally filtered so they are stable enough to diff in the
repo while still showing the key result shape: no-error on the positive model,
counterexample on contrast models, and the main state-count summary.
