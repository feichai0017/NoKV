# Formal Artifact Snapshots

This directory stores sanitized, checked-in outputs from the current TLA+ runs.

- `tlc-eunomia.out`: positive `Eunomia.tla` run
- `tlc-eunomiamultidim.out`: positive `EunomiaMultiDim.tla` run
- `tlc-mountlifecycle.out`: positive `MountLifecycle.tla` run
- `tlc-subtreeauthority.out`: positive `SubtreeAuthority.tla` run
- `tlc-leaseonly.out`: counterexample for `LeaseOnly.tla`
- `tlc-leasestart.out`: counterexample for `LeaseStartOnly.tla`
- `tlc-tokenonly.out`: counterexample for `TokenOnly.tla`
- `tlc-chubbyfenced.out`: counterexample for `ChubbyFencedLease.tla`
- `tlc-subtreewithoutfrontiercoverage.out`: counterexample for `SubtreeWithoutFrontierCoverage.tla`
- `tlc-subtreewithoutseal.out`: counterexample for `SubtreeWithoutSeal.tla`

The outputs are intentionally filtered so they are stable enough to diff in the
repo while still showing the key result shape: no-error on the positive model,
counterexample on contrast models, and the main state-count summary. They are
TLC bounded model-checking artifacts, not machine-checked proofs.
