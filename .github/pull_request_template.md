## Summary

- 

## Scope

- [ ] This PR changes one logical boundary only.
- [ ] No unrelated refactor, benchmark, generated-code, fsmeta/Peras, root, coordinator, raftstore, or docs change is mixed in.
- [ ] Any breaking change is intentional and documented.
- [ ] No compatibility shim, deprecated alias, or forwarding wrapper was added without a removal condition.

## Code Contract

- [ ] Package boundaries follow `docs/guide/development/code_contract.md`.
- [ ] Shared helpers reuse the standard library or existing repository helpers; new `utils/` code is domain-neutral and tested.
- [ ] File names and file placement follow the code contract.
- [ ] New types, interfaces, structs, fields, and functions use domain-specific names.
- [ ] New errors are in the owning package's `errors.go` and carry stable error kinds when crossing package boundaries.
- [ ] New metrics/stats are owned by `metrics.go`, `stats.go`, `/metrics`, or a `*/stats` package.
- [ ] Generated code was regenerated rather than manually edited.
- [ ] Distributed changes state the authority, freshness, visibility, durability, recovery, GC/seal, and fallback boundaries.

## Validation

- 

## Contributor Sign-off

- [ ] Every commit in this PR includes a DCO `Signed-off-by` trailer.
