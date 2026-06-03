<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

## Summary

- 

## Scope

- [ ] This PR changes one logical boundary only.
- [ ] No unrelated refactor, benchmark, metadata model, Holt layout,
      object-store, or docs change is mixed in.
- [ ] Any breaking change is intentional and documented.
- [ ] No compatibility shim, deprecated alias, or forwarding wrapper was added without a removal condition.

## Code Contract

- [ ] Package boundaries follow `docs/guide/development/code_contract.md`.
- [ ] Shared helpers reuse the standard library or existing repository helpers.
      New generic helper modules are domain-neutral and tested.
- [ ] File names and file placement follow the code contract.
- [ ] New types, interfaces, structs, fields, and functions use domain-specific names.
- [ ] New errors are in the owning package's `errors.go` and carry stable error kinds when crossing package boundaries.
- [ ] New metrics/stats are owned by `metrics.go`, `stats.go`, `/metrics`, or a `*/stats` package.
- [ ] Metadata changes state durability, object-reference lifetime,
      watch/snapshot retention, GC, and fallback boundaries.

## Validation

- 

## Contributor Sign-off

- [ ] Every commit in this PR includes a DCO `Signed-off-by` trailer.
