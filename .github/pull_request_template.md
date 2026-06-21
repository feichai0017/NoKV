<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

## Related Issue

<!--
Every non-trivial PR should be tied to an existing issue so maintainers can
review the agreed problem, scope, and design history before reading the diff.

Use one of:
- Closes #issue_number
- Fixes #issue_number
- Relates to #issue_number

Small exceptions are allowed for typo-only docs edits, CI/dependency chores,
release chores, and maintainer-approved emergency fixes. If this PR has no
issue, explain the exception here.
-->

Closes | Fixes | Relates to #

## Summary

-

## Scope

- [ ] This PR changes one logical boundary only.
- [ ] No unrelated refactor, benchmark, metadata model, Holt layout,
      object-store, or docs change is mixed in.
- [ ] The linked issue describes the user-visible problem, design decision, or
      maintenance task this PR resolves.
- [ ] Any breaking change is intentional and documented.
- [ ] No compatibility shim, deprecated alias, or forwarding wrapper was added without a removal condition.

## Code Contract

- [ ] Package boundaries follow `docs/development/code_contract.md`.
- [ ] Shared helpers reuse the standard library or existing repository helpers.
      New generic helper modules are domain-neutral and tested.
- [ ] File names and file placement follow the code contract.
- [ ] New types, interfaces, structs, fields, and functions use domain-specific names.
- [ ] New errors are in the owning package's `errors.rs` and carry stable error kinds when crossing package boundaries.
- [ ] New metrics/stats are owned by the package that reports or serves them.
- [ ] Metadata changes state durability, object-reference lifetime,
      watch/snapshot retention, GC, and fallback boundaries.

## Validation

- 

## Contributor Sign-off

- [ ] Every commit in this PR includes a DCO `Signed-off-by` trailer.
