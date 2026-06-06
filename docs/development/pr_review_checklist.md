<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# PR Review Checklist

Findings come first. Do not approve a change because tests pass if it weakens
metadata atomicity, object publish safety, watch/snapshot retention, or package
boundaries.

## Scope

- Does the PR change one logical boundary?
- Are unrelated metadata schema, object, client, FUSE, docs, or example changes
  mixed together?
- Is every behavior change described?
- Does every non-merge commit include `Signed-off-by`?

## Boundaries

- Does the package import direction match the code contract?
- Did a lower layer import a higher layer for convenience?
- Does `nokv-types` remain storage-neutral?
- Does `nokv-meta` keep schema, command execution, Holt binding, and service
  semantics inside the metadata boundary?
- Does `nokv-object` avoid namespace metadata?
- Does `nokv-client` resolve paths through `nokv-meta` instead of importing
  layout or storage internals?
- Does `nokv-fuse` stay inode-first and call `nokv-meta` rather than the
  path SDK?
- Does `nokv` keep the `nokv` binary thin over `client`/`fuse` instead
  of duplicating metadata semantics?

## Correctness

- Are predicates checked before mutations and applied atomically?
- Can a failed object publish or metadata publish leave user-visible partial
  state?
- Are duplicate request ids deterministic?
- Does remove/replace return old body descriptors when GC needs them?
- Are snapshot/watch retention and history GC rules explicit?
- Does a read path observe a complete dentry projection or fall back safely?

## Performance

- Does a hot metadata operation avoid unnecessary history writes?
- Does `ReadDirPlus` hit dentry projection without inode fanout on the common
  path?
- Does prefix-empty use Holt prefix iteration with early exit?
- Does the change report benchmark evidence when it claims speedup?

## Tests

- Is there a package test for each local invariant?
- Is there a contract test for metadata commands or object-store behavior?
- Are S3/RustFS integration tests env-gated rather than hard-required?
- Are error paths and predicate failures covered?

## Required Validation

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```
