<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# PR Review Checklist

Use this checklist for human review and for agent-assisted review. Findings
come first. Do not approve a PR because it passes tests if it weakens an
authority, durability, recovery, or package boundary.

## Scope

- Does the PR change one logical boundary?
- Are unrelated fsmeta, root, raftstore, coordinator, docs, benchmark, or
  generated-code changes mixed together?
- Is every behavior change described in the PR summary?
- Is every commit DCO signed with `Signed-off-by`?

## Package Boundary

- Does the package import direction match `docs/development/code_contract.md`?
- Did a lower layer import a higher layer for convenience?
- Is a new interface defined near the consumer?
- Is the new package split by ownership rather than file count?
- Does every new package clearly state what it owns and what it must not import?

## Shared Helpers

- Did the change reuse the standard library or an existing repository helper
  instead of reimplementing one?
- If code was added to `utils/`, is it domain-neutral, used by multiple
  packages, and tested?
- Is domain-specific helper logic kept in its owning package instead of being
  dumped into `utils/`?

## File and Naming Discipline

- Are file names lowercase snake_case and responsibility-based?
- Are `errors.go`, `metrics.go`, `stats.go`, `validation.go`, `recovery.go`,
  and `encode.go` used for their intended responsibilities?
- Are vague files such as `utils.go`, `helpers.go`, `common.go`, and `misc.go`
  avoided?
- Do exported types and interfaces name their domain responsibility?
- Are functions named by state transition, not implementation detail?
- Are functions placed near the flow they serve?
- Are one-line forwarding wrappers avoided or explicitly justified?

## Errors

- Are new sentinel errors in `errors.go`?
- Do cross-package errors carry a stable NoKV error kind?
- Does caller logic use `errors.Is`, `errors.As`, or `nokverrors.KindOf`?
- Are gRPC status errors created only at RPC boundaries?
- Is string-matching on errors avoided?

## Metrics and Stats

- Are runtime counters owned by `metrics.go`?
- Are diagnostic snapshots typed and owned by `stats.go` or a `*/stats` package?
- Does business logic update metrics through `recordX` helpers?
- Is `map[string]any` limited to export/diagnostics boundaries?
- Are metric names stable and documented when user-visible?

## Generated Code

- Is there one semantic source of truth?
- Are generated files deterministic?
- Does `go generate` leave the tree clean?
- Are generated files not manually edited?
- If old and generated paths coexist, is the removal condition documented?

## Distributed Safety

For distributed changes, identify:

- Authority owner.
- Freshness/fence check.
- Visibility boundary.
- Durability boundary.
- Recovery source.
- GC/seal condition.
- Fallback path.

Reject or request changes when:

- A new write path bypasses authority checks.
- A recovery path depends on best-effort repair for safety.
- A GC path can drop unsealed or unrooted evidence.
- A witness, seal, or catalog record is not self-describing enough for replay.
- A read path can observe partially installed state.

## Tests

- Does every bug fix include a regression test naming the failure mode?
- Does cross-module behavior have an integration or contract test?
- Do distributed changes cover at least one stale leader, stale epoch, retry,
  cancellation, crash, duplicate request, or recovery replay path?
- Are test helpers in `test_helpers_test.go` and not mixed into production code?
- Does the PR include benchmark evidence for performance claims?

## Breaking Changes

- Is the breaking change simpler than a compatibility shim?
- Are docs, config examples, CLI help, and tests updated?
- Is any compatibility exception explicitly time-bounded?
- Are deprecated aliases avoided unless required by an already released surface?

## Required Validation

The PR should report exact commands and results. At minimum:

```bash
make fmt
make lint
make test
```

Add targeted tests for the changed subsystem. Examples:

```bash
make test-architecture
go test ./meta/root/... ./meta/root/server -count=1
go test ./fsmeta/exec/compile ./fsmeta/exec/peras ./fsmeta/runtime/peras -count=1
go generate ./fsmeta/exec/compile
git diff --exit-code -- fsmeta/exec/compile
```
