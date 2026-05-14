# Agent Review Instructions

This repository uses `docs/development/code_contract.md` as the source of truth
for code structure, package boundaries, naming, errors, metrics, tests, DCO,
and distributed-safety review.

Before reviewing or editing a PR:

1. Read `docs/development/code_contract.md`.
2. Use `docs/development/pr_review_checklist.md`.
3. Inspect the real changed files before relying on README or design docs.
4. Report findings first, ordered by severity.

Check for:

- Scope drift across root, coordinator, raftstore, fsmeta, Peras, docs, and
  benchmark files.
- Missing DCO `Signed-off-by` trailers.
- Package-boundary violations.
- New helpers that reimplement standard library or existing repository helpers.
- Misuse of `utils/` for domain-specific or single-use code.
- Misplaced errors, metrics, stats, validation, recovery, or encoding code.
- Vague file names, type names, interface names, or function names.
- Redundant forwarding wrappers or compatibility shims.
- Authority, freshness, durability, recovery, or GC ambiguity.
- Generated-code drift.
- Missing regression, integration, recovery, or benchmark evidence.

Do not suggest compatibility shims by default. NoKV accepts breaking changes
when they remove ambiguity or reduce long-term maintenance cost. If a
compatibility path is necessary, require a removal condition.
