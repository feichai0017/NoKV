<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Contributing to NoKV

Thanks for contributing. This file is the authoritative contribution guide for this repository.

## Scope

- Repository: `github.com/feichai0017/NoKV`
- Main branch: `main`
- Main product line: Rust NoKV under `crates/`
- Rust toolchain: stable

## Development Setup

1. Fork on GitHub and clone your fork.
2. Add the upstream remote to keep your fork up to date.
3. Install Rust stable and Node.js for documentation builds.

```bash
git clone https://github.com/feichai0017/NoKV.git
cd NoKV
git remote rename origin upstream
cargo fetch
npm --prefix docs ci
```

If you use a fork-based workflow, add your fork as `origin`.

## Branch and Commit Conventions

Use these branch prefixes:

- `feature/...` for new features
- `fix/...` for bug fixes
- `refactor/...` for non-functional refactors
- `docs/...` for documentation updates
- Commit format: `<type>: <subject>`
- Common types: `feat`, `fix`, `refactor`, `docs`, `test`, `chore`
- Keep each commit focused on one logical change.
- Sign every commit with the Developer Certificate of Origin trailer:

```bash
git commit -s -m "feat: add feature"
```

If a local commit is missing the trailer, amend or rebase before opening the PR:

```bash
git commit --amend -s --no-edit
git rebase --signoff origin/main
```

## Local Validation

Run these before opening a PR:

```bash
make fmt
make lint
make test
make docs-build
```

For benchmark-related changes:

```bash
cargo test --workspace --release
```

## Pull Request Rules

- Rebase on latest `upstream/main` before opening or updating a PR.
- PR description must include: what changed, why it changed, and how you validated it (commands + key results).
- Link related issue(s).
- Include docs updates when behavior/config/CLI changes.
- Keep PRs small enough for focused review.
- Keep each PR scoped to one logical boundary. Do not mix metadata model,
  Holt layout, object-store, docs, benchmark, or unrelated refactors.
- Every non-merge commit must include a `Signed-off-by` trailer matching the Developer Certificate of Origin in [`DCO`](./DCO).
- If you use Codex or another agent to review a PR, point it at [`docs/development/code_contract.md`](./docs/development/code_contract.md) and [`docs/development/pr_review_checklist.md`](./docs/development/pr_review_checklist.md).

## Code Guidelines

- Use `rustfmt` formatting and pass `clippy` with warnings denied.
- Add or maintain Rustdoc comments for public APIs when the semantics are not
  obvious from the type name.
- Keep package boundaries clear; avoid cross-package coupling without need.
- Do not mix unrelated refactors with behavior changes in one PR.
- Add tests for every bug fix or behavior change.
- Follow the repository code contract in [`docs/development/code_contract.md`](./docs/development/code_contract.md), including package responsibilities, shared-helper reuse, file naming, type/interface/function naming, error placement, metrics/stats ownership, generated-code discipline, and compatibility rules.
- Prefer breaking changes that remove ambiguity over compatibility wrappers. Add a compatibility shim only when a released RPC, CLI, config, or persisted format requires it, and document the removal condition.

## Testing Expectations

- Unit test for local logic changes.
- Integration test for cross-module behavior changes.
- Bench evidence for performance-sensitive modifications.
- If a test cannot be added, explain why in the PR.

## Issues and Proposals

- Use GitHub Issues for bugs/features.
- Use the repository issue template when opening a new issue.
- For broad design topics, use GitHub Discussions first, then split into implementable issues.

## Documentation Policy

When you change behavior, update related docs in the same PR:

- `README.md`
- `docs/`
- config examples and scripts if flags/config fields changed

## License

By contributing, you agree your contribution is licensed under Apache License 2.0, consistent with this repository.
