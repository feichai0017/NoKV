# Contributing to NoKV

Thanks for contributing. This file is the authoritative contribution guide for this repository.

## Scope

- Repository: `github.com/feichai0017/NoKV`
- Main branch: `main`
- Go version: `1.26.x` (see `go.mod`)

## Development Setup

1. Fork on GitHub and clone your fork.
2. Add the upstream remote to keep your fork up to date.
3. Install toolchain and dependencies.

```bash
git clone https://github.com/feichai0017/NoKV.git
cd NoKV
git remote rename origin upstream
go mod download
make install-tools
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

## Local Validation

Run these before opening a PR:

```bash
make fmt
make lint
make test
```

Recommended when changing concurrency-sensitive code:

```bash
make test-race
```

For benchmark-related changes:

```bash
make bench
```

## Pull Request Rules

- Rebase on latest `upstream/main` before opening or updating a PR.
- PR description must include: what changed, why it changed, and how you validated it (commands + key results).
- Link related issue(s).
- Include docs updates when behavior/config/CLI changes.
- Keep PRs small enough for focused review.

## Code Guidelines

- Use `gofmt` formatting and pass `golangci-lint`.
- Add/maintain GoDoc comments for exported symbols.
- Keep package boundaries clear; avoid cross-package coupling without need.
- Do not mix unrelated refactors with behavior changes in one PR.
- Add tests for every bug fix or behavior change.

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
