<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Testing

Use the smallest test scope that proves the boundary you changed.

## Default Gates

```bash
go test -count=1 ./...
cargo test --manifest-path raftstore/Cargo.toml --workspace
git diff --check
```

Run `make lint` for package-boundary changes, generated code, or PR-ready
validation.

## Package Focus

| Area | Tests |
|---|---|
| fsmeta semantics | `go test ./fsmeta/model ./fsmeta/layout ./fsmeta/exec ./fsmeta/contract` |
| local Pebble runtime | `go test ./fsmeta/runtime/local ./cmd/nokv-fsmeta` |
| root truth | `go test ./meta/root/...` |
| coordinator | `go test ./coordinator/...` |
| Rust data plane | `cargo test --manifest-path raftstore/Cargo.toml --workspace` |
| fsmeta over Rust data plane | `make fsmeta-rust-smoke` |

## Benchmarks

The stable benchmark target is local fsmeta:

```bash
NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench
```

The Docker Compose benchmark path is also local fsmeta today. Use
`make fsmeta-rust-smoke` for the current Rust distributed data-plane gate until
the distributed benchmark launcher starts `meta-root`, `coordinator`,
`raftstore`, and `nokv-fsmeta --runtime=raftstore` as one harness.

Do not claim a performance improvement without a before/after workload result
and the command used to produce it.
