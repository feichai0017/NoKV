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

The Docker Compose benchmark path is also local fsmeta today. The Rust
distributed data-plane smoke path starts a three-peer `meta-root`, one
`coordinator`, one Rust `raftstore`, registers the benchmark mount, and then
starts `nokv-fsmeta --runtime=raftstore`. Rust benchmark runs build the
raftstore server in release mode by default; set
`NOKV_FSMETA_RUST_CARGO_PROFILE=debug` only for local debugging:

```bash
NOKV_FSMETA_BENCH_MODE=rust \
NOKV_FSMETA_WORKLOADS=mdtest-easy \
NOKV_FSMETA_CLIENTS=1 \
NOKV_FSMETA_DIRS=1 \
NOKV_FSMETA_FILES_PER_DIR=2 \
make fsmeta-bench
```

Use `make fsmeta-rust-smoke` for the faster Rust MetadataPlane package gate.
It runs the tagged fsmeta runtime suite, including the single-node and
three-peer contract checks plus coordinator rebuild, restart, leader handoff,
follower catch-up, removed-peer, watch replay, and retention paths. Use
`NOKV_FSMETA_BENCH_MODE=rust make fsmeta-bench` when the change needs real
process startup, mount registration, coordinator routing, or benchmark-client
evidence.

Do not claim a performance improvement without a before/after workload result
and the command used to produce it.
