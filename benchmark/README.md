<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarking

The benchmark tree is kept focused on `fsmeta` workloads. Generic
storage-engine comparisons were removed from the main repo because NoKV is no
longer positioned as a standalone storage engine benchmark target.

## fsmeta Workloads

Run the local fsmeta workload matrix:

```sh
NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench
```

Run the Docker Compose distributed matrix:

```sh
NOKV_FSMETA_BENCH_MODE=compose make fsmeta-bench
```

Run the Rust distributed smoke or workload matrix:

```sh
NOKV_FSMETA_BENCH_MODE=rust make fsmeta-bench
```

Rust mode starts three `nokv-raftstore-server` peers, the Go root and
coordinator control plane, and `nokv-fsmeta --runtime=raftstore` before running
the benchmark client.

Useful environment variables:

| Variable | Purpose |
|---|---|
| `NOKV_FSMETA_PROFILE` | Workload profile: `median`, `long`, or `official`. |
| `NOKV_FSMETA_WORKLOADS` | Comma-separated workload filter. |
| `NOKV_FSMETA_OUTPUT_DIR` | Directory for CSV outputs and manifests. |
| `NOKV_FSMETA_RESET_BETWEEN_WORKLOADS` | Reset the runtime between workloads when set to `1`. |
| `NOKV_FSMETA_RUST_RAFTSTORE_ADDRS` | Three comma-separated Rust raftstore gRPC endpoints for Rust mode. |
| `NOKV_FSMETA_RUST_RAFTSTORE_METRICS_ADDRS` | Three comma-separated Rust raftstore metrics endpoints for Rust mode. |

The benchmark package lives under `benchmark/fsmeta` and the reusable workload
definitions live under `benchmark/fsmeta/workload`.

## Plotting

`benchmark/cmd/plotbench` renders observation CSV files produced by benchmark
or analysis runs:

```sh
go run ./cmd/plotbench \
  -format observations \
  -input data/fsmeta/results/example.csv \
  -category-col workload \
  -series-col operation \
  -value-col ops_per_sec \
  -output figures/fsmeta.svg
```
