# Yanex Agent Interface Benchmark

This directory contains the experimental Yanex agent-interface benchmark
harness. It compares how different agent-facing surfaces affect correctness,
evidence quality, token cost, tool calls, and bytes read over the same fixed
Yanex experiment corpus.

This is not a storage-engine throughput benchmark. The product benchmark for
metadata, training-read, and checkpoint workloads lives in the root `bench/`
crate and is documented in `docs/benchmarks.md`.

## Current Status

The harness is a local experimental benchmark tree. It is not part of
`origin/main` at the time this README was written.

The current NoKV product crates now expose the intended low-level native
namespace surface:

- `nokvfs-meta`: `stat_card`, `list_page`, `find_paths`, and `read_page`;
- `nokvfs-protocol`: `StatCard`, `ListPage`, `FindPaths`, and `ReadPage` RPC
  DTOs;
- `nokvfs-client`: SDK methods plus the product-native agent adapter exposing
  `ls`, `stat`, `read`, and `find`;
- `nokvfs-server`: framed metadata RPC handlers for the same operations.

The benchmark arm named `nokv_native_v1` now uses the `nokvfs-client` agent
adapter. The harness translates OpenAI tool calls into product API calls, but
does not own the measured card, find, structured read, index catalog,
pagination, consistency, or evidence semantics.

## Corpus

The benchmark uses a fixed Yanex experiment tracking corpus. The materialized
state includes:

- experiment metadata;
- params;
- metrics;
- dependencies;
- artifacts;
- git state;
- generated read-only index files.

Default local data root:

```text
benchmark/data/yanex-demo
```

Expected local layout after preparation:

```text
benchmark/data/yanex-demo/
  corpus/
  sqlite/yanex.db
  nokv/meta
  rustfs/
  manifest.json
  results/
```

The local corpus archive path is intentionally not committed. Pass it with
`--archive` when preparing data.

## Arms

The Phase 1 harness compares three read-only arms:

| Arm | Surface |
| --- | --- |
| `sqlite_raw_v1` | Raw SQLite schema/query/blob tools plus ETL-maintained agent-index materialization tables. |
| `sqlite_agentfs_v1` | Filesystem-shaped projection backed by SQLite. |
| `nokv_native_v1` | NoKV product-native agent adapter. |

The fixed Phase 1 tasks live in `tasks/phase1_readonly.yaml`. The rubric lives
in `rubric/phase1_readonly.yaml`.

## Valid Comparisons

The benchmark has one core A/B comparison:

- Raw SQLite tools vs NoKV Native Namespace.

SQLite AgentFS remains useful as sensitivity/context because it isolates the
effect of a filesystem-shaped projection backed by the same SQLite corpus. It
should not be presented as the main product claim because it is a harness
projection rather than the NoKV product-native surface.

## Materialized Index Fairness

Precomputed indexes are a normal experiment-tracking system behavior and are
part of the intended benchmark model. They make the benchmark closer to a real
agent workload, where agents inspect catalogs and facets instead of scanning
all raw blobs for every question.

The fairness rule is that every valid A/B comparison must expose logically
equal introspection over the benchmarked facts. The syntax and access pattern
can differ by surface, but the visible catalog fields, index facts, evidence
handles, and limitations must not give one arm hidden task answers that the
paired arm cannot discover through its own public interface.

## NoKV Native Definition

In this benchmark, "NoKV native" must mean that the agent-facing tools map to
NoKV product APIs. The harness may adapt OpenAI tool calls into SDK calls and
enforce limits, but it must not own the measured metadata semantics.

Target native behavior:

- `ls`/`stat` return typed directory/file cards, not flat file entries.
- `entry_count`, `record_count`, `schema`, `sample`, and body descriptors are
  first-class fields.
- `read` defaults to structured pagination; raw bytes require explicit
  `format = "bytes"`.
- `find(path, filter, sort, limit, cursor, include)` is the core exploration
  primitive.
- `find` uses a constrained predicate grammar declared by stat/catalog cards.
- every result includes evidence, snapshot/generation identity, truncation
  state, and `next_cursor` when more results exist.
- `record_count` includes provenance: live namespace, structured body,
  materialized index, or approximate.

Generated `/index/*.json` files must not become hidden answer files. They can
support facet-count tasks, but they are not a substitute for product-level
typed index namespaces or future derived metric/set-pipeline APIs.

## Prepare Data

Start RustFS:

```bash
./benchmark/agent-interface-benchmark/scripts/start_rustfs.sh
```

Prepare the fixed corpus:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- prepare \
  --archive /path/to/yanex-experiment-metadata-origami-data-gen-project.tar.gz \
  --data-root benchmark/data/yanex-demo \
  --reset
```

Verify materialization:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- verify \
  --data-root benchmark/data/yanex-demo
```

## Run Phase 1

Set OpenAI credentials and model:

```bash
export OPENAI_API_KEY=...
export OPENAI_MODEL=gpt-5.5
```

Run one task for one arm:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --arm nokv_native_v1 \
  --task-id largest_stderr_files \
  --repeats 1 \
  --output-jsonl benchmark/data/yanex-demo/results/phase1.jsonl
```

Run all fixed Phase 1 tasks for all arms:

```bash
./benchmark/agent-interface-benchmark/scripts/run_phase1_batch.sh \
  --repeats 10 \
  --output-jsonl benchmark/data/yanex-demo/results/phase1.jsonl
```

## Direct Tool Checks

Inspect the tool registry:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- tools \
  --arm nokv_native_v1
```

Inspect a NoKV raw namespace path:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- nokv-stat \
  --data-root benchmark/data/yanex-demo \
  --path /yanex/runs/00023013/metadata.json
```

The `nokv-*` direct commands above remain raw debugging commands. The benchmark
arm uses the product-native `ls`/`stat`/`read`/`find` adapter exposed by
`nokvfs-client`.

Inspect SQLite schema:

```bash
cargo run --manifest-path benchmark/agent-interface-benchmark/harness/Cargo.toml -- sqlite-show-schema \
  --db benchmark/data/yanex-demo/sqlite/yanex.db
```

## Next Work

The next benchmark-specific PR should run the full `sqlite_raw_v1` vs
`nokv_native_v1` Phase 1 batch and use the metric output to decide the next
NoKV API increment. V1 must produce correctness, evidence, token, tool, bytes,
and wall-time metrics; outperforming SQL is not the first PR gate.

Useful follow-up product increments include typed facets for metric
latest/min/max by run, params, dependencies, and richer git patch availability.
Do not implement one-sided semantics in the harness as benchmark-only
shortcuts.
The harness should remain a thin adapter over `nokvfs-client` or `nokvfs-meta`.
