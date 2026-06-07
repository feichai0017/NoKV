---
title: Benchmarks
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

NoKV keeps product microbenchmarks inside the product crates and puts
system-level workload runs in the root-level `bench/` crate. The benchmark harness is
for metadata smoke, MLPerf Storage/DLIO-style generated training reads, and
checkpoint publish/read paths:

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile smoke \
  --workload all
```

The default object backend is a local RustFS endpoint at
`http://127.0.0.1:9000`, bucket `nokv`, with the standard local RustFS
development credentials. Start RustFS first when running object-backed
workloads.

For a disposable local RustFS-backed end-to-end run, use the repository script:

```bash
scripts/run-rustfs-e2e.sh
```

The script starts RustFS with the AI training buffer profile, creates the
default bucket, runs `nokv-bench`, and removes its temporary RustFS data
directory. Override the workload with environment variables, for example:

```bash
NOKV_E2E_PROFILE=standard \
NOKV_E2E_WORKLOAD=checkpoint-publish \
NOKV_E2E_OBJECT_CONCURRENCY=8 \
scripts/run-rustfs-e2e.sh
```

For the current AI-training product smoke gate, use:

```bash
scripts/run-ai-training-smoke.sh
```

The default gate runs `metadata-concurrent-read`, `checkpoint-publish`,
`mlperf-dlio`, `metadata-ha-smoke`, `metadata-ha-fault-smoke`, and
`metadata-ha-learner-read`. This is the fast regression set for Holt-native
metadata reads, chunked object publish, learner read freshness, and the
OpenRaft metadata HA path. You can narrow it to a single workload while
iterating:

```bash
scripts/run-ai-training-smoke.sh metadata-ha-fault-smoke
```

## The L2 benchmark framework (NoKV vs JuiceFS)

The headline comparison is a **boundary-labeled L2 (mounted FUSE) matrix**. Every
row carries the canonical label prefix
`boundary,system,metadata_tier,object_backend,cache_state,concurrency,tool,…`
(`bench/drivers/schema.py`), so the comparison is always **L2-vs-L2 under a
declared metadata tier and cache state** — never NoKV's faster L1 service path
against JuiceFS's mount. One entry point starts a disposable RustFS + Redis,
builds and mounts NoKV and JuiceFS, and runs the grid:

```bash
# quick: one NoKV tier (direct/WAL) + JuiceFS, concurrency 1
scripts/run-fs-benchmark.sh

# full matrix: both NoKV tiers (direct/WAL, raft/none), concurrency sweep
NOKV_BENCH_PROFILE=standard scripts/run-fs-benchmark.sh --matrix \
  --repeats 3 \
  --result-csv "bench/results/$(hostname).raw.csv" \
  --aggregate-csv "bench/results/$(hostname).aggregate.csv" \
  --env-json "bench/results/$(hostname).env.json" \
  --decompose-csv "bench/results/$(hostname).decompose.csv"
```

It runs **two co-equal drivers** against each mount:

- `bench/drivers/posix_bench.py` — a self-contained, `juicefs bench`-shaped driver
  (`bigfile` / `smallfile` / `metadata`-tree, plus the product-thesis
  `metadata_create_list` / `checkpoint` / `training_read`). Product-thesis
  workloads run once at `p=1`; primitive workloads are swept across the selected
  concurrency levels. `metadata_create_list` emits separate `create` and `list`
  rows so create latency and directory-enumeration latency are not mixed.
  `training_read` emits both a **cold** row (kernel page cache bypassed via
  `F_NOCACHE` / `posix_fadvise`) and a **warm** row, so the block-cache
  write-through gap is visible, not conflated.
- `bench/drivers/real_tools.py` — the actual `fio`, `mdtest` (+`mpirun`), and
  `juicefs bench` binaries parsed into the same schema. Absent tools surface as
  explicit `tool-missing` rows, never silently dropped. Point at local binaries
  via `NOKV_BENCH_FIO_BIN` / `NOKV_BENCH_MDTEST_BIN` / `NOKV_JUICEFS_BIN`.

**Tier fairness:** NoKV's `direct/WAL` tier (no consensus) is the apples-to-apples
opponent for JuiceFS-on-Redis; the `raft/none` tier is the opponent for a
consensus metadata backend (JuiceFS-on-TiKV). The tier is recorded on every row so
a comparison is never consensus-vs-non-consensus by accident.

**Regression gate** — store a baseline and fail on drift:

```bash
scripts/run-fs-benchmark-baseline.sh
scripts/run-fs-benchmark.sh --matrix --repeats 3 \
  --aggregate-csv /tmp/nokv-run.aggregate.csv \
  --baseline "bench/baselines/$(hostname).aggregate.csv"
# or standalone:
python3 scripts/compare-baseline.py --baseline bench/baselines/host.aggregate.csv run.aggregate.csv
```

`scripts/run-fs-benchmark-baseline.sh` is the clean local baseline command. By
default it runs `standard` profile, `--matrix`, `p=1 4 16`, NoKV `local` and
`raft` metadata tiers, and three repeats. It writes:

- `*.raw.csv` — every measured phase from every repeat.
- `*.aggregate.csv` — one row per comparison key, with canonical metric columns
  set to the median and extra `samples` / `*_p95` / `run_ids` / `env_ids`
  columns.
- `*.env.json` — host, git, third-party IOR/mdtest, RustFS, JuiceFS, Redis, fio,
  MPI, Rust, and run-parameter versions.
- `*.decompose.csv` — NoKV `/stats` before/after deltas for each measured phase.

The baseline gate should compare aggregate CSVs, not single-run raw CSVs. It
fails on metric regressions, baseline rows missing from the run, and blocking
caveats such as `workload-failed`, `parse-failed`, `tool-missing`, `fio-failed`,
`mdtest-failed`, or `juicefs-bench-failed`. For local exploratory runs, use
explicit allowances such as `--allow-missing-rows` or
`--allow-caveat-prefix tool-missing:`; CI should avoid those allowances unless
the missing coverage is intentional.

Native workload selection is split so product workloads and primitive workloads
cannot be mixed accidentally:

```bash
scripts/run-fs-benchmark.sh \
  --product-workloads metadata_create_list,training_read \
  --primitive-workloads smallfile,metadata

# Skip one side of the native driver:
scripts/run-fs-benchmark.sh --product-workloads none
scripts/run-fs-benchmark.sh --primitive-workloads none
```

**Latency decomposition** is part of the main entry point for NoKV rows. Pass
`--decompose` or `--decompose-csv PATH`; the runner snapshots the NoKV
`/stats` endpoint before and after each measured phase and writes a sidecar CSV
with metadata commit, Raft proposal, object writeback, object GET/PUT, and
read-plan-cache deltas. The canonical result CSV stays comparable across NoKV
and JuiceFS; the NoKV-only decomposition lives in the sidecar.

Orchestration is shared in `scripts/lib/fs-bench-common.sh` (RustFS / Redis /
server / mount lifecycle, cache-state mount flags, OS page-cache drop).
Configuration is via `NOKV_BENCH_*` env vars documented in that library; required
infra tools (rustfs/redis/juicefs/aws/python) must be present.

### L1 service-path reference (not the headline)

To measure NoKV's **L1 service path** (the SDK/client against the framed metadata
RPC) end to end, use `scripts/run-rustfs-e2e.sh`, which starts a disposable RustFS
endpoint and runs `nokv-bench`. Those rows are labeled `boundary=L1`; read them as
the engine/service ceiling, not as an L2 filesystem comparison — comparing them to
JuiceFS's mount would mix boundaries, which is exactly what the L2 matrix above
exists to prevent. The L2 matrix is the product-surface measurement.

These scripts produce local engineering evidence only. They are not MLCommons
official submission results and do not replace running the official MLPerf
Storage or DLIO harness against a NoKV FUSE mount. To capture an official
harness run reproducibly, mount NoKV first and wrap the official command:

```bash
scripts/run-official-training-benchmark.sh \
  --harness dlio \
  --mount /mnt/nokv \
  --result-dir bench/results/dlio-nokv \
  -- /path/to/official/dlio/command --data-dir /mnt/nokv
```

The wrapper records stdout/stderr/status, the exact command, mount path, and the
same environment JSON used by the local matrix. The score still comes from the
official harness, not from NoKV's generated local workload.

The gate also accepts the special workload `fuse-smoke` for the mounted POSIX
semantics check. It is not part of the default list because it requires a
working local FUSE installation and mount permissions:

```bash
scripts/run-ai-training-smoke.sh fuse-smoke
NOKV_AI_SMOKE_INCLUDE_FUSE=1 scripts/run-ai-training-smoke.sh
```

The special workload `metadata-raft-smoke` runs the explicit 3-voter OpenRaft
process smoke. It starts RustFS, starts three metadata voters, publishes data
through one endpoint, then reads the artifact through follower voters:

```bash
scripts/run-ai-training-smoke.sh metadata-raft-smoke
NOKV_AI_SMOKE_INCLUDE_METADATA_RAFT_SMOKE=1 scripts/run-ai-training-smoke.sh
```

For direct benchmark rows, use the `metadata-ha-*` workloads:

```bash
scripts/run-rustfs-e2e.sh --metadata-raft-log-sync none \
  --workload metadata-ha-smoke
scripts/run-rustfs-e2e.sh --metadata-raft-log-sync none \
  --workload metadata-ha-coalescing-smoke
```

`metadata-ha-smoke` validates normal replicated writes, follower reads, and
OpenRaft state. `metadata-ha-coalescing-smoke` fires concurrent independent
`create_file` requests so `metadata_raft_proposal_max_batch` can verify Raft
proposal coalescing.

For a disposable local RustFS-backed FUSE semantics smoke, use:

```bash
scripts/run-fuse-smoke.sh
```

The script builds `nokv`, starts RustFS, mounts a temporary NoKV FUSE
filesystem, and exercises mkdir, file write/read, file fsync, directory fsync,
rename, readdir, truncate, symlink/readlink, xattr roundtrip, access(2),
hardlink, statfs, lseek, fallocate, copy_file_range, rm, and rmdir through the
mounted filesystem. This is a correctness smoke, not a performance benchmark.

For JuiceFS-style mounted-filesystem compatibility gates, mount NoKV first
and run the external-suite driver against that mountpoint:

```bash
NOKV_FUSE_MOUNT=/mnt/nokv \
scripts/run-fuse-compat-gate.sh
```

The default compatibility gate runs a small namespace/data smoke plus xattr
roundtrip. Heavier suites are opt-in because they require external tools and
will expose the remaining POSIX hardening gaps until NoKV reaches the full
external-suite gate:

```bash
NOKV_FUSE_MOUNT=/mnt/nokv \
NOKV_FUSE_COMPAT_TESTS="basic xattr pjdfstest ltp" \
scripts/run-fuse-compat-gate.sh
```

This mirrors the way mature FUSE filesystems such as JuiceFS validate the
mounted boundary: `pjdfstest` for syscall-level POSIX behavior, LTP filesystem
groups for kernel-facing semantics, randomized filesystem operation tests for
state-space coverage, and separate performance runs for mdtest/fio/object-store
paths. NoKV keeps the default CI smoke smaller, then uses these external gates
when claiming POSIX compatibility.

The harness prints CSV. The exact column set is owned by
`bench/src/bin/nokv-bench.rs`; the important column families are:

```text
workload, profile, throughput, object stats, metadata store stats,
metadata_raft_* state, path_index stats, ReadDirPlus projection stats, caveat
```

Most benchmark workloads start a real single-node `metad` process and run the
Rust service client against its framed metadata RPC. HA workloads start local
multi-process metadata topologies. Object bytes are still read and written
directly by the client against the configured S3-compatible object store. This
keeps benchmark numbers attached to the deployable service boundary instead of
an in-process metadata shortcut.

Metadata smoke workloads use the SDK's ordered non-atomic `create_files`
batching for file create bursts. This measures the deployable SDK/server path
without charging one network round trip per independent file create; each
subrequest still has its own success or error result.

`metadata_atomic_applies`, `metadata_atomic_apply_commands`, and
`metadata_atomic_apply_max_batch` report how command batching reaches the Holt
atomic apply boundary. They are the key columns for checking whether request
coalescing reduced storage-engine apply calls rather than only coalescing
requests above the metadata engine.

`metadata_raft_*` columns report the OpenRaft state observed from the server
stats endpoint: current term, leader, last log index, last applied index,
snapshot/purge frontier, quorum freshness, and voter/learner counts. These
fields show whether an HA smoke run actually advanced the replicated metadata
group.

`metadata_raft_proposal_batches`, `metadata_raft_proposal_commands`,
`metadata_raft_proposal_max_batch`, and `metadata_raft_proposal_ns` report the
OpenRaft proposal layer. Use these together with the Holt apply columns to
distinguish Raft-entry coalescing from state-machine apply coalescing.

Object-backed workloads can be scaled without editing code:

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile standard \
  --workload mlperf-dlio \
  --object-backend rustfs \
  --object-concurrency 8 \
  --checkpoint-bytes 1048576 \
  --sample-bytes 65536 \
  --read-repeats 2 \
  --block-cache on
```

`--object-concurrency` controls parallel object publish/read work inside the
benchmark. `checkpoint-publish` stages checkpoint objects in parallel, then
keeps the final `rename_replace` sequence ordered so the latest-checkpoint
semantics stay clear. `--read-repeats` intentionally rereads the same sample per
directory; use it with `--block-cache on|off` to separate object-store latency
from cache reuse.

## Workloads

`mdtest-easy` creates files across many directories. File bodies are
metadata-only, so this isolates namespace create cost.

`mdtest-hard` creates many files in one shared directory. This stresses the hot
directory dentry prefix and Holt current tree.

`metadata-negative-lookup` creates present files, then times missing-path
lookups in the same directory shape. It verifies that live misses do not fall
back into history scans.

`artifact-index-lookup` seeds artifact paths and times indexed `stat_path`
lookups plus indexed directory listing. It measures the path-index fast path
without object reads.

`metadata-concurrent-read` uses the same artifact-index shape but runs
`stat_path` and indexed list operations through parallel workers after warmup.
Use it to measure metadata read concurrency, cache contention, and framed RPC
worker behavior.

`metadata-ha-learner-read` starts a local 3-voter OpenRaft metadata group plus
one learner. It writes through a voter endpoint, imports the write receipt into
a learner-only metadata client, then times repeated learner directory reads.
This isolates read-your-writes freshness and learner-side read scaling.

`checkpoint-publish` writes checkpoint bodies to the configured
S3-compatible object backend, then atomically promotes staged files with
`rename_replace`. This measures object put plus metadata publish, not metadata
alone.

`training-read` seeds a dataset tree, then times directory listing plus one
sample read per shard. The reported time excludes seed time and represents a
warm object read path through the configured backend.

`mlperf-dlio` uses deterministic generated data in an MLPerf Storage/DLIO-style
shape: dataset seed, timed training reads, and checkpoint writes with atomic
latest-checkpoint replacement. It is an official-style local gate, not an
MLCommons submission result.

`demo-dataset` uses a small public-dataset-shaped class/sample directory tree
without downloading external data. It is meant for demos and CLI validation, not
for CI performance claims.

## Profiles

| Profile | Use |
| --- | --- |
| `smoke` | Fast correctness and shape check: 4 KiB checkpoints and 512 B samples. |
| `standard` | Local performance baseline: 1 MiB checkpoints and 16 KiB samples. |
| `long` | Larger local stress run: 8 MiB checkpoints and 256 KiB samples. |

## JuiceFS-Style Evidence Ladder

JuiceFS is a useful reference because its tests are layered rather than a
single benchmark number:

| Layer | JuiceFS practice | NoKV equivalent |
| --- | --- | --- |
| Quick mounted smoke | `juicefs bench` and targeted mount checks | `scripts/run-fuse-smoke.sh` |
| POSIX compatibility | `pjdfstest`, LTP fs/fsx/io/fcntl groups, xattr tests | `scripts/run-fuse-compat-gate.sh` with `pjdfstest ltp xattr` |
| Random operation stress | Hypothesis fs state machines and long random-test runs with create/read/ls/delete/rename/link/truncate/walk mixes | Planned NoKV mounted random-operation gate; current coverage is unit/contract tests plus FUSE smoke |
| Metadata regression | mdtest scenarios across metadata engines, current-vs-previous comparison with tolerance | `mdtest-easy`, `mdtest-hard`, `metadata-ha-*`, L2 `mdtest`, and `compare-baseline.py` over aggregate CSV |
| Data path regression | fio and object-store bench paths | `checkpoint-publish`, `training-read`, `mlperf-dlio`, and future object microbench expansion |
| Long-run stress | vdbench and scheduled workflows | Planned long profile plus multi-node OpenRaft/object-store soak |

Use this ladder when interpreting results. A NoKV metadata microbench can prove
a Holt/OpenRaft hot path improved; it cannot by itself prove POSIX correctness,
object-store behavior, or training-cluster readiness.

## Current Caveats

Most workloads run a single-node `metad` process with a configured
S3-compatible object backend. `metadata-ha-smoke` and
`metadata-ha-fault-smoke` are the write-path HA exceptions:
they start a local OpenRaft metadata topology and report Raft state plus
metadata apply metrics. `metadata-ha-learner-read` adds a learner and reports
read metrics from that learner node.

The harness still does not include FUSE kernel caching, Python DataLoader
overhead, object-store multipart upload, or a multi-machine training cluster.
Treat metadata-only numbers as a metadata-service baseline, and object-backed
numbers as specific to the configured endpoint. Cluster claims must report the
metadata topology, network, object-store, cache, and durability settings.
