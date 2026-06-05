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
cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- \
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
default bucket, runs `nokv-fs-bench`, and removes its temporary RustFS data
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

For a local NoKV-FS versus JuiceFS same-shape comparison, mount JuiceFS first
and pass its mountpoint to the comparison script:

```bash
JUICEFS_MOUNT=/mnt/jfs \
NOKV_COMPARE_PROFILE=standard \
scripts/run-training-comparison.sh
```

The script runs NoKV-FS through the RustFS-backed `mlperf-dlio` workload, then
runs a generated dataset/checkpoint workload with the same shape inside the
existing JuiceFS mount. This is useful for local engineering comparisons. It is
not an MLCommons official submission result and does not replace running the
official MLPerf Storage or DLIO harness against a NoKV-FS FUSE mount.

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

For a disposable local RustFS-backed FUSE semantics smoke, use:

```bash
scripts/run-fuse-smoke.sh
```

The script builds `nokv-fs`, starts RustFS, mounts a temporary NoKV-FS FUSE
filesystem, and exercises mkdir, file write/read, file fsync, directory fsync,
rename, readdir, truncate, symlink/readlink, xattr error handling, access(2),
rm, and rmdir through the mounted filesystem. This is a correctness smoke, not
a performance benchmark.

For JuiceFS-style mounted-filesystem compatibility gates, mount NoKV-FS first
and run the external-suite driver against that mountpoint:

```bash
NOKV_FUSE_MOUNT=/mnt/nokv \
scripts/run-fuse-compat-gate.sh
```

The default compatibility gate runs a small namespace/data smoke plus xattr
roundtrip. Heavier suites are opt-in because they require external tools and
will expose unsupported POSIX semantics until NoKV-FS reaches the full POSIX
gate:

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
`bench/src/bin/nokv-fs-bench.rs`; the important column families are:

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
cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- \
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

| Layer | JuiceFS practice | NoKV-FS equivalent |
| --- | --- | --- |
| Quick mounted smoke | `juicefs bench` and targeted mount checks | `scripts/run-fuse-smoke.sh` |
| POSIX compatibility | `pjdfstest`, LTP fs/fsx/io/fcntl groups, xattr tests | `scripts/run-fuse-compat-gate.sh` with `pjdfstest ltp xattr` |
| Random operation stress | Hypothesis fs state machines and long random-test runs with create/read/ls/delete/rename/link/truncate/walk mixes | Planned NoKV mounted random-operation gate; current coverage is unit/contract tests plus FUSE smoke |
| Metadata regression | mdtest scenarios across metadata engines, current-vs-previous comparison with tolerance | `mdtest-easy`, `mdtest-hard`, `metadata-ha-*`; current-vs-baseline comparison still needs an automated gate |
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
