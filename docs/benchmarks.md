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
`mlperf-dlio`, `metadata-ha-smoke`, and `metadata-ha-fault-smoke`. This is the
fast regression set for Holt-native metadata reads, chunked object publish, and
the shared-log HA path. You can narrow it to a single workload while iterating:

```bash
scripts/run-ai-training-smoke.sh metadata-ha-fault-smoke
```

The gate also accepts the special workload `fuse-smoke` for the mounted POSIX
semantics check. It is not part of the default list because it requires a
working local FUSE installation and mount permissions:

```bash
scripts/run-ai-training-smoke.sh fuse-smoke
NOKV_AI_SMOKE_INCLUDE_FUSE=1 scripts/run-ai-training-smoke.sh
```

The special workload `shared-log-smoke` runs the heavier checkpoint/bootstrap
process smoke. It starts RustFS, two initial metadata voters, publishes data,
compacts through a metadata checkpoint, then brings up a late voter and learner
that must bootstrap from checkpoint plus retained tail:

```bash
scripts/run-ai-training-smoke.sh shared-log-smoke
NOKV_AI_SMOKE_INCLUDE_SHARED_LOG_BOOTSTRAP=1 scripts/run-ai-training-smoke.sh
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

The harness prints CSV:

```text
workload,profile,operations,seconds,ops_per_second,mb_per_second,samples_per_second,object_puts,object_put_bytes,object_gets,object_get_bytes,cache_hits,cache_hit_bytes,cache_hit_rate,manifest_chunks,manifest_blocks,metadata_commits,metadata_dedupe_hits,metadata_predicates,metadata_prefix_empty_predicates,metadata_log_entries,metadata_log_commands,metadata_log_max_batch,metadata_log_stale_reads,metadata_gets,metadata_get_user_strong,metadata_get_write_plan_local,metadata_get_snapshot,metadata_scans,metadata_scan_user_strong,metadata_scan_write_plan_local,metadata_scan_snapshot,metadata_scan_visited,metadata_scan_returned,metadata_history_lookups,metadata_current_puts,metadata_current_deletes,metadata_history_writes,metadata_watch_writes,metadata_dedupe_writes,metadata_commit_prepare_ns,metadata_atomic_apply_ns,path_index_lookups,path_index_hits,path_index_misses,path_index_stale,path_index_scan_stale,path_index_fallback,path_index_hit_rate,create_files_batches,create_files_entries,create_dirs_batches,create_dirs_entries,read_dir_plus_calls,read_dir_plus_entries,read_dir_plus_projection_hits,read_dir_plus_projection_hit_rate,object_concurrency,read_repeats,block_cache,checksum,shape,caveat
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

## Current Caveats

Most workloads run a single-node `metad` process with a configured
S3-compatible object backend. `metadata-ha-smoke` and
`metadata-ha-fault-smoke` are the exceptions: they start a local shared-log
metadata topology and report log-entry, command, batch, and stale-read metrics.

The harness still does not include FUSE kernel caching, Python DataLoader
overhead, object-store multipart upload, or a multi-machine training cluster.
Treat metadata-only numbers as a metadata-service baseline, and object-backed
numbers as specific to the configured endpoint. Cluster claims must report the
metadata topology, network, object-store, cache, and durability settings.
