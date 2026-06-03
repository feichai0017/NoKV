---
title: Benchmarks
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

NoKV-FS keeps product microbenchmarks inside the product crates and puts
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

The harness prints CSV:

```text
workload,profile,operations,seconds,ops_per_second,mb_per_second,samples_per_second,object_puts,object_gets,cache_hits,cache_hit_rate,manifest_chunks,manifest_blocks,checksum,shape,caveat
```

## Workloads

`mdtest-easy` creates files across many directories. File bodies are
metadata-only, so this isolates namespace create cost.

`mdtest-hard` creates many files in one shared directory. This stresses the hot
directory dentry prefix and Holt current tree.

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
| `smoke` | Fast correctness and shape check. |
| `standard` | Local performance baseline for development. |
| `long` | Larger local stress run. |

## Current Caveats

The current harness runs local Holt metadata and a configured S3-compatible
object backend. It does not include distributed metadata replication, FUSE
kernel caching, Python DataLoader overhead, object-store multipart upload, or
restart recovery.

Treat metadata-only numbers as a local engine baseline, and object-backed
numbers as specific to the configured endpoint. Distributed and training-cluster
claims need a separate benchmark that reports network, object-store, cache, and
durability settings.
