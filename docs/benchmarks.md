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
  --workload all \
  --metadata-mode local
```

The default object backend is a local RustFS endpoint at
`http://127.0.0.1:9000`, bucket `nokv`, with the standard local RustFS
development credentials. Start RustFS first when running object-backed
workloads.

The harness prints CSV:

```text
workload,profile,operations,seconds,ops_per_second,mb_per_second,samples_per_second,object_puts,object_gets,cache_hits,cache_hit_rate,manifest_chunks,manifest_blocks,metadata_mode,object_concurrency,read_repeats,block_cache,checksum,shape,caveat
```

`--metadata-mode local` runs the in-process Holt metadata service. It is the
fast local engine baseline. `--metadata-mode remote` starts a real `metad`
server and runs the Rust remote SDK against its framed metadata RPC; object
bytes are still read and written directly by the client against the configured
S3-compatible object store. Use remote mode when evaluating the deployable
service boundary.

Object-backed workloads can be scaled without editing code:

```bash
cargo run --release -p nokvfs-bench --bin nokv-fs-bench -- \
  --profile standard \
  --workload mlperf-dlio \
  --metadata-mode remote \
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

The current harness runs either local Holt metadata or a single-node remote
`metad` process with a configured S3-compatible object backend. It does not
include distributed metadata replication, FUSE kernel caching, Python
DataLoader overhead, object-store multipart upload, or restart recovery.

Treat metadata-only numbers as a local engine baseline, and object-backed
numbers as specific to the configured endpoint and metadata mode. Distributed
and training-cluster claims need a separate benchmark that reports network,
object-store, cache, and durability settings.
