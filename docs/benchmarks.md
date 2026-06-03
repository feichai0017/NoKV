---
title: Benchmarks
---

<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Benchmarks

NoKV-FS includes a local workload harness for metadata and AI-training access
patterns:

```bash
cargo run --release -p nokvfs-client --bin nokv-fs-bench -- \
  --profile smoke \
  --workload all
```

The harness prints CSV:

```text
workload,profile,operations,seconds,ops_per_second,checksum,shape,caveat
```

## Workloads

`mdtest-easy` creates files across many directories. File bodies are
metadata-only, so this isolates namespace create cost.

`mdtest-hard` creates many files in one shared directory. This stresses the hot
directory dentry prefix and Holt current tree.

`checkpoint-publish` writes checkpoint bodies to the local object backend, then
atomically promotes staged files with `rename_replace`. This measures object put
plus metadata publish, not metadata alone.

`training-read` seeds a dataset tree, then times directory listing plus one
sample read per shard. The reported time excludes seed time and represents a
warm local-object read path.

## Profiles

| Profile | Use |
| --- | --- |
| `smoke` | Fast correctness and shape check. |
| `standard` | Local performance baseline for development. |
| `long` | Larger local stress run. |

## Current Caveats

The current harness runs local Holt metadata and local filesystem object
storage. It does not include distributed metadata replication, remote S3/RustFS
latency, FUSE kernel caching, Python DataLoader overhead, object-store
multipart upload, or restart recovery.

Treat the numbers as a local engine baseline. Distributed and training-cluster
claims need a separate benchmark that reports network, object-store, cache, and
durability settings.
