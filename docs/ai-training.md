<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# AI Training

NoKV-FS targets training clusters where file bodies live in object storage and
metadata needs to be fast, typed, and easy to mount.

## Target Workloads

- immutable training datasets;
- manifest-heavy dataset directories;
- checkpoint publish and resume;
- experiment artifacts;
- agent workspace inputs and outputs.

## Access Paths

FUSE gives immediate compatibility with tools that expect paths. The Rust SDK
is the lower-overhead path for native jobs and future Python bindings.

```text
PyTorch / training process
  -> FUSE or SDK
  -> nokvfs-meta
  -> Holt metadata
  -> S3-compatible object store
```

The current FUSE frontend is read-only and maps inode operations to metadata
lookups plus object range reads. Write support should use write-on-close
staging rather than exposing object-store partial writes.

FUSE should not be the only high-performance path. Training frameworks that can
use a native client should bypass kernel/FUSE overhead and call the Rust or
Python API directly:

```text
compatibility path:
  existing tool -> FUSE -> metadata/object service

performance path:
  dataloader/checkpoint writer -> SDK/fsspec -> metadata/object service
```

## Cache Direction

Training jobs should cache attributes, dentries, negative lookups, and object
range reads locally. Cache invalidation should come from typed watch events
rather than raw key notifications.

The target cluster deployment should run a node-local cache agent on GPU or
training nodes. It can prefetch dataset shards, keep hot object ranges on local
NVMe, and subscribe to metadata watch events for invalidation.
