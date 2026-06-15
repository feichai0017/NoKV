<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# AI Training

NoKV targets training clusters where file bodies live in object storage and
metadata needs to be fast, typed, and easy to mount.

## Target Workloads

- immutable training datasets;
- manifest-heavy dataset directories;
- checkpoint publish and resume;
- experiment artifacts;
- agent workspace inputs and outputs.

## Access Paths

FUSE is the mounted-file frontend for tools that expect paths. The Rust SDK and
Python/fsspec binding are the lower-overhead paths for native jobs.

```text
PyTorch / training process
  -> FUSE, Rust SDK, or Python/fsspec
  -> nokv-meta
  -> Holt metadata
  -> S3-compatible object store
```

The current FUSE frontend maps inode operations to metadata lookups plus object
range reads, and uses buffered write-on-close publishing for writes. Read-only
snapshot mounts expose pinned input subtrees without allowing mutation.

FUSE should not be the only high-performance path. Training frameworks that can
use a native client should bypass kernel/FUSE overhead and call the Rust or
Python API directly:

```text
mounted-file path:
  existing tool -> FUSE -> metadata/object service

performance path:
  dataloader/checkpoint writer -> SDK/fsspec -> metadata/object service
```

The Python binding exposes the Rust SDK's batch range primitive instead of
rebuilding range planning above POSIX. A dataloader can submit many shard sample
ranges in one call, and the client batch-opens read plans before object reads.
For callers that own a reusable CPU staging buffer, `read_ranges_batch_into`
fills a caller bytearray and returns per-shard `(offset, len)` windows. For
callers that want NoKV-owned staging memory, `ReadBuffer` and
`read_ranges_batch_buffer` reuse a Rust-owned buffer and run the SDK read while
the Python GIL is released. `RangeBatchPlan` lets a Python dataloader prepare
the native range-batch layout once: normalized requests, packed output windows,
coalesced read windows, and the ordered metadata batch-open request. Repeated
reads reuse that static layout for `ReadBuffer` fills, but still batch-open
metadata read plans on every read, so path visibility and generation fences are
unchanged. The prepared executor borrows the static plan during each read; it
does not rebuild or clone the path, range-offset, or coalesced-window layout in
the hot loop. `RangeBatchReader` packages that plan with a reusable
NoKV-owned staging buffer so a dataloader can prepare the batch once and call
`read()` for each training step. `RangeBatchEpochReader` groups multiple
prepared readers into a resettable round-robin epoch iterator, matching a
long-lived DataLoader worker that cycles through preplanned shard batches
without rebuilding Python request objects. Workers can step one batch at a time
with `read_next()` or fill every prepared batch with one GIL-released
`read_all()` call; `read_all()` runs prepared batch readers through persistent
bounded native workers so shard batches can overlap metadata batch-open and
object reads without opening all object-store responses at once or respawning
threads for every epoch.
Exact single-range windows already write through the object executor into the
staging buffer; multi-range coalesced windows use guarded scatter direct-write
only when that does not expand the physical block plan.
Gap-coalesced cold reads stay coalesced; warm gap windows scatter directly from
the local block cache only when every semantic range is already cached. This
should still be described as staged direct-write rather than
zero-copy: `ReadBuffer` now sits behind an explicit staging-memory boundary and
supports `memory_kind="system"` plus Unix `memory_kind="page_locked"`.
`page_locked` uses host `mlock` to keep CPU staging pages resident; it is not
CUDA `cudaHostAlloc`, RDMA memory registration, or HBM-backed storage. It
exposes a read-only `ReadBufferView` export token that pins the logical buffer
contents against resize/refill while the view is alive. The current Python
package keeps `abi3-py39`, so this is not a PEP 3118 memoryview; a true
memoryview should be added behind a Python 3.11+ or non-abi3 build once that
compatibility tradeoff is acceptable.

## Cache Direction

Training jobs should cache attributes, dentries, negative lookups, and object
range reads locally. Cache invalidation should come from typed watch events
rather than raw key notifications.

The target cluster deployment should run a node-local cache agent on GPU or
training nodes. It can prefetch dataset shards, keep hot object ranges on local
NVMe, and subscribe to metadata watch events for invalidation.
