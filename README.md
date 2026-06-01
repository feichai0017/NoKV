<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV

NoKV is a metadata execution framework for AI agent workspaces and distributed
namespace services.

The product core is `fsmeta`: a filesystem-shaped metadata model with inode,
dentry, watch, snapshot, quota, session, atomic publish, remove, and subtree
move semantics. The local demo runtime uses Badger directly. The distributed
track keeps `meta/root` and `coordinator` in Go and develops the replicated
data plane in `raftstore` over Holt.

NoKV does not own object bodies, model weights, checkpoint payloads, or POSIX
file data. Those bytes stay in object stores, local filesystems, model stores,
or application-defined body stores. NoKV owns namespace truth, compact body
references, metadata versions, watches, and lifecycle coordination.

## Architecture

```text
application / SDK
  -> fsmeta API
  -> fsmeta/exec semantic compiler and executor
  -> fsmeta/backend metadata command contract
  -> fsmeta/runtime/local       # local Badger demo path
  -> meta/root + coordinator    # distributed truth/control plane
  -> raftstore               # Rust/Holt distributed data-plane target
```

The main package boundaries are:

| Package | Responsibility |
|---|---|
| `fsmeta/model` | Inode, dentry, mount, quota, session, watch, and snapshot domain model. |
| `fsmeta/layout` | Ordered namespace key layout and value codecs. |
| `fsmeta/backend` | Metadata command backend contract consumed by fsmeta execution. |
| `fsmeta/exec` | Semantic executor and compiler for namespace operations. |
| `fsmeta/runtime/local` | Embedded Badger-backed fsmeta runtime for demos and tests. |
| `meta/root` | Rooted truth for topology, authority, lifecycle facts, grants, and seals. |
| `coordinator` | Routing, TSO, store discovery, root-event publish, and serving-plane rebuild. |
| `raftstore` | Rust replacement data plane using OpenRaft and Holt family current/history trees. |
| `pb` | Public protobuf wire contracts. |

## fsmeta

`fsmeta` is a durable, versioned, watchable workspace namespace. It is not a
FUSE frontend, not a POSIX filesystem, and not an object store. It is the
metadata kernel consumed by AI workload stores, artifact SDKs, workspace
services, DFS frontends, and object namespace layers.

Core primitives:

| Primitive | Semantics |
|---|---|
| `Create` | Atomically creates a dentry and inode. |
| `Lookup` / `LookupPlus` | Reads a dentry, optionally fused with inode attributes. |
| `GetAttr` / `BatchGetAttr` | Reads inode attributes directly by inode ID. |
| `ReadDir` / `ReadDirPlus` | Reads a directory page, optionally fused with inode attrs. |
| `UpdateInode` | Updates mutable inode metadata and bounded opaque attrs. |
| `Rename` | Moves one namespace entry when the destination does not exist. |
| `RenameReplace` | Atomically publishes or overwrites a file entry. |
| `RenameSubtree` | Moves a subtree root with rooted authority handoff. |
| `Remove` | Removes one non-directory entry and returns removed inode metadata. |
| `RemoveDirectory` | Removes one empty directory after child-count verification. |
| `Link` / `Unlink` | Non-directory hard-link semantics with link-count updates. |
| `SnapshotSubtree` | Creates an MVCC read-version token for point-in-time reads. |
| `WatchSubtree` | Prefix-scoped change feed with cursor, ack, replay, and overflow handling. |
| Writer sessions | Open, heartbeat, close, and expire staged writers. |
| Quota usage | Persistent counters plus rooted quota fences. |

`InodeRecord.OpaqueAttrs` is application-owned metadata capped at 16 KiB. It is
for compact body references, checksums, media types, or small descriptors, not
large artifact bodies.

## Local Demo Runtime

The default runnable path is the local Badger-backed fsmeta server:

```sh
go run ./cmd/nokv-fsmeta \
  --addr 127.0.0.1:8090 \
  --metrics-addr 127.0.0.1:9400 \
  --local-work-dir ./nokv-fsmeta-local \
  --local-mount-id default \
  --local-mount-key-id 1
```

The local runtime disables Badger `SyncWrites` by default for demo throughput.
It preserves fsmeta MVCC and restart semantics, but strict power-loss durability
requires opening the runtime with Badger sync writes enabled.

Library usage:

```go
package main

import (
	"context"

	"github.com/feichai0017/NoKV/fsmeta/model"
	fsmetalocal "github.com/feichai0017/NoKV/fsmeta/runtime/local"
)

func main() {
	ctx := context.Background()
	rt, err := fsmetalocal.Open(ctx, fsmetalocal.Options{
		WorkDir: "./nokv-fsmeta-local",
		Mount:   model.MountIdentity{MountID: "default", MountKeyID: 1},
	})
	if err != nil {
		panic(err)
	}
	defer rt.Close()

	_, err = rt.Executor.Create(ctx, model.CreateRequest{
		Mount:  "default",
		Parent: model.RootInode,
		Name:   "hello.txt",
		Attrs:  model.CreateAttrs{Type: model.InodeTypeFile, Size: 13},
	})
	if err != nil {
		panic(err)
	}
}
```

## Distributed Track

The distributed control plane remains in Go:

```text
meta/root      rooted lifecycle and authority truth
coordinator    rebuildable routing, TSO, discovery, and root-event serving
```

The data plane target is `raftstore`, not the deleted Go `raftstore` /
`txn` / `local` stack. `raftstore` exposes the metadata-native protobuf
boundary while moving execution to Rust, OpenRaft, and Holt multi-tree storage.

Operational entrypoints are intentionally split:

```sh
go run ./cmd/nokv meta-root ...
go run ./cmd/nokv coordinator ...
go run ./cmd/nokv-fsmeta ...
cargo run --manifest-path raftstore/Cargo.toml -p nokv-raftstore-server -- ...
```

`cmd/nokv` is now only the Go control-plane CLI. It does not start the deleted
Go raftstore.

## Development

Common checks:

```sh
go test ./...
cargo test --manifest-path raftstore/Cargo.toml --workspace
make lint
make test
NOKV_FSMETA_BENCH_MODE=local make fsmeta-bench
```

Repository structure and review rules live in
`docs/guide/development/code_contract.md` and
`docs/guide/development/pr_review_checklist.md`.

## License

[Apache-2.0](./LICENSE)
