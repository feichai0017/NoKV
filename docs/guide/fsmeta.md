<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# fsmeta

`fsmeta` is NoKV's native workspace metadata service. It exposes a
filesystem-shaped namespace for agent workspaces, artifact stores, DFS
frontends, and object namespace layers. It does not store large object bodies
or implement a POSIX data plane.

## API Surface

The public wire API is in `pb/fsmeta/fsmeta.proto`; the domain types are in
`fsmeta/model`.

| RPC | Semantics |
|---|---|
| `Create` | Atomically creates a dentry and inode. |
| `Lookup` / `LookupPlus` | Reads a dentry, optionally fused with inode attributes. |
| `GetAttr` / `BatchGetAttr` | Reads inode attributes directly by inode ID for FUSE low-level style clients. |
| `ReadDir` / `ReadDirPlus` | Reads a directory page, optionally fused with inode attributes. |
| `UpdateInode` | Updates mutable inode metadata and bounded opaque attrs. |
| `Rename` | Moves one namespace entry when the destination does not exist. |
| `RenameReplace` | Atomically publishes or overwrites a file entry. |
| `RenameSubtree` | Moves a subtree root; distributed authority is handled by rooted truth. |
| `Link` | Adds a second dentry for one existing non-directory inode. |
| `Unlink` | Removes one non-directory dentry and updates or deletes the inode. |
| `Remove` | Product-facing single-entry remove; same non-directory delete semantics as `Unlink`, with removed inode metadata returned. |
| `RemoveDirectory` | Deletes one empty directory after an atomic empty-dir check. |
| `SnapshotSubtree` / `RetireSnapshotSubtree` | Publishes and retires MVCC read-version tokens. |
| `WatchSubtree` | Prefix-scoped change stream with cursor, ack, replay, and overflow handling. |
| `client.CreateView` / `CreateReadOnlySnapshotView` | Client-side sub-agent capability over a subtree; rules are relative path prefixes and snapshot views reject all mutation. |
| Writer sessions | Open, heartbeat, close, and expire exclusive writer leases. |
| Quota usage | Persistent usage counters plus rooted quota fences. |

## Data Model

`fsmeta/model` owns the semantic records:

- mount identity;
- inode attributes;
- dentry mappings;
- writer sessions;
- quota usage;
- snapshot tokens;
- watch events.

`fsmeta/layout` owns the ordered key layout and value codecs. The main key
families are:

| Family | Purpose |
|---|---|
| mount | mount-local metadata anchor |
| inode | inode attributes |
| dentry | parent/name to inode mapping |
| parent | reverse child inode to parent/name link index |
| path | derived path-to-dentry lookup index for agent workspace fast paths |
| chunk | reserved body-reference layout, not a data-plane API |
| session | writer session and inode-owner records |
| usage | quota accounting |
| snapshot | local snapshot registry |

`InodeRecord.OpaqueAttrs` is caller-owned bytes capped at 16 KiB. Artifact
publishers should use the canonical `fsmeta/model.BodyDescriptor` JSON shape:
`producer`, `digest_uri`, `size`, `content_type`, `body_ref`, and `generation`.
The client helper `PublishArtifact` writes this descriptor into a staged inode
and then atomically renames or replaces the final namespace entry. `Remove` and
`RenameReplace` return the old inode metadata, so callers can decode the old
descriptor and retry external body GC after namespace mutation succeeds.

## Execution Boundary

`fsmeta/exec.Executor` depends on `fsmeta/backend.Store`:

```go
type Store interface {
    ReserveTimestamp(ctx context.Context, count uint64) (uint64, error)
    Get(ctx context.Context, key []byte, version uint64) ([]byte, bool, error)
    BatchGet(ctx context.Context, keys [][]byte, version uint64) (map[string][]byte, error)
    Scan(ctx context.Context, startKey, prefix []byte, limit uint32, version uint64) ([]backend.KV, error)
    CommitMetadata(ctx context.Context, command backend.MetadataCommand) (backend.MetadataCommitResult, error)
}
```

The executor compiles namespace operations into `MetadataCommand` objects:
backend-neutral predicates, mutations, watch keys, metadata families, and an
optional explicit commit version. Runtime packages own concrete commit
mechanics. Local runtimes may flatten families into one keyspace; the Rust
distributed runtime maps them to Holt current trees plus a shared history tree.
Prefix-bounded scans are part of the backend contract so directory, path,
session, and audit reads can use storage-native prefix iterators instead of
walking an entire metadata family.
Dentry values carry a small inode projection so `ReadDirPlus` can return common
single-link files and directories from the dentry scan alone. Hard-linked files
still fall back to inode reads until the parent index can prove projection
freshness across all parents.

The path family is deliberately derived, not canonical truth. It accelerates
common artifact and checkpoint path lookups from a view root, but every hit is
validated against canonical dentry/inode state. Multi-component hits also
validate the full canonical path, so stale descendant index entries left by a
subtree rename cannot make an old path visible again.

## Local Runtime

`fsmeta/runtime/local` is the default runnable implementation. It stores
versioned fsmeta records directly in Badger and implements the backend contract
inside the runtime package. This keeps the local path easy to reason about:
there is no separate generic local KV database and no external transaction
engine layer. Badger `SyncWrites` is disabled by default for local demo
throughput; strict power-loss durability requires explicit Badger options.

## Distributed Target

Distributed fsmeta keeps the same semantic compiler and backend contract. The
target path is:

```text
fsmeta/server
  -> fsmeta/exec
  -> coordinator route / TSO / root view
  -> raftstore mount-scoped Raft group
  -> Holt multi-tree state machine
```

`raftstore` must not import `fsmeta/exec` or reinterpret namespace
semantics. It executes the already-compiled predicate and mutation contract at
the replicated data-plane boundary.

## Agent Workspace Fit

Agent workspaces need a durable and watchable namespace for artifacts,
checkpoints, memory objects, tool outputs, and staged publish. The inode/dentry
model is intentionally familiar: it gives hierarchical discovery, atomic
publish, snapshots, and watches without forcing every application to rebuild a
filesystem metadata layer on top of Redis, SQL tables, or raw object keys.

Scoped views are client-side capabilities for sub-agents. A view has one mount,
one root inode, and longest-prefix access rules over relative paths; no matching
rule means deny. Read-only snapshot views bind the same path API to a snapshot
read version, so `/workspace/input` style mounts can be exposed without giving
the child agent a mutable full-mount client. The underlying fsmeta service still
owns namespace truth; views do not create another root/control-plane authority.
