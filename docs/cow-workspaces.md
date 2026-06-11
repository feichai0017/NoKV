<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Copy-on-Write Workspaces

NoKV is a **CoW workspace runtime** — "git for live, large, derived state." Where
git versions small text at dev time, NoKV gives `snapshot` / `clone` / `diff` /
`rollback` over **live filesystem namespaces** (code + data + model weights) as
runtime primitives, with the **data shared zero-copy** (clone never copies file
bodies) and a **native, content-addressed `diff`** of what changed.

The differentiating capability: spin up 100 agent workspaces from one base — each
sharing the base's object blocks zero-copy and writing only its own delta — then
**see exactly what an agent changed** (a content-addressed `diff`, including the
untracked build/dependency output a `git diff` misses) and **revert** a workspace
to an earlier point. A pluggable metadata engine (Redis, TiKV) cannot offer the
diff + rollback + sharing-aware GC as one integrated story — it requires owning
the copy-on-write metadata engine and the object layout together, which NoKV
does. (Clone's *metadata* cost is O(entries); the *data* is what stays zero-copy.
A change-bounded diff and batched/lazy clone are tracked future work.)

## The operations

All operate on a directory **subtree** (a "workspace") and are atomic + GC-safe.

| Op | Service API | Semantics |
|----|-------------|-----------|
| **snapshot** | `snapshot_subtree(root) -> SnapshotPin{root, read_version}` | Read-only, zero-copy MVCC version pin. Pins a stable view and protects its object blocks from GC. |
| **clone** | `clone_subtree(src) -> CloneHandle{root, snapshot_id}` | **Writable fork.** New root sees all of the source; **shares object blocks zero-copy** (same `generation` → same object keys); diverges on write (CoW). Metadata cost is O(entries) (one record per node); the **data is never copied**. |
| **diff** | `diff_subtrees(a, b) -> Vec<SubtreeDelta>` | Reports `Added` / `Removed` / `Modified` paths, each with a **content digest + net `size_delta`** (content-addressed, not just nominal). An unchanged shared file (same `generation`) is skipped; a rewrite surfaces as `Modified`. (Today the walk is O(tree); a change-bounded diff is future work.) |
| **rollback** | `rollback_subtree(target, snapshot_id)` | Revert a workspace to a prior snapshot. Clone-from-snapshot + atomic graft onto the target (keeps its inode identity). The discarded delta's blocks become GC-reclaimable; restored blocks survive. |

Path variants (`clone_subtree_path`, `clone_subtree_path_into`, `diff_subtrees_path`,
`rollback_subtree_path`) take string paths.

## The agent workflow

```text
base workspace  ──snapshot──▶  base@v1   (immutable, shareable)
       │
   clone(base) ──▶  /forks/agent-1   (writable, shares base blocks)
       │
   agent runs: stat / read / write / rename …  (local hot path, no quorum)
       │
   diff(base, /forks/agent-1)  ──▶  what the agent changed
       │
   ┌── success: keep / promote the fork
   └── failure: rollback(/forks/agent-1, base@v1)   or   discard the fork
```

100 agents off one base share the base's files and object blocks; each fork's
writes get a fresh `generation` → new object keys, so forks never clobber each
other or the base.

## CLI

```sh
# Clone a base subtree into a new, navigable workspace
nokv clone /base /forks/agent-1
# → cloned /base -> /forks/agent-1 root=N snapshot=M

# See what diverged between two subtrees
nokv diff /base /forks/agent-1
# A    /b          (added in the fork)
# M    /a          (modified)
# D    /old        (removed)
```

(`snapshot` and `rollback` are exposed at the service/RPC layer; CLI wiring lands
with the surrounding workspace-management commands.)

## Guarantees

- **Zero-copy sharing.** A clone references the source's object blocks by key
  (`blocks/{mount}/{inode}/{generation}/…`); only small metadata records are
  copied. Cloning a 1 TB dataset workspace copies kilobytes of metadata, not the
  data.
- **CoW isolation.** A write in any workspace mints a fresh generation under its
  own inode → new object keys. Forks and the base are fully isolated; neither
  sees the other's post-clone writes.
- **GC safety (the `owns_block_object_key` invariant).** An inode's GC reclaims
  only blocks **it minted** (its `{inode}/{generation}` prefix). A workspace
  never enqueues another workspace's shared blocks for deletion, and a retained
  snapshot pin keeps still-shared base blocks protected (`blocked_by_snapshots`).
- **Leased pins (no permanent GC blocker).** A snapshot pin carries a renewable
  lease (`renew_snapshot`); an abandoned pin — a crashed holder that never
  retires it — **expires** and stops holding the GC retention floor down, so a
  forgotten pin can never block reclamation forever. Expired pins are reaped
  during GC.
- **Atomicity.** Clone-link, divergent publish, and the rollback graft each
  commit in a single predicate-guarded metadata transaction. Crash-in-between
  leaves orphan objects (GC-able), never a corrupt namespace.

## Why single-node (and how it scales later)

A live workspace has a **single writer-owner**; read-only snapshots are shared by
many readers. This relaxes POSIX cross-node coherence (a tax AI workloads don't
need) and avoids consensus-replicated metadata entirely. Cloud scale comes from
**sharding + lease** (compute disaggregated over durable S3 + a tiny control
plane that owns only the shard map and owner leases), not from a distributed
metadata quorum. See `architecture.md`.

## Performance

- Metadata hot path is local (no quorum, no Redis/TiKV round-trip): low-latency
  `lookup`/`stat`/`readdir`.
- Writes: ~213 MiB/s sequential (release), durable — `close` drains to S3.
- `clone` / `snapshot` / `rollback` are O(metadata) pointer/record operations,
  not data copies.
