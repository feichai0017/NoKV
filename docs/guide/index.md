<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Documentation

NoKV is centered on **fsmeta**, a filesystem-shaped metadata API for AI agent
workspaces, artifact stores, DFS frontends, and object namespace layers.

The current repository is intentionally smaller than previous iterations:

- `fsmeta` owns the namespace model, layout, compiler, executor, local Badger
  runtime, server, and client.
- `meta/root` owns rooted lifecycle and authority truth.
- `coordinator` owns rebuildable routing, TSO, discovery, and scheduling views.
- `raftstore` is the Rust/OpenRaft/Holt distributed data-plane target.

The old Go `local`, `storage`, `txn`, `raftstore`, and `experimental` package
trees were removed from the mainline.

## Start Here

| Topic | Doc |
|---|---|
| Build and run local fsmeta | [Getting Started](./getting_started) |
| Layering and package ownership | [Architecture](./architecture) |
| Target NoKV-FS product architecture | [NoKV-FS Design](./nokv_fs_design) |
| fsmeta API and data model | [fsmeta](./fsmeta) |
| Rooted truth | [Rooted Truth](./rooted_truth) |
| Coordinator | [Coordinator](./coordinator) |
| Recovery and lifecycle notes | [Recovery](./recovery) |
| Stats and observability | [Stats & Observability](./stats) |
| Testing strategy | [Testing](./testing) |
| Code ownership rules | [Code Contract](./development/code_contract) |

## Architecture at a Glance

```text
Application / SDK
  -> fsmeta API
  -> fsmeta/exec
  -> fsmeta/backend
  -> fsmeta/runtime/local  -> Badger
  -> coordinator + meta/root + raftstore  -> Holt
```

NoKV keeps namespace semantics above the storage engine. Badger and Holt are
persistence choices; they do not own the inode/dentry model or workspace API.

The long-term product direction is described in [NoKV-FS Design](./nokv_fs_design):
Rust-native clients and metadata services, distributed Holt-backed metadata
shards, and object storage for file bodies.
