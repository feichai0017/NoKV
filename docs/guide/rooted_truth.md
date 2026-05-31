<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Rooted Truth

`meta/root` owns durable cluster and namespace lifecycle facts:

- mount registration and retirement;
- subtree authority declarations and handoff state;
- snapshot epoch publication and retirement;
- quota fences;
- topology and data-plane lifecycle facts;
- grants and seals used by control-plane protocols.

Root truth is deliberately separate from fsmeta inode/dentry storage. High
frequency namespace records do not live in `meta/root`; they are committed by
the selected fsmeta runtime.

## Storage

The rooted log has two implementations under `meta/root`:

- in-memory/storage test helpers;
- file-backed storage under `meta/root/storage/file`.

The file backend uses normal OS file operations directly. It does not depend on
the removed `storage/vfs` package.

## Relationship to Coordinator

The coordinator may cache and materialize root facts, but it must be able to
rebuild from root. If coordinator state disagrees with root, root wins.

## Relationship to raftstore

`raftstore` is expected to publish lifecycle outcomes through coordinator
or root-facing APIs once the distributed fsmeta adapter is complete. It should
not make root truth implicit in Holt state or Raft logs.
