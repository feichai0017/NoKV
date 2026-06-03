<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Metadata Schema

NoKV-FS uses inode/dentry as canonical namespace truth. Path indexes are derived
accelerators for agent and artifact workflows; they are not the source of
truth.

## Core Records

```text
inode_current
  key: mount_id | inode_id
  val: InodeAttr

dentry_current
  key: mount_id | parent_inode | name
  val: DentryRecord + InodeAttr projection + optional BodyDescriptor

chunk_manifest_current
  key: mount_id | inode_id | generation | chunk_index
  val: BodyDescriptor when chunk_index = u64::MAX
  val: ChunkManifest otherwise

history
  key: family | user_key_len | user_key | inverted_commit_version
  val: previous versioned value
```

The dentry value intentionally stores a small inode projection. This makes
`ReadDirPlus` a single prefix scan on the dentry tree for the common case.

## Command Boundary

Metadata changes flow through `MetadataCommand`:

```text
request_id
kind
read_version
commit_version
primary family/key
predicates
mutations
watch projection
```

Predicates are the atomicity fence. A command that fails a predicate must not
apply any mutation.

## Current Status

Implemented hot path records:

- inode attributes;
- dentry projection;
- chunk manifest body summary plus per-chunk block descriptors;
- command dedupe;
- history records for previous values.

Planned records:

- path index for artifact/channel fast lookup;
- watch log indexed by scope;
- snapshot pins and GC floor;
- parent index for hardlink and subtree operations.

## Target Families

The target schema keeps full paths out of canonical truth. Path-like keys may be
used for acceleration, but inode/dentry records define the namespace.

```text
mount
  key: mount_id
  val: mount options, root inode, generation

inode_current
  key: mount_id | inode_id
  val: attributes, link count, body summary, generation

dentry_current
  key: mount_id | parent_inode | name
  val: child inode, file type, dentry generation, inode projection

manifest_current
  key: mount_id | inode_id | manifest_generation | chunk_index
  val: body summary at u64::MAX, or chunk block descriptors for real chunk ids

parent_index
  key: mount_id | child_inode | parent_inode | name
  val: link metadata

path_index
  key: mount_id | workspace_or_channel | normalized_path
  val: inode, dentry generation, root epoch

watch_log
  key: mount_id | watch_scope | sequence
  val: typed watch event

snapshot_pin
  key: mount_id | snapshot_id
  val: read frontier, root inode, retention evidence

gc_queue
  key: mount_id | epoch | manifest_id_hash
  val: pending object cleanup record
```

`path_index` entries must be verified against the canonical dentry generation
before use. A stale path-index hit falls back to inode/dentry traversal and can
be repaired later.
