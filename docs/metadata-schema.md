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
  val: BodyDescriptor

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
- chunk manifest body descriptor;
- command dedupe;
- history records for previous values.

Planned records:

- path index for artifact/channel fast lookup;
- watch log indexed by scope;
- snapshot pins and GC floor;
- parent index for hardlink and subtree operations.
