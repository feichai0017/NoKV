<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Checkpointing

Checkpointing needs atomic metadata publication even when bytes are stored in an
external object store.

## Current Primitive

`PublishArtifact` uploads the object body first, then publishes metadata with a
single metadata command:

```text
body bytes -> object store
metadata command:
  - inode attr
  - dentry projection
  - body descriptor
```

The namespace entry appears only after the metadata command commits.

## Required Next Semantics

- atomic replace that returns the old body descriptor;
- durable GC retry for old and orphaned body descriptors;
- read-only snapshot views for `/workspace/input`;
- typed watch events for checkpoint consumers;
- chunk manifest for large checkpoint files.

## Failure Handling

The product contract should be explicit:

- object upload failure means no metadata publish;
- metadata publish failure returns staged object refs for explicit cleanup;
- metadata remove/replace success persists old body refs in the metadata GC
  queue and returns the old body descriptor to the caller;
- snapshot pins and watch cursors must protect history before GC.
