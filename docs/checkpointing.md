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

## Current And Remaining Semantics

- atomic replace returns the old body descriptor;
- durable object GC queue stores old body refs from remove/replace;
- snapshot pins protect old body refs until retired;
- chunk manifests describe large checkpoint files;
- read-only snapshot FUSE mounts can expose a pinned subtree as `/`;
- service-level typed watch replay lets checkpoint consumers observe publish and
  replace events;

Remaining work:

- SDK/FUSE watch consumer integration;

## Failure Handling

The product contract should be explicit:

- object upload failure means no metadata publish;
- metadata publish failure returns staged object refs for explicit cleanup;
- metadata remove/replace success persists old body refs in the metadata GC
  queue and returns the old body descriptor to the caller;
- snapshot pins protect old body refs from object cleanup until retired;
- watch cursors must protect history before history GC.
