<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Object Layout

NoKV-FS stores file bodies outside the metadata service. Metadata stores compact
body descriptors; durability of bytes is delegated to the configured object
store.

## Body Descriptor

```text
producer
digest_uri
size
content_type
object_ref
generation
```

`object_ref` is provider-neutral. The current local CLI writes artifact bodies
under `artifacts/<path>`. S3-compatible backends use the same object boundary
for AWS S3, RustFS, MinIO, and Ceph RGW.

Use `--object-backend rustfs` or `--object-backend s3` to store bodies outside
the local filesystem. See [RustFS Backend](./rustfs.md) for the local RustFS
shape.

## Publish Rule

Artifact publish is staged:

```text
upload object bytes
  -> commit inode + dentry projection + chunk manifest descriptor
  -> expose namespace entry
```

If object upload succeeds and metadata publish fails, the object is staged but
not reachable from the namespace. A future GC worker should clean staged or
orphaned objects after a retention window.

## Large File Direction

The first implementation stores one artifact as one object. The target training
layout is chunked:

```text
inode -> ordered chunk descriptors -> S3-compatible objects
```

This preserves parallel range reads for large datasets and checkpoints without
moving file bytes through the metadata service.
