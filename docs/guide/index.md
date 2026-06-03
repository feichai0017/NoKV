<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Documentation

NoKV-FS is an open-source Rust filesystem for AI training and agent
workspaces. It stores metadata in Holt and file bodies in S3-compatible object
storage such as AWS S3, RustFS, MinIO, or Ceph RGW.

The repository is now centered on the Rust `nokv-fs` workspace:

```text
nokv-fs/
  crates/model
  crates/layout
  crates/metastore
  crates/holtstore
  crates/object
  crates/metad
```

## Start Here

| Topic | Doc |
|---|---|
| Build and run the current workspace | [Getting Started](./getting_started) |
| Layering and package ownership | [Architecture](./architecture) |
| Target product design | [NoKV-FS Design](./nokv_fs_design) |
| Code ownership rules | [Code Contract](./development/code_contract) |
| PR review checklist | [PR Review Checklist](./development/pr_review_checklist) |

## Current Shape

```text
AI training / agent workspace client
  -> NoKV-FS metad
  -> Holt metadata store
  -> S3-compatible object store for file bodies
```

The first usable product path is an artifact/checkpoint workspace API. FUSE and
distributed metadata shards are planned after the local service semantics,
object backend, and durable allocator are stable.
