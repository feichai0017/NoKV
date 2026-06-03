<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# Getting Started

NoKV-FS is currently a Rust workspace with an in-process metadata service,
Holt-backed metadata storage, and local/S3-compatible object backends.

## Prerequisites

- Rust stable
- Git
- Optional: Node.js 20+ for the documentation site
- Optional: RustFS, MinIO, or another S3-compatible endpoint for object-store
  integration tests

## Build And Test

```bash
make test
```

Equivalent cargo command:

```bash
cargo test --manifest-path nokv-fs/Cargo.toml --workspace
```

Run formatting and clippy:

```bash
make fmt
make lint
```

Build docs:

```bash
cd docs
npm install
npm run build
```

## S3-Compatible Object Backend

The object crate has a local filesystem backend for tests and an S3-compatible
backend for AWS S3, RustFS, MinIO, and compatible services.

To run the real S3/RustFS contract test:

```bash
export NOKV_FS_S3_BUCKET=nokv-fs-test
export NOKV_FS_S3_ENDPOINT=http://127.0.0.1:9000
export NOKV_FS_S3_REGION=auto
export NOKV_FS_S3_ACCESS_KEY_ID=minioadmin
export NOKV_FS_S3_SECRET_ACCESS_KEY=minioadmin

cargo test \
  --manifest-path nokv-fs/Cargo.toml \
  -p nokv-fs-object \
  s3_object_store_contract_from_env
```

RustFS is configured through the same S3-compatible backend. No RustFS-specific
metadata semantics are exposed to higher layers.

## What Works Today

- metadata model and Holt-friendly layout;
- metadata command contract;
- Holt-backed metadata store;
- local filesystem object backend;
- S3-compatible object backend;
- in-process `metad` operations for root bootstrap, directory create, artifact
  publish, lookup-plus, and readdir-plus.

## Next User-Facing Step

The next milestone is a small CLI:

```text
nokv-fs init
nokv-fs mkdir
nokv-fs put-artifact
nokv-fs ls
nokv-fs cat
```

That makes the current local Holt + S3/RustFS path usable before FUSE and
distributed metadata shards are introduced.
