<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV-FS

NoKV-FS is an open-source Rust filesystem for AI training and agent
workspaces. It keeps filesystem metadata in Holt and stores file bodies in
S3-compatible object storage such as AWS S3, RustFS, MinIO, or Ceph RGW.

The product target is a real file interface with metadata optimized for:

- AI training datasets with many files, manifests, and repeated directory
  scans;
- checkpoint and artifact publication where object bytes are uploaded first
  and metadata publish must be atomic;
- agent workspaces with scoped views, read-only snapshots, and typed watch
  events;
- DFS-style metadata services that want inode/dentry semantics without owning
  the object body durability layer.

NoKV-FS is not a general-purpose distributed KV database and does not implement
object-store durability itself. Object bytes live in an external body store.
NoKV-FS owns namespace truth, metadata transactions, inode/dentry records,
body descriptors, watch/snapshot state, and later distributed metadata shards.

## Current Status

The repository has been cut down to the Rust NoKV-FS line. The current
workspace contains the first local metadata slice:

```text
crates/
  nokvfs-types   # mount, inode, dentry, body descriptor, watch types
  nokvfs-meta    # schema, MetadataCommand, Holt store, in-process metad
  nokvfs-object  # S3-compatible object backend, including RustFS
  nokvfs-client  # Rust SDK
  nokvfs-fuse    # low-level FUSE frontend
  nokvfs-cli     # nokv-fs CLI binary

bench/           # system workload benchmark harness

docs/
  architecture.md
  product-design.md
  metadata-schema.md
  object-layout.md
  ai-training.md
  checkpointing.md

examples/
  pytorch/
  k8s/
```

Implemented today:

- local Holt-backed metadata store;
- S3-compatible object backend for AWS S3, RustFS, MinIO, and compatible
  services;
- in-memory object backend for package tests;
- metadata commands with predicates, mutations, family trees, command dedupe,
  and dentry projection;
- staged object references and explicit cleanup helpers for failed artifact
  publish paths;
- durable pending-object GC queue and explicit cleanup API for removed or
  replaced artifact bodies;
- basic root bootstrap, directory create, artifact publish, lookup-plus,
  readdir-plus, remove, rmdir, rename, and rename-replace in the in-process
  service;
- path-oriented Rust SDK for mkdir, artifact publish, lookup, list, cat,
  remove, rmdir, rename, and rename-replace;
- low-level FUSE frontend for lookup, getattr, readdir, open, range read, mkdir,
  create, buffered write, flush/fsync/release publish, unlink, rmdir, and
  rename-replace;
- `nokv-fs` local CLI: init, mkdir, put-artifact, ls, cat, rm, rmdir, rename,
  rename-replace, mount, and manual object GC cleanup.

Not implemented yet:

- long-running metad server;
- full POSIX random-write/truncate semantics;
- watch replay, snapshot retention, and background object GC worker;
- distributed metadata shards.

## Quick Check

```bash
make test
```

This runs:

```bash
cargo test --workspace
```

For a real S3-compatible object backend contract test, set:

```bash
export NOKV_FS_S3_BUCKET=nokv-fs-test
export NOKV_FS_S3_ENDPOINT=http://127.0.0.1:9000
export NOKV_FS_S3_REGION=auto
export NOKV_FS_S3_ACCESS_KEY_ID=minioadmin
export NOKV_FS_S3_SECRET_ACCESS_KEY=minioadmin
cargo test -p nokvfs-object s3_object_store_contract_from_env
```

RustFS uses the same S3-compatible backend; it should be configured through
the endpoint and credentials, not through a RustFS-specific code path.

## Local CLI Smoke

```bash
cargo run -p nokvfs-cli --bin nokv-fs -- init
cargo run -p nokvfs-cli --bin nokv-fs -- mkdir /runs
printf '{"step":1}' > /tmp/checkpoint.json
cargo run -p nokvfs-cli --bin nokv-fs -- \
  put-artifact /runs/checkpoint.json /tmp/checkpoint.json
cargo run -p nokvfs-cli --bin nokv-fs -- ls /runs
cargo run -p nokvfs-cli --bin nokv-fs -- cat /runs/checkpoint.json
```

To mount the current FUSE frontend:

```bash
mkdir -p /tmp/nokv-fs-mount
cargo run -p nokvfs-cli --bin nokv-fs -- mount /tmp/nokv-fs-mount
```

Linux builds use fuser's pure-Rust mount path. macOS builds require macFUSE
and its `pkg-config` metadata so the CLI can perform a real FUSE mount.

## Documentation

- [Architecture](docs/architecture.md)
- [Product Design](docs/product-design.md)
- [Metadata Schema](docs/metadata-schema.md)
- [Object Layout](docs/object-layout.md)
- [AI Training](docs/ai-training.md)
- [Checkpointing](docs/checkpointing.md)
- [Code Contract](docs/development/code_contract.md)

## License

[Apache-2.0](./LICENSE)
