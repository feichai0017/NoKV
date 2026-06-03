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
nokv-fs/
  crates/model        # mount, inode, dentry, body descriptor, watch types
  crates/layout       # Holt-friendly keys and durable value codecs
  crates/metastore    # storage-neutral MetadataCommand contract
  crates/holtstore    # Holt-backed metadata store
  crates/object       # local filesystem and S3-compatible object backends
  crates/metad        # in-process metadata service
  crates/client       # path-oriented Rust SDK over metad
  crates/cli          # minimal local CLI
```

Implemented today:

- local Holt-backed metadata store;
- local filesystem object backend;
- S3-compatible object backend for AWS S3, RustFS, MinIO, and compatible
  services;
- metadata commands with predicates, mutations, family trees, command dedupe,
  and dentry projection;
- basic root bootstrap, directory create, artifact publish, lookup-plus, and
  readdir-plus in the in-process service;
- path-oriented Rust SDK for mkdir, artifact publish, lookup, list, and cat;
- minimal local CLI: init, mkdir, put-artifact, ls, and cat.

Not implemented yet:

- long-running metad server;
- FUSE client;
- durable inode/version allocator;
- remove/rmdir/rename-replace;
- watch replay, snapshot retention, and object GC worker;
- distributed metadata shards.

## Quick Check

```bash
make test
```

This runs:

```bash
cargo test --manifest-path nokv-fs/Cargo.toml --workspace
```

For a real S3-compatible object backend contract test, set:

```bash
export NOKV_FS_S3_BUCKET=nokv-fs-test
export NOKV_FS_S3_ENDPOINT=http://127.0.0.1:9000
export NOKV_FS_S3_REGION=auto
export NOKV_FS_S3_ACCESS_KEY_ID=minioadmin
export NOKV_FS_S3_SECRET_ACCESS_KEY=minioadmin
cargo test --manifest-path nokv-fs/Cargo.toml -p nokv-fs-object s3_object_store_contract_from_env
```

RustFS uses the same S3-compatible backend; it should be configured through
the endpoint and credentials, not through a RustFS-specific code path.

## Local CLI Smoke

```bash
cargo run --manifest-path nokv-fs/Cargo.toml -p nokv-fs-cli -- init
cargo run --manifest-path nokv-fs/Cargo.toml -p nokv-fs-cli -- mkdir /runs
printf '{"step":1}' > /tmp/checkpoint.json
cargo run --manifest-path nokv-fs/Cargo.toml -p nokv-fs-cli -- \
  put-artifact /runs/checkpoint.json /tmp/checkpoint.json
cargo run --manifest-path nokv-fs/Cargo.toml -p nokv-fs-cli -- ls /runs
cargo run --manifest-path nokv-fs/Cargo.toml -p nokv-fs-cli -- cat /runs/checkpoint.json
```

## Documentation

- [Getting Started](docs/guide/getting_started.md)
- [Architecture](docs/guide/architecture.md)
- [NoKV-FS Design](docs/guide/nokv_fs_design.md)
- [Code Contract](docs/guide/development/code_contract.md)

## License

[Apache-2.0](./LICENSE)
