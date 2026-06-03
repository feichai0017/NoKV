<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV-FS Code Contract

This repository is the Rust NoKV-FS product line: an AI-training and
agent-workspace filesystem with Holt-backed metadata and S3-compatible object
storage.

NoKV-FS accepts breaking internal changes when they reduce ambiguity or remove
old compatibility paths. Do not add forwarding wrappers, deprecated aliases, or
dual execution paths unless the PR states the operational need and removal
condition.

## Package Boundaries

| Package | Owns | Must Not Do |
| --- | --- | --- |
| `nokv-fs/crates/model/` | Storage-neutral namespace model: mount ids, inode ids, dentry names, inode attrs, body descriptors, record families, and typed watch events. | Import layout, Holt, Raft, object-store clients, FUSE, protobuf, or service packages. |
| `nokv-fs/crates/layout/` | Holt-friendly ordered key layout and durable value codecs. | Own namespace semantics, execute commands, import Holt tree handles, Raft, object-store clients, FUSE, or wire packages. |
| `nokv-fs/crates/metastore/` | Metadata command contract between service semantics and concrete metadata stores. | Import Holt, Raft, object-store clients, FUSE, protobuf, or expose a raw KV API. |
| `nokv-fs/crates/holtstore/` | Holt implementation of `metastore`: family current trees, history records, predicate checks, and command dedupe. | Own filesystem semantics, import object-store clients, FUSE, Raft, protobuf, or expose raw Holt handles through the service boundary. |
| `nokv-fs/crates/object/` | Object-store boundary and local/S3-compatible backends for file bodies. | Own namespace metadata, import Holt, Raft, FUSE, protobuf, or implement metadata transactions. |
| `nokv-fs/crates/metad/` | In-process filesystem metadata service that compiles operations into `MetadataCommand`s and coordinates object publish with metadata publish. | Own Holt tree layout, bypass `metastore`, import Raft/FUSE/protobuf, or expose a generic object/KV API. |
| `nokv-fs/crates/client/` | Path-oriented Rust SDK over `metad`, including path resolution and user-facing artifact operations. | Own metadata layout, import Holt, bypass `metad`, expose object-store internals, or implement FUSE/kernel cache semantics. |
| `nokv-fs/crates/cli/` | Local developer/user entrypoint for the current embedded Holt path. | Reimplement service semantics, bypass `client`, own server/RPC behavior, or add provider-specific object semantics. |

Planned package owners:

| Package | Owns |
| --- | --- |
| `nokv-fs/crates/fuse/` | FUSE low-level frontend and kernel cache invalidation. |
| `nokv-fs/crates/server/` | Long-running metad process, config, health, and service boundary. |
| `nokv-fs/crates/raftgroup/` | Distributed metadata shard replication. |

## File Layout

Use responsibility-based file names. Avoid `utils.rs`, `helpers.rs`,
`common.rs`, and `misc.rs` unless the package is tiny and the file has one
clear responsibility.

Recommended package layout:

| File | Contents |
| --- | --- |
| `lib.rs` | Package contract and public exports. |
| `types.rs` | Core domain types and interfaces. |
| `options.rs` | Construction options and validation. |
| `errors.rs` | Package error enum and conversions. |
| `codec.rs` | Durable encoding and decoding. |
| `store.rs` | Authoritative store object. |
| `service.rs` | Service boundary. |
| `tests.rs` / `*_test.rs` | Focused behavior tests. |

## Rules

- Keep filesystem semantics above storage engine bindings.
- Keep object bytes out of metadata values except compact descriptors.
- Use inode/dentry as canonical truth; path indexes are derived acceleration.
- Use `MetadataCommand` predicates as the atomicity fence.
- Do not leak Holt internals into model, layout, metastore, client, or FUSE.
- Do not introduce provider-specific RustFS metadata semantics; RustFS uses the
  S3-compatible object backend.
- Prefer explicit invariants and local reasoning over manager-style wrappers.

## Validation

Before pushing substantial changes:

```bash
cargo fmt --manifest-path nokv-fs/Cargo.toml --all -- --check
cargo clippy --manifest-path nokv-fs/Cargo.toml --workspace --all-targets -- -D warnings
cargo test --manifest-path nokv-fs/Cargo.toml --workspace
git diff --check
```

Run the docs build when docs or navigation changes:

```bash
cd docs && npm run build
```
