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
| `crates/nokvfs-types/` | Storage-neutral namespace model: mount ids, inode ids, dentry names, inode attrs, body descriptors, record families, and typed watch events. | Import metadata layout, Holt, Raft, object-store clients, FUSE, protobuf, or service packages. |
| `crates/nokvfs-meta/` | Metadata schema, Holt-friendly layout, `MetadataCommand`, Holt-backed metastore, in-process metadata service, snapshot retention pins, object-reference GC queue policy, and service-level object GC worker. | Own provider-specific object behavior, FUSE/kernel cache policy, Python bindings, CSI behavior, or remote protocol compatibility shims. |
| `crates/nokvfs-object/` | Object-store boundary, S3-compatible backend, and in-memory test object store for file bodies. | Own namespace metadata, import Holt/FUSE/protobuf, implement metadata transactions, or expose filesystem-directory object storage as a product backend. |
| `crates/nokvfs-client/` | Path-oriented Rust SDK over the in-process metadata service. | Own metadata layout, bypass `nokvfs-meta`, expose object-store internals, implement FUSE/kernel cache semantics, or depend on `nokvfs-fuse`. |
| `crates/nokvfs-fuse/` | FUSE low-level frontend, inode mapping, kernel-facing attr conversion, range reads, and close-to-open buffered writes through `nokvfs-meta`. | Resolve paths through the SDK, own metadata layout, import Holt directly, or implement object-provider-specific behavior. |
| `crates/nokvfs-cli/` | `nokv-fs` CLI binary, command parsing, local service startup, and CLI wiring across client, FUSE, metadata, and object config. | Own metadata semantics, durable layout, object backend internals, or FUSE filesystem implementation. |
| `bench/` | System workload harnesses for metadata smoke, MLPerf Storage/DLIO-style generated training reads, checkpoint publish/read, and demo dataset shapes. | Own product APIs, metadata layout, object backend implementation, FUSE/kernel cache policy, or hidden benchmark-only behavior in product crates. |

Planned package owners:

| Package | Owns |
| --- | --- |
| `crates/nokvfs-server/` | Long-running metadata service, config, health, and remote service boundary. |
| `crates/nokvfs-csi/` | Kubernetes CSI integration and mount lifecycle. |
| `crates/nokvfs-python/` | Python SDK/fsspec bindings for training workflows. |

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
- Do not leak Holt internals into types, object, client, or FUSE.
- Do not introduce provider-specific RustFS metadata semantics; RustFS uses the
  S3-compatible object backend.
- Prefer explicit invariants and local reasoning over manager-style wrappers.

## Validation

Before pushing substantial changes:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
git diff --check
```

Run the docs build when docs or navigation changes:

```bash
cd docs && npm run build
```
