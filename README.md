<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.svg" width="200" alt="NoKV" />

  <p>
    <strong>A Rust filesystem for AI training and agent workspaces.</strong>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/rust.yml?branch=main&label=ci" /></a>
    <a href="https://crates.io/crates/nokvfs-cli"><img alt="crates.io" src="https://img.shields.io/crates/v/nokvfs-cli?logo=rust" /></a>
    <a href="https://docs.rs/nokvfs-meta"><img alt="docs.rs" src="https://img.shields.io/docsrs/nokvfs-meta?logo=docs.rs" /></a>
    <img alt="Rust" src="https://img.shields.io/badge/rust-1.88%2B-f74c00?logo=rust&logoColor=white" />
  </p>

  <p>
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
    <a href="https://dbdb.io/db/nokv"><img alt="DBDB.io" src="https://img.shields.io/badge/DBDB.io-profiled-244A64" /></a>
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>

  <p>
    <a href="https://nokv.io/architecture">Docs</a> ·
    <a href="#-quick-start">Quick Start</a> ·
    <a href="#-headline-evidence">Benchmarks</a> ·
    <a href="https://github.com/feichai0017/NoKV/discussions">Discussions</a>
  </p>
</div>

<br/>

## What Is NoKV?

NoKV is a Rust filesystem for AI training and agent workspaces. It keeps
namespace metadata in its own path-native engine
([Holt](https://crates.io/crates/holt)) and stores file bodies as immutable
blocks in S3-compatible object storage (RustFS, MinIO, Ceph RGW, AWS S3).

```text
FUSE / SDK / CLI
  -> NoKV metadata service     (self-contained — no separate database to run)
  -> Holt inode/dentry metadata
  -> S3-compatible object store for file bodies
```

NoKV owns namespace truth, metadata transactions, snapshots, watches, and
object-reference GC; the object store owns byte durability and replication. The
metadata engine is built in, so you operate a filesystem — not a filesystem plus
a separate Redis/MySQL/TiKV cluster.

## Why NoKV

AI training and agent systems want a normal file interface while their real data
lives in object storage. The hard part isn't moving bytes — it's the metadata:

- **datasets** want fast directory scans and stable snapshot views;
- **checkpoints** want *atomic* publish — readers see a complete checkpoint or
  the previous one, never a half-written one, even across a crash;
- **artifacts** want body references, digests, and cleanup of failed staged
  uploads;
- **agent workspaces** want scoped namespace views and typed change events;
- **immutable, versioned data** means node-local caches never need invalidation.

The write model is *write-once publish* — exactly how datasets, checkpoints, and
artifacts are written.

## 📊 Headline evidence

Measured single-node (release build, full server + RPC + Holt path, local
RustFS). Distributed numbers need separate runs.

| Workload | Result |
| --- | --- |
| Metadata create (`mdtest`, 65k records) | **~127K ops/s** (single-writer, batched) |
| Same, **one directory of 65k entries** | **same throughput** — path-native ART doesn't degrade on big directories |
| Checkpoint publish (1 MiB blocks, concurrency 16) | **~1.1 GB/s** |
| Dataset read (16 KiB samples, concurrency 16) | **~3,000 samples/s** |
| Resident metadata | **~1.5 KB / file** |
| Atomic checkpoint | object bytes land first → metadata publishes atomically as a new generation → readers never see a half-written checkpoint |

## NoKV vs JuiceFS

Same proven skeleton — object-backed, metadata/data split, FUSE/SDK/CSI. The
difference is the metadata layer and the semantics:

| | JuiceFS | NoKV |
| --- | --- | --- |
| Metadata engine | rents a general DB (Redis / MySQL / TiKV) | **built-in, path-native** (Holt) — no separate DB to run |
| Atomic checkpoint publish | general POSIX | **first-class primitive** |
| Block model | slice + compaction (random-write) | immutable + new-generation (publish-oriented, trivially cacheable) |
| AI-native primitives | bolted on | snapshots, typed watch, GC floor — designed around the workload |
| POSIX completeness | full | partial (single-node prototype) |
| Maturity | production, billions of files | young — single-node, no HA yet |

NoKV trades general-POSIX completeness for a purpose-built engine and AI-native
semantics, in Object Mode.

## 🏗️ Architecture

```text
Application surface   nokvfs-fuse · nokvfs-client (SDK) · nokvfs-cli
Metadata layer        nokvfs-meta (MetadataCommand, schema) → Holt engine
                      nokvfs-server (long-running metad + framed RPC)
Body storage layer    nokvfs-object (S3-compatible immutable blocks)
```

For artifact/checkpoint publish, object bytes are uploaded **first**, then the
metadata commit publishes the dentry, inode projection, and body manifest
**atomically**. A crash between the two leaves orphan objects for GC — never a
corrupt namespace. See [Architecture](https://nokv.io/architecture).

## 🚦 Quick Start

```bash
# Build + test
cargo test --workspace
cargo build --release -p nokvfs-cli --bin nokv-fs

# A local S3 endpoint (RustFS) — bucket `nokv`, dev creds rustfsadmin/rustfsadmin
#   see docs/rustfs.md
rustfs server --address 127.0.0.1:9000 \
  --access-key rustfsadmin --secret-key rustfsadmin ./rustfs-data &

# Initialize, publish an artifact, read it back
nokv-fs --object-backend rustfs init
nokv-fs --object-backend rustfs put-artifact /runs/1/ckpt.bin ./ckpt.bin
nokv-fs --object-backend rustfs cat /runs/1/ckpt.bin > restored.bin

# Mount with FUSE (macOS needs macFUSE)
mkdir -p /tmp/nokv-mount
nokv-fs --object-backend rustfs mount /tmp/nokv-mount
```

## 🧩 Crates

| Crate | Role |
| --- | --- |
| [`nokvfs-types`](https://crates.io/crates/nokvfs-types) | Storage-neutral namespace model |
| [`nokvfs-protocol`](https://crates.io/crates/nokvfs-protocol) | Framed metadata RPC DTOs + binary codec |
| [`nokvfs-object`](https://crates.io/crates/nokvfs-object) | S3-compatible object body storage |
| [`nokvfs-meta`](https://crates.io/crates/nokvfs-meta) | Schema, `MetadataCommand`, Holt store, service core |
| [`nokvfs-client`](https://crates.io/crates/nokvfs-client) | Rust SDK over the metadata service |
| [`nokvfs-fuse`](https://crates.io/crates/nokvfs-fuse) | Low-level FUSE frontend |
| [`nokvfs-server`](https://crates.io/crates/nokvfs-server) | Long-running metad process + framed RPC |
| [`nokvfs-cli`](https://crates.io/crates/nokvfs-cli) | `nokv-fs` CLI binary |

## ✅ Current status

**Implemented:** read-write FUSE (lookup/getattr/setattr/readdir(plus)/create/
mkdir/symlink/rename/unlink/rmdir/read/write/truncate), Holt-backed inode/dentry
metadata, chunked immutable object data path, S3-compatible backend (RustFS
default), Rust SDK + `nokv-fs` CLI, long-running `nokvfs-server` with framed RPC,
read-only snapshot mounts + snapshot reads, typed watch replay + FUSE
invalidation, snapshot-aware object/history GC.

**Not yet:** distributed metadata replication / HA, FUSE over the network,
Python/fsspec + Kubernetes CSI, and full POSIX (hardlinks, xattrs, locks,
`statfs`, ACLs). NoKV is a usable single-node object-backed filesystem — not yet
a JuiceFS/3FS-class distributed filesystem. The next major step is a distributed
metadata layer with Holt as the shard-local state machine.

## 📚 Documentation

[Architecture](https://nokv.io/architecture) ·
[Product Design](https://nokv.io/product-design) ·
[Metadata Schema](https://nokv.io/metadata-schema) ·
[Object Layout](https://nokv.io/object-layout) ·
[AI Training](https://nokv.io/ai-training) ·
[Checkpointing](https://nokv.io/checkpointing) ·
[RustFS Backend](https://nokv.io/rustfs) ·
[Benchmarks](https://nokv.io/benchmarks)

## 📄 License

[Apache-2.0](./LICENSE)
