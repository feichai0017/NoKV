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
    <a href="https://crates.io/crates/nokv"><img alt="crates.io" src="https://img.shields.io/crates/v/nokv?logo=rust" /></a>
    <a href="https://docs.rs/nokv"><img alt="docs.rs" src="https://img.shields.io/docsrs/nokv?logo=docs.rs" /></a>
    <img alt="Rust Version" src="https://img.shields.io/badge/rust-1.88%2B-f74c00?logo=rust&logoColor=white" />
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

  <h3>Recognized In The AI-Native Storage Ecosystem</h3>

  <table>
    <tr>
      <td align="center" width="360">
        <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv">
          <img src="./docs/public/img/recognition/cncf.svg" width="128" alt="Cloud Native Computing Foundation" />
        </a>
        <br />
        <strong>Linux Foundation CNCF Landscape</strong>
        <br />
        <sub>Listed in AI Native Infra / Storage and Cloud Native Storage.</sub>
      </td>
      <td align="center" width="360">
        <a href="https://dbdb.io/db/nokv">
          <img src="./docs/public/img/recognition/dbdb.svg" width="128" alt="DBDB.io Database of Databases" />
        </a>
        <br />
        <strong>DBDB.io Database of Databases</strong>
        <br />
        <sub>Historical database profile; current NoKV is the Rust filesystem product line.</sub>
      </td>
    </tr>
  </table>
</div>

<br/>

## What Is NoKV?

NoKV is a Rust filesystem for AI training and agent workspaces. It keeps
namespace metadata in its own path-native engine
([Holt](https://crates.io/crates/holt)) and stores file bodies as immutable
blocks in S3-compatible object storage such as RustFS, MinIO, Ceph RGW, or AWS
S3.

```text
FUSE / SDK / CLI
  -> NoKV metadata service     (self-contained; no separate metadata DB to run)
  -> Holt inode/dentry metadata
  -> S3-compatible object store for file bodies
```

NoKV owns namespace truth, metadata transactions, snapshots, watches, and
object-reference GC. The object store owns byte durability and replication. The
metadata engine is built in, so local deployments operate a filesystem rather
than a filesystem plus a separate Redis, MySQL, or TiKV cluster.

## Why NoKV

AI training and agent systems want a normal file interface while their real data
lives in object storage. The hard part is not just moving bytes; it is metadata:

- **datasets** want fast directory scans and stable snapshot views;
- **checkpoints** want atomic publish, where readers see a complete checkpoint
  or the previous one, never a half-written one;
- **artifacts** want body references, digests, and cleanup of failed staged
  uploads;
- **agent workspaces** want scoped namespace views and typed change events;
- **immutable, versioned data** lets node-local caches avoid invalidating object
  bytes after publish.

The primary write model is write-once publish, matching how datasets,
checkpoints, and artifacts are commonly written.

## 📊 Headline Evidence

Measured local engineering baselines, not official MLPerf results. Single-node
service numbers are release builds through the NoKV server and Holt metadata
path. FUSE comparison numbers depend on kernel/FUSE, object backend, cache
settings, and workload shape.

| Workload | Result |
| --- | --- |
| Metadata create (`mdtest`, 65k records) | **~127K ops/s** (single-writer, batched service path) |
| Same, one directory of 65k entries | Same order of throughput; path-native ART does not degrade on large directories |
| Checkpoint publish (1 MiB blocks, concurrency 16) | **~1.1 GiB/s** in the service/object benchmark |
| Dataset read (16 KiB samples, concurrency 16) | **~3,000 samples/s** in the service/object benchmark |
| Resident metadata | **~1.5 KiB / file** in the measured shape |
| Atomic checkpoint | Object bytes land first; metadata publishes a new generation atomically |

Same-machine FUSE-vs-FUSE smoke against one RustFS endpoint currently shows
NoKV behind JuiceFS on the end-to-end mounted path. That gap is expected to come
from FUSE/RPC fixed costs and data-plane cache/writeback maturity, not from the
Holt metadata engine alone.

## NoKV vs JuiceFS

NoKV follows the same high-level separation used by systems like JuiceFS and
3FS: metadata is separate from file body storage. The difference is that NoKV
ships its metadata engine as part of the filesystem and optimizes for
AI-training publish/read patterns first.

| | JuiceFS | NoKV |
| --- | --- | --- |
| Metadata engine | External DB such as Redis, MySQL, or TiKV | Built-in, path-native Holt engine |
| Atomic checkpoint publish | POSIX rename/write semantics over the metadata engine | First-class publish-by-generation primitive |
| Block model | Slice/block model supporting broad POSIX behavior | Immutable object blocks plus new-generation manifests |
| AI-native primitives | Layered on top of the filesystem | Snapshots, typed watch, body descriptors, and GC floors are core metadata concepts |
| POSIX completeness | Mature production filesystem | P0 subset implemented; still hardening compatibility gates |
| Maturity | Production, large deployments | Young Rust implementation; local mode is usable, production HA is still in progress |

NoKV is currently a usable object-backed filesystem with a local Holt metadata
mode and an OpenRaft-backed metadata server path. It is not yet a JuiceFS/3FS
class distributed filesystem.

## 🏗️ Architecture

```text
crates/
  nokv-types     storage-neutral namespace model types
  nokv-protocol  framed metadata RPC DTOs and binary codec
  nokv-meta      schema, MetadataCommand, Holt store, service core
  nokv-object    S3-compatible object body storage helpers
  nokv-client    Rust SDK over metadata service and object backend
  nokv-fuse      low-level FUSE frontend
  nokv-server    long-running metad process and framed RPC service
  nokv-cluster   OpenRaft metadata replication boundary
  nokv           CLI binary

bench/             system workload benchmark harness
docs/              product, architecture, layout, RustFS, and benchmark docs
examples/          PyTorch and Kubernetes examples
```

For artifact and checkpoint publish, object bytes are uploaded first, then the
metadata commit publishes the dentry, inode projection, and body manifest
atomically. A crash between the two leaves orphan objects for GC, never a
corrupt namespace. See [Architecture](https://nokv.io/architecture).

## 🚦 Quick Start

Build and test:

```bash
cargo test --workspace
cargo build --release -p nokv --bin nokv
```

Start a local RustFS-compatible S3 endpoint:

```bash
rustfs server --address 127.0.0.1:9000 ./rustfs-data
```

By default NoKV expects bucket `nokv` at `http://127.0.0.1:9000` with
development credentials `rustfsadmin` / `rustfsadmin`. See
[docs/rustfs.md](docs/rustfs.md) for setup commands.

Initialize local metadata and start a metadata server:

```bash
cargo run --release -p nokv --bin nokv -- init
cargo run --release -p nokv --bin nokv -- serve
```

Publish and read an artifact:

```bash
cargo run --release -p nokv --bin nokv -- \
  put-artifact /runs/1/checkpoint.bin ./checkpoint.bin

cargo run --release -p nokv --bin nokv -- \
  cat /runs/1/checkpoint.bin > restored.bin
```

Mount with FUSE:

```bash
mkdir -p /tmp/nokv-mount

cargo run --release -p nokv --bin nokv -- \
  mount /tmp/nokv-mount
```

On macOS this requires macFUSE. NoKV passes `noappledouble` and `noapplexattr`
mount options to avoid Finder/resource-fork AppleDouble sidecars; user xattr
roundtrip is covered by the FUSE smoke test.

## 🧩 Crates

| Crate | Role |
| --- | --- |
| [`nokv-types`](https://crates.io/crates/nokv-types) | Storage-neutral namespace model |
| [`nokv-protocol`](https://crates.io/crates/nokv-protocol) | Framed metadata RPC DTOs and binary codec |
| [`nokv-object`](https://crates.io/crates/nokv-object) | S3-compatible object body storage |
| [`nokv-meta`](https://crates.io/crates/nokv-meta) | Schema, `MetadataCommand`, Holt store, service core |
| [`nokv-client`](https://crates.io/crates/nokv-client) | Rust SDK over the metadata service |
| [`nokv-fuse`](https://crates.io/crates/nokv-fuse) | Low-level FUSE frontend |
| [`nokv-server`](https://crates.io/crates/nokv-server) | Long-running metad process and framed RPC |
| [`nokv-cluster`](https://crates.io/crates/nokv-cluster) | OpenRaft metadata replication boundary |
| [`nokv`](https://crates.io/crates/nokv) | `nokv` CLI binary |

## ✅ Current Status

Implemented today:

- low-level FUSE frontend for lookup, getattr, readdir, readdirplus, create,
  mkdir, symlink/readlink, rename, unlink, rmdir, read, write, flush, release,
  fsync, setattr/truncate, hardlink, xattr, advisory locks, special files,
  `statfs`, `lseek`, `fallocate`, and `copy_file_range`;
- Holt-backed local metadata service with inode/dentry canonical metadata,
  dentry projection, command predicates, command dedupe, and history records;
- chunked object data path where file bodies are split into immutable object
  blocks and published by metadata manifest;
- S3-compatible object backend, with RustFS as the local development default;
- Rust SDK and `nokv` CLI for namespace operations, artifact publish,
  metadata server access, and object range reads;
- long-running `nokv-server` with health, readiness, stats, manual GC, and
  framed binary metadata RPC;
- OpenRaft metadata group support with explicit voter membership, persistent
  Raft log storage, follower replay, and a local 3-voter smoke harness;
- read-only snapshot mounts, snapshot-version reads, typed watch replay, and
  FUSE cache invalidation from watch events;
- pending-object GC and metadata history GC tied to snapshot retention.

Not implemented yet:

- production-grade distributed metadata operations such as managed membership,
  checkpoint archive, learner read scaling, and multi-machine deployment;
- Python/fsspec and Kubernetes CSI packages;
- full POSIX hardening such as ACL enforcement, broad external compatibility
  gate coverage, and mature multi-client cache coherence.

## Benchmarks

The root `bench/` package contains all benchmark entry points. System workload
runs use `nokv-bench`:

```bash
cargo run --release -p nokv-bench --bin nokv-bench -- \
  --profile smoke \
  --workload all
```

- `mdtest-easy` and `mdtest-hard` metadata smoke workloads;
- `metadata-negative-lookup`, `artifact-index-lookup`, and
  `metadata-concurrent-read` Holt metadata read-path workloads;
- `checkpoint-publish` object-backed checkpoint publish/read;
- `training-read` dataset-shaped object reads;
- `mlperf-dlio` generated MLPerf Storage/DLIO-style training and checkpoint
  shape;
- `metadata-ha-smoke` and `metadata-ha-fault-smoke` OpenRaft metadata HA smoke
  workloads.

Agent-interface experiments live under `bench/agent-interface/` and run through
the same package:

```bash
cargo run --release -p nokv-bench --bin yanex-agent-bench -- list-tasks
```

For the fast AI-training product gate, run:

```bash
scripts/run-ai-training-smoke.sh
```

The default gate covers Holt metadata read concurrency, checkpoint publish,
DLIO-style object reads/writes, OpenRaft HA, and OpenRaft fault catch-up. Most
benchmark workloads are still single-node service runs; HA workloads report
OpenRaft state metrics separately. Training-cluster claims need separate runs
that report replication, cache, object-store, and durability settings.

Run `scripts/run-ai-training-smoke.sh fuse-smoke` when the local machine has a
working FUSE installation and you want the mounted POSIX smoke in the same
workflow. Run `scripts/run-ai-training-smoke.sh metadata-raft-smoke` when you
want the explicit 3-voter OpenRaft process smoke in the same entrypoint.

The local OpenRaft metadata smoke starts RustFS plus three metadata voters and
verifies that a leader-published artifact is readable through follower voters:

```bash
scripts/run-metadata-raft-smoke.sh
```

## 📚 Documentation

- [Architecture](docs/architecture.md)
- [Product Design](docs/product-design.md)
- [Metadata Schema](docs/metadata-schema.md)
- [Object Layout](docs/object-layout.md)
- [AI Training](docs/ai-training.md)
- [Checkpointing](docs/checkpointing.md)
- [RustFS Backend](docs/rustfs.md)
- [Benchmarks](docs/benchmarks.md)

## 📄 License

Apache-2.0. See [LICENSE](LICENSE).
