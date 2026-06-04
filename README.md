<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

<div align="center">
  <img src="./docs/public/img/logo.svg" width="200" alt="NoKV" />

  <p>
    <strong>Rust filesystem metadata for AI training and agent workspaces.</strong>
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
        <sub>Profiled by the CMU Database Group catalog as a Go-native log-structured key/value DBMS.</sub>
      </td>
    </tr>
  </table>

  <p>
    <a href="https://github.com/avelino/awesome-go#databases-implemented-in-go"><img alt="Mentioned in Awesome" src="https://awesome.re/mentioned-badge.svg" /></a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV"><img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" /></a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV"><img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" /></a>
    <a href="https://landscape.cncf.io/?group=projects-and-products&item=runtime--cloud-native-storage--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
    <a href="https://dbdb.io/db/nokv"><img alt="DBDB.io" src="https://img.shields.io/badge/DBDB.io-profiled-244A64" /></a>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" /></a>
    <a href="https://codecov.io/gh/feichai0017/NoKV"><img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" /></a>
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>
</div>

<br/>

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
implementation is a basic usable local filesystem surface: it can mount with
FUSE, create and list directories, write and read files through object-backed
chunk manifests, rename or remove namespace entries, publish artifacts
atomically, serve remote metadata clients, and expose read-only snapshot views.

It is not yet a production distributed DFS. Multi-node metadata replication,
remote FUSE over the metadata server, and the remaining strict POSIX corners
are still future work.

The current workspace contains:

```text
crates/
  nokvfs-types   # mount, inode, dentry, body descriptor, watch types
  nokvfs-protocol # metadata RPC wire DTOs
  nokvfs-meta    # schema, MetadataCommand, Holt store, in-process metad
  nokvfs-object  # S3-compatible object backend, including RustFS
  nokvfs-client  # Rust SDK
  nokvfs-fuse    # low-level FUSE frontend
  nokvfs-server  # long-running local metad process and health/control plane
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

- local Holt-backed metadata store with inode/dentry canonical truth,
  family trees, dentry projection, and snapshot-aware history;
- S3-compatible object backend for AWS S3, RustFS, MinIO, and compatible
  services;
- in-memory object backend for package tests;
- chunked object data plane with 64 MiB chunks, 4 MiB immutable object blocks,
  range read plans, and metadata-published body manifests;
- metadata commands with predicates, mutations, family trees, command dedupe,
  and dentry projection;
- staged object references and explicit cleanup helpers for failed artifact
  publish paths;
- durable pending-object GC queue and explicit cleanup API for removed or
  replaced artifact bodies;
- service-level background object GC worker, enabled by the live FUSE mount;
- durable snapshot pins, snapshot-version artifact reads, and snapshot-protected
  object cleanup;
- snapshot-aware metadata history cleanup and background history GC worker;
- read-only FUSE snapshot mounts rooted at a snapshot subtree;
- durable typed watch replay for namespace and artifact publication events;
- live FUSE mount invalidates kernel entry/inode caches from typed watch replay;
- long-running local `nokvfs-server` process with health, stats, and manual
  object/history GC endpoints;
- framed metadata RPC v3 on `nokvfs-server` for bootstrap, lookup,
  readdir-plus, create, remove, rename, snapshot, artifact publish, and
  snapshot retirement, with pipelined request ids, out-of-order responses,
  bounded server workers, and ordered non-atomic batch requests for remote SDK
  throughput; HTTP `/rpc` remains available for debug requests;
- remote Rust metadata client for path and inode namespace operations over the
  framed RPC;
- remote Rust file client that uploads object blocks directly, commits metadata
  through `metad`, fetches body read plans, and reads object ranges directly
  from the configured object store;
- basic root bootstrap, directory create, artifact publish, lookup-plus,
  readdir-plus, remove, rmdir, rename, and rename-replace in the in-process
  service;
- path-oriented Rust SDK for mkdir, artifact publish, lookup, list, cat,
  remove, rmdir, rename, and rename-replace;
- low-level FUSE frontend for lookup, getattr, readdir, open, range read,
  snapshot read mounts, mkdir, create, dirty-range buffered write sessions,
  flush/fsync/release publish, unlink, rmdir, and rename-replace;
- `nokv-fs` local CLI: init, mkdir, put-artifact, ls, cat, rm, rmdir, rename,
  rename-replace, mount, mount-snapshot, snapshot, cat-snapshot,
  retire-snapshot, serve, and manual GC cleanup.

Not implemented yet:

- remote FUSE over the metadata server;
- full POSIX mmap, lock, truncate, and strict fsync semantics;
- multi-node distributed metadata shards.

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

The live mount starts background object and metadata-history GC workers. Use
`--object-gc-interval-ms`, `--object-gc-limit`, `--history-gc-interval-ms`,
and `--history-gc-limit` before `mount` to tune them.

To run the local metadata process without mounting FUSE:

```bash
cargo run -p nokvfs-cli --bin nokv-fs -- serve
curl http://127.0.0.1:7777/healthz
curl http://127.0.0.1:7777/stats
curl -X POST http://127.0.0.1:7777/rpc \
  -H 'content-type: application/json' \
  -d '{"op":"read_dir_plus","parent":1}'
curl -X POST http://127.0.0.1:7777/gc
```

Use `--server-bind ADDR` before `serve` to change the health/control address.
The Rust SDK uses the server's framed metadata RPC on the same port; HTTP
`/rpc` is kept for curl/debug visibility. `/stats` includes object counters,
GC worker state, and metadata-store write attribution counters for current,
history, watch, and dedupe writes.

To mount a read-only snapshot view:

```bash
SNAPSHOT_ID=$(cargo run -q -p nokvfs-cli --bin nokv-fs -- snapshot /runs \
  | sed -n 's/.* id=\([0-9][0-9]*\) .*/\1/p')
mkdir -p /tmp/nokv-fs-snapshot
cargo run -p nokvfs-cli --bin nokv-fs -- \
  mount-snapshot "$SNAPSHOT_ID" /tmp/nokv-fs-snapshot
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
