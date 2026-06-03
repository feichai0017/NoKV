<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# NoKV Code Contract

This document is the repository-level source of truth for code structure,
package boundaries, naming, errors, metrics, tests, DCO, and
distributed-safety review.

NoKV accepts breaking internal changes when they remove ambiguity or long-term
maintenance cost. Do not add compatibility shims, forwarding wrappers,
deprecated aliases, or dual execution paths unless the PR states the
operational need and removal condition.

## Pull Request Scope

- One PR changes one logical boundary.
- Do not mix behavior changes, broad refactors, benchmarks, generated-code
  rewrites, and documentation churn unless they are inseparable.
- A root/control-plane fix must not include unrelated fsmeta, raftstore, or
  benchmark changes.
- A performance PR must include benchmark evidence and must not hide semantic
  changes.
- A recovery or failover PR must include the crash/failover boundary it changes.
- Every non-merge commit must include a DCO `Signed-off-by` trailer.

Use:

```bash
git commit -s -m "fix: catch up root leader before writes"
```

## Package Boundaries

Package boundaries follow ownership of truth, not convenience.

| Package | Owns | Must Not Do |
| --- | --- | --- |
| `fsmeta/model/` | Storage-engine-neutral namespace model: mount, inode, dentry, session, quota, watch, snapshot, operation request/result types, and validation. | Import key layout, protobuf, coordinator, root, raftstore, Badger, Holt, or runtime packages. |
| `fsmeta/layout/` | Ordered fsmeta key layout, value codecs, key-family kinds, placement ranges, and operation key plans. | Own high-level namespace semantics, import protobuf, coordinator, root, raftstore, Badger, Holt, or runtime packages. |
| `fsmeta/backend/` | Storage-engine-neutral metadata command contract consumed by `fsmeta/exec`. | Import protobuf, concrete runtimes, coordinator, root, raftstore, Badger, Holt, migration, SST, or storage-engine diagnostics. |
| `fsmeta/observe/` | Runtime-neutral watch and snapshot observation surfaces. | Own namespace model objects, protobuf conversion, concrete runtimes, or backend clients. |
| `fsmeta/exec/` | Semantic compiler, executor, visible-read helpers, and runtime-neutral holder logic over `fsmeta/backend`. | Import protobuf, coordinator, root, raftstore, Badger, Holt, or concrete runtime packages. |
| `fsmeta/runtime/local/` | Badger-backed one-process fsmeta backend for demos, tests, and small deployments. | Become a generic KV database, import coordinator/root, or reinterpret compiler semantics outside the executor contract. |
| `fsmeta/runtime/raftstore/` | Distributed fsmeta runtime adapter: coordinator-backed route/TSO/ID/mount resolution, MetadataPlane calls, watch apply streams, and rooted snapshot publication. | Own root truth, own topology, import Holt, reinterpret fsmeta semantics, or bypass coordinator routing and rooted lifecycle state. |
| `fsmeta/server/`, `fsmeta/client/` | gRPC server/client boundary and protobuf conversion. | Put semantic execution or persistence logic in wire conversion code. |
| `meta/root/` | Rooted truth for topology, authority, lifecycle events, grants, and seals. | Import coordinator service/client packages or fsmeta execution packages. |
| `coordinator/` | Rebuildable serving view over root truth: routing, TSO, store discovery, scheduling, and root-event publish. | Become the source of rooted truth or own high-frequency fsmeta data. |
| `raftstore/` | Rust distributed data-plane target: OpenRaft isolation, mount-scoped replicated execution, raft log, Holt state-machine storage, snapshots, apply notifications, and the MetadataPlane/RaftAdmin/RaftTransport boundary. | Import Go fsmeta semantic packages, redesign public protobuf without a wire-compatibility plan, expose StoreKV/Percolator as a new mainline API, or revive deleted Go raftstore/txn/local/storage/experimental paths. |
| `pb/`, `*/wire/` | Proto definitions and conversion glue. | Leak protobuf structs into semantic cores when a domain type exists. |
| `metrics/` | Reusable metric value types. | Own subsystem state. |
| `cmd/` | Binary assembly, flags, env, and config wiring. | Contain core protocol, semantic, or storage logic. |
| `nokv-fs/crates/model/` | Rust-native NoKV-FS namespace model: mount ids, inode ids, dentry names, inode attrs, body descriptors, record families, and typed watch events. | Import key layout, Holt, Raft, object-store clients, FUSE, protobuf, or service/runtime packages. |
| `nokv-fs/crates/layout/` | Holt-friendly ordered key layout and durable value codecs for NoKV-FS metadata families. | Own namespace semantics, execute metadata commands, import Holt tree handles, Raft, object-store clients, FUSE, or wire packages. |
| `nokv-fs/crates/metastore/` | Rust-native metadata command contract between NoKV-FS service semantics and concrete metadata stores. | Import Holt, Raft, object-store clients, FUSE, protobuf, or expose a generic KV database API. |
| `nokv-fs/crates/object/` | Object-store boundary and demo local filesystem object backend for file bodies. | Own namespace metadata, import Holt, Raft, FUSE, protobuf, or implement metadata transactions. |
| `nokv-fs/crates/holtstore/` | Concrete Holt implementation of the `metastore` command contract, including family current trees, history records, predicate checks, and command dedupe. | Own filesystem namespace semantics, import object-store clients, FUSE, Raft, protobuf, or expose raw Holt handles through the service boundary. |
| `nokv-fs/crates/metad/` | In-process NoKV-FS metadata service that compiles namespace operations into `MetadataCommand`s and coordinates metadata publish with object-store writes. | Own Holt tree layout, bypass `metastore`, import Raft/FUSE/protobuf, or expose a generic object/KV database API. |

Deleted package trees are intentionally not compatibility surfaces:

- old Go `local/`;
- old Go `storage/`;
- old Go `txn/`;
- old Go `raftstore/`;
- `experimental/`.

If a lower layer needs a higher-layer operation, define a narrow interface at
the caller boundary instead of importing the higher layer.

Every new package must have a clear owner and a package comment or `doc.go`
stating:

- what authoritative state it owns, if any;
- which package may mutate that state;
- which package may observe it;
- which lower-layer packages it may import;
- which higher-layer packages must not import it.

## Shared Helpers

Before adding helper code, check the standard library and existing repository
packages.

Do not add catch-all helper packages by default. Reintroduce a top-level
`utils/` package only when all of these are true:

- the helper is domain-neutral;
- at least two non-test packages use it, or the second use is part of the same
  PR;
- it has no hidden global state;
- it does not import `errors`, `fsmeta`, `coordinator`, `meta/root`, or
  raftstore-related packages;
- it has focused tests.

Domain helpers belong in the domain package.

## File Layout

Use responsibility-based file names. Avoid `utils.go`, `helpers.go`,
`common.go`, and `misc.go` unless the package is tiny and the file has a single
clear purpose.

Recommended package layout:

| File | Contents |
| --- | --- |
| `doc.go` | Package responsibility, truth boundary, and major invariants. |
| `types.go` | Core domain types, small enums, and interfaces. |
| `options.go` | Options, defaults, and validation. |
| `errors.go` | Package sentinel errors and error helpers. |
| `metrics.go` | Runtime counters and record methods. |
| `stats.go` | Typed diagnostic snapshots and aggregation. |
| `store.go` | Authoritative in-memory state or the package's primary object. |
| `service.go` | RPC/service boundary and request/response conversion. |
| `client.go` | Remote client wrapper. |
| `recovery.go` | Recovery, replay, bootstrap, and restart behavior. |
| `encode.go` | Durable format encoding/decoding. |
| `validation.go` | Input, invariant, and state validation. |
| `*_test.go` | Tests for the behavior under test. |

When a file grows because it has multiple responsibilities, split by protocol
stage or data owner.

## Naming

- File names are lowercase snake_case.
- Exported types must describe domain responsibility.
- Avoid vague names such as `Manager`, `Handler`, `Processor`, `Data`, `Info`,
  and `Config` when a package has multiple authorities.
- Interfaces should name behavior required by the caller and stay near the
  consumer.
- Use `Options` for construction-time configuration and validate invalid
  combinations.
- Use `Stats` for live collectors and `StatsSnapshot` for read-only snapshots.
- Boolean fields should read naturally at call sites.

## Errors

- Sentinel errors belong in the package that owns the semantic condition.
- Do not introduce string matching for errors.
- Preserve root, freshness, durability, recovery, and GC ambiguity in typed
  errors instead of collapsing everything into `ErrInternal`.
- Wire conversion belongs at the service/client boundary.

## Metrics and Stats

- Metrics are owned by the subsystem whose behavior they describe.
- Stats snapshots must be typed at the owner boundary and converted to generic
  maps only at diagnostics/API edges.
- Do not put runtime diagnostics into lower-layer contracts unless every
  implementation can expose the same meaning.

## Tests

Match test level to risk:

- package tests for local invariants;
- contract tests for `fsmeta/backend` and public fsmeta semantics;
- recovery/fault tests for rooted truth and distributed data-plane changes;
- benchmark evidence for performance claims;
- Rust workspace tests for `raftstore` behavior.

Required local gates before pushing substantial changes:

```bash
go test -count=1 ./...
cargo test --manifest-path raftstore/Cargo.toml --workspace
git diff --check
```

Run `make lint` when package boundaries or generated code are touched.

## Generated Code

- Edit `.proto` files, not generated `.pb.go` files.
- Run `make proto-check` after protobuf changes.
- Generated files must not be manually patched.

## Documentation

When a PR moves or deletes packages, update:

- this code contract;
- `README.md`;
- `docs/guide/architecture.md`;
- any guide page that names the moved package;
- benchmark and Docker instructions if user-facing commands change.

Do not keep docs for deleted mainline modules unless the page is explicitly
marked as historical.
