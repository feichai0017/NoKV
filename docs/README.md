# NoKV — Documentation

<div align="center">
  <img src="assets/logo.svg" width="160" alt="NoKV" />

  <p><strong>An open-source namespace metadata substrate for distributed filesystems and object storage — with formally verified authority handoff.</strong></p>

  <p>
    <em>Native fsmeta primitives · Eunomia handoff protocol (TLA+ verified) · Own LSM · Own Raft · Own MVCC · Own control plane</em>
  </p>

  <p>
    <a href="https://github.com/feichai0017/NoKV/actions"><img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" /></a>
    <a href="https://codecov.io/gh/feichai0017/NoKV"><img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" /></a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV"><img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" /></a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV"><img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" /></a>
    <a href="https://github.com/avelino/awesome-go#databases-implemented-in-go"><img alt="Mentioned in Awesome" src="https://awesome.re/mentioned-badge.svg" /></a>
    <a href="https://landscape.cncf.io/?item=app-definition-and-development--database--nokv"><img alt="CNCF Landscape" src="https://img.shields.io/badge/CNCF%20Landscape-listed-5699C6?logo=cncf&logoColor=white" /></a>
  </p>

  <p>
    <img alt="Go Version" src="https://img.shields.io/badge/go-1.26%2B-00ADD8?logo=go&logoColor=white" />
    <img alt="License" src="https://img.shields.io/badge/license-Apache--2.0-yellow" />
    <a href="https://deepwiki.com/feichai0017/NoKV"><img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" /></a>
  </p>
</div>

NoKV is the open-source counterpart of the **"stateless schema layer + transactional KV"** pattern that powers Meta Tectonic (over ZippyDB), Google Colossus (over Bigtable), and DeepSeek 3FS (over FoundationDB). The headline service is **`fsmeta`**, a namespace metadata API for distributed filesystems / object storage / AI dataset metadata; the headline protocol is **`Eunomia`**, a TLA+-verified service-level correctness class for authority handoff continuation legality.

The interesting part isn't the feature list. The interesting part is that **layer separation is enforced in code**: Layer 1 (`fsmeta`) doesn't import the raftstore client; Layer 2 (`meta/root` + `coordinator` + `raftstore` + `percolator`) doesn't know about fsmeta concepts; Layer 3 (`engine`) doesn't know there's a namespace.

> This site is the **technical docs hub**. For the project landing page, headline benchmarks, and the `Why NoKV vs X?` matrix, see the [root README](../README.md).

---

## 🧭 Three Audiences, One Substrate

|  | DFS frontend | Object storage namespace | AI dataset metadata |
|---|---|---|---|
| **Consumer shape** | FUSE / NFS / SMB driver | S3-compatible HTTP gateway | training pipeline / scheduler |
| **fsmeta primitives used** | Mount, Create, ReadDirPlus, RenameSubtree, Unlink, Link, WatchSubtree | Mount = bucket; Create = PUT-IfNotExist; LIST = ReadDirPlus; atomic prefix rename = RenameSubtree; bucket events = WatchSubtree; bucket version = SnapshotSubtree | Mount = dataset; SnapshotSubtree = dataset version; WatchSubtree = checkpoint storm notification; ReadDirPlus = batch metadata fetch |
| **Eunomia coverage** | Subtree authority handoff during rename / migration | Bucket ownership transfer | Dataset version handoff |
| **Comparable industrial pattern** | Tectonic / Colossus / 3FS / HopsFS | Tectonic / Colossus over object layer | Mooncake / Quiver / 3FS dataset layer |

All three consume the **same** rooted truth in `meta/root`, the **same** native primitives in `fsmeta`, and the **same** Eunomia handoff protocol — schema is not specialized to any single consumer.

Deep dive: [fsmeta positioning](notes/2026-04-24-fsmeta-positioning.md) · [namespace authority events umbrella](notes/2026-04-25-namespace-authority-events-umbrella.md)

---

## 📑 If You Read Only Three Pages

Start here:

1. **[fsmeta.md](fsmeta.md)** — namespace metadata service (the headline). Primitives, lifecycle authority, deployment.
2. **[architecture.md](architecture.md)** — three-layer architecture. Where each module lives, what each layer is allowed to know.
3. **[control_and_execution_protocols.md](control_and_execution_protocols.md)** — the contract between control plane (`coordinator/`), execution plane (`raftstore/`), and rooted truth (`meta/root/`).

Plus, if you want to understand the verified protocol:

4. **[notes/2026-04-25-namespace-authority-events-umbrella.md](notes/2026-04-25-namespace-authority-events-umbrella.md)** — what counts as rooted truth vs runtime view, and why
5. Eunomia paper drafts are kept local-only and are intentionally excluded from Git.

---

## 🗺️ Read By Interest

### 🗂️ Namespace metadata service (`fsmeta`) — the primary product

| Topic | Doc |
|---|---|
| Complete reference (primitives + lifecycle + deployment) | [fsmeta.md](fsmeta.md) |
| Positioning v4 (DFS / OSS / AI three-audience) | [notes/2026-04-24-fsmeta-positioning.md](notes/2026-04-24-fsmeta-positioning.md) |
| Namespace authority events umbrella (Mount / SubtreeAuthority / SnapshotEpoch / QuotaFence schema) | [notes/2026-04-25-namespace-authority-events-umbrella.md](notes/2026-04-25-namespace-authority-events-umbrella.md) |
| Snapshot subtree MVCC epoch | [notes/2026-04-25-snapshot-subtree-mvcc-epoch.md](notes/2026-04-25-snapshot-subtree-mvcc-epoch.md) |
| Benchmark results (rolling: Stage 1 native vs generic-KV, Stage 2.2 watch latency) | [notes/2026-04-25-fsmeta-stage1-benchmark-results.md](notes/2026-04-25-fsmeta-stage1-benchmark-results.md) · [notes/2026-04-25-fsmeta-stage2-watch-results.md](notes/2026-04-25-fsmeta-stage2-watch-results.md) |

### 🔬 Eunomia protocol (verified authority handoff) — the intellectual core

| Topic | Location |
|---|---|
| Formal specifications (TLA+ + Apalache) | [`spec/`](../spec) · [spec/README.md](../spec/README.md) |
| `Eunomia.tla` positive model + 4 contrast specs | [`spec/Eunomia.tla`](../spec/Eunomia.tla) · [`spec/LeaseOnly.tla`](../spec/LeaseOnly.tla) · [`spec/TokenOnly.tla`](../spec/TokenOnly.tla) · [`spec/ChubbyFencedLease.tla`](../spec/ChubbyFencedLease.tla) · [`spec/LeaseStartOnly.tla`](../spec/LeaseStartOnly.tla) |
| `SubtreeAuthority.tla` — Eunomia projected onto `RenameSubtree` | [`spec/SubtreeAuthority.tla`](../spec/SubtreeAuthority.tla) |
| `MountLifecycle.tla` — mount as monotonic Eunomia instance | [`spec/MountLifecycle.tla`](../spec/MountLifecycle.tla) |
| Subtree contrast family (`SubtreeWithoutSeal`, `SubtreeWithoutFrontierCoverage`) | [`spec/`](../spec) |
| TLC / Apalache artifacts (sanitized, checked in) | [`spec/artifacts/`](../spec/artifacts/) |
| **eunomia-audit** read-only audit tool | [eunomia-audit.md](eunomia-audit.md) |

### 🏛️ Distributed runtime — the layer below fsmeta

| Topic | Doc |
|---|---|
| **Rooted truth kernel** (`meta/root`) | [rooted_truth.md](rooted_truth.md) |
| Coordinator (route / TSO / heartbeats / WatchRootEvents stream) | [coordinator.md](coordinator.md) |
| Coordinator ↔ meta/root deployment separation | [notes/2026-04-12-coordinator-meta-separation.md](notes/2026-04-12-coordinator-meta-separation.md) |
| Coordinator-driven store registry | [notes/2026-04-25-coordinator-store-registry.md](notes/2026-04-25-coordinator-store-registry.md) |
| Store membership: rooted truth vs runtime view | [notes/2026-04-25-store-membership-rooted-vs-runtime-view.md](notes/2026-04-25-store-membership-rooted-vs-runtime-view.md) |
| Raftstore overview (store / peer / admin) | [raftstore.md](raftstore.md) |
| Control-plane ↔ execution-plane contract | [control_and_execution_protocols.md](control_and_execution_protocols.md) |
| Standalone → distributed migration | [migration.md](migration.md) |
| Recovery model | [recovery.md](recovery.md) |
| Percolator MVCC 2PC + AssertionNotExist | [percolator.md](percolator.md) |
| Runtime call chains (sequence diagrams) | [runtime.md](runtime.md) |

### 🔧 Storage engine internals — the foundation

The single-node substrate that everything sits on. Independently usable as an embedded Go LSM + Raft library.

| Topic | Doc |
|---|---|
| High-level architecture | [architecture.md](architecture.md) |
| WAL discipline and replay | [wal.md](wal.md) |
| MemTable + ART/SkipList (ART pinned for fsmeta) | [memtable.md](memtable.md) |
| Flush pipeline | [flush.md](flush.md) |
| Leveled compaction + ingest buffer | [compaction.md](compaction.md) · [ingest_buffer.md](ingest_buffer.md) |
| Value log (KV separation + GC) | [vlog.md](vlog.md) |
| Manifest semantics | [manifest.md](manifest.md) |
| Range filter | [range_filter.md](range_filter.md) |
| Block / row cache | [cache.md](cache.md) |
| VFS abstraction + FaultFS | [vfs.md](vfs.md) · [file.md](file.md) |
| Hot-key observer (Thermos) | [thermos.md](thermos.md) |
| Entry / error model | [entry.md](entry.md) · [errors.md](errors.md) |

### 🛠️ Operations and tooling

| Topic | Doc |
|---|---|
| **CLI reference** (`nokv` — stats / manifest / regions / mount / quota / eunomia-audit / migrate) | [cli.md](cli.md) |
| **`nokv-fsmeta`** standalone gRPC gateway | [fsmeta.md](fsmeta.md) |
| **`nokv-redis`** secondary Redis-compatible gateway (KV layer only, does not consume fsmeta) | [nokv-redis.md](nokv-redis.md) |
| **`eunomia-audit`** offline audit tool against meta-root | [eunomia-audit.md](eunomia-audit.md) |
| Configuration (one JSON file shared by all binaries) | [config.md](config.md) |
| Cluster demo & dashboard | [demo.md](demo.md) |
| Scripts layout | [scripts.md](scripts.md) |
| Stats / expvar / metrics (4 namespaces: executor, watch, quota, mount) | [stats.md](stats.md) |
| Testing strategy (failpoints, chaos, restart, migration) | [testing.md](testing.md) |

### 📒 Notable design decision records

All notes under [`notes/`](notes/) are dated decision records — they explain the *why*, not just the what.

- [Why WAL is stdio and vlog/SST are mmap](notes/2026-01-16-mmap-choice.md)
- [Compaction and ingest buffer design](notes/2026-02-01-compaction-and-ingest.md)
- [Value log KV separation + HashKV buckets](notes/2026-02-05-vlog-design-and-gc.md)
- [Arena memory kernel + adaptive index (SkipList ↔ ART)](notes/2026-02-09-memory-kernel-arena-and-adaptive-index.md)
- [MPSC write pipeline with adaptive coalescing](notes/2026-02-09-write-pipeline-mpsc-and-adaptive-batching.md)
- [VFS abstraction + deterministic reliability testing](notes/2026-02-15-vfs-abstraction-and-deterministic-reliability.md)
- [Coordinator ↔ execution layering](notes/2026-03-30-coordinator-and-execution-layering.md)
- [SST-based snapshot install](notes/2026-03-31-sst-snapshot-install.md)
- [Delos-lite rooted-truth roadmap](notes/2026-04-03-delos-lite-metadata-root-roadmap.md)
- [Range filter — from GRF, but not quite](notes/2026-04-05-range-filter-from-grf.md)
- [fsmeta positioning v4 (DFS + OSS + AI dataset)](notes/2026-04-24-fsmeta-positioning.md)
- [Namespace authority events umbrella](notes/2026-04-25-namespace-authority-events-umbrella.md)
- [WatchSubtree rooted change feed](notes/2026-04-25-watchsubtree-rooted-changefeed.md)
- [Snapshot subtree MVCC epoch](notes/2026-04-25-snapshot-subtree-mvcc-epoch.md)

---

## 🏗️ Architecture at a Glance

<p align="center">
  <img src="assets/architecture.svg" alt="NoKV Architecture" width="100%" />
</p>

```
Layer 1  fsmeta            ← namespace primitives (Create / ReadDirPlus / WatchSubtree / RenameSubtree / SnapshotSubtree / Link / Unlink with GC)
   │
Layer 2  meta/root         ← rooted authority truth (Mount / SubtreeAuthority / SnapshotEpoch / QuotaFence; Eunomia Tenure / Legacy / Handover)
         coordinator       ← routing, TSO, store discovery, root-event publish + WatchRootEvents stream
         raftstore         ← per-region Raft + apply observer
         percolator        ← 2PC + MVCC + AssertionNotExist + commit-ts retry
   │
Layer 3  engine            ← LSM + ART memtable + WAL + value log (with per-CF/prefix value separation policy: fsm\x00 → AlwaysInline)
```

**Four invariants enforced in code:**

1. **Single source of truth.** All authority lifecycle (mount, subtree, snapshot, quota) lives in `meta/root` as typed rooted events. No parallel truths.
2. **Layer separation enforced.** Layer 1 doesn't import raftstore client; Layer 2 doesn't know about fsmeta; Layer 3 doesn't know there's a namespace.
3. **Multi-gateway-safe.** Quota usage and subtree authority handoff are Percolator-transactional against `meta/root`, not in-process counters.
4. **Streaming lifecycle, not polling.** `coordinator.WatchRootEvents` pushes mount retire / quota fence updates / pending handoff to gateways within milliseconds.

---

## ⚡ Quick Start

### Bring up a full cluster + register a mount + use fsmeta

```bash
# 1. Build binaries
make build

# 2. Launch full cluster: meta-root + coordinator + 3 stores + fsmeta gateway
./scripts/dev/cluster.sh --config ./raft_config.example.json
# (Or: docker compose up --build  — includes mount-init bootstrap)

# 3. Register a mount (rooted authority)
nokv mount register --coordinator-addr 127.0.0.1:2379 \
  --mount default --root-inode 1 --schema-version 1

# 4. (Optional) Set a quota fence
nokv quota set --coordinator-addr 127.0.0.1:2379 \
  --mount default --limit-bytes 10737418240 --limit-inodes 10000000

# 5. Use fsmeta from any gRPC client (Go typed client at fsmeta/client/)
#    or embedded Go: see fsmetaexec.OpenWithRaftstore in the root README

# 6. Inspect runtime state
curl http://127.0.0.1:9101/debug/vars | jq '.nokv_fsmeta_executor, .nokv_fsmeta_watch, .nokv_fsmeta_quota, .nokv_fsmeta_mount'
nokv eunomia-audit --workdir ./artifacts/cluster/coord-1
nokv stats --workdir ./artifacts/cluster/store-1
```

Full walkthrough: [getting_started.md](getting_started.md) · CLI reference: [cli.md](cli.md)

### Run TLA+ verification

```bash
make tlc-eunomia                            # positive model
make tlc-leaseonly-counterexample           # contrast: no rooted seal → counterexample
make tlc-tokenonly-counterexample           # contrast: bounded freshness only
make tlc-chubbyfenced-counterexample        # contrast: per-reply sequencer only
make apalache-check-eunomia                 # bounded inductive Primacy check
make record-formal-artifacts                # refresh spec/artifacts/
```

---

## 🔗 Jump Points

| | |
|---|---|
| **[fsmeta service](fsmeta.md)** | The headline product — namespace metadata API |
| **[Eunomia spec](../spec/README.md)** | TLA+ verified handoff protocol |
| **[eunomia-audit](eunomia-audit.md)** | Read-only audit tool |
| **[CLI surface](cli.md)** | `nokv` — stats, manifest, regions, mount, quota, audit, migrate |
| **[Topology config](config.md)** | One JSON file shared by scripts, Docker, all CLI |
| **[Coordinator](coordinator.md)** | Route / TSO / heartbeat / root-event subscribe |
| **[Rooted truth](rooted_truth.md)** | `meta/root` typed event log |
| **[Percolator / MVCC](percolator.md)** | 2PC primitives in distributed mode |
| **[Runtime call chains](runtime.md)** | Function-level sequence diagrams |
| **[Testing](testing.md)** | Failpoints, chaos, restart, migration |
| **[NoKV Redis](nokv-redis.md)** | Secondary RESP gateway (KV layer only) |
| **[SUMMARY.md](SUMMARY.md)** | Full mdbook table of contents |

---

<div align="center">
  <sub><strong>Open-source namespace metadata substrate for DFS, OSS, and AI dataset metadata — with TLA+-verified authority handoff.</strong></sub><br/>
  <sub>Built from scratch — no external storage engine, no external Raft library, no external coordinator.</sub>
</div>
