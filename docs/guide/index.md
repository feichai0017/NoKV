# Documentation

**NoKV** is the open-source counterpart of the *"stateless schema layer + transactional KV"* pattern that powers Meta Tectonic (over ZippyDB), Google Colossus (over Bigtable), and DeepSeek 3FS (over FoundationDB). The headline service is **`fsmeta`** — a namespace metadata API for distributed filesystems, object storage, and AI dataset metadata.

The interesting part is not the feature list. The interesting part is that **layer separation is enforced in code**: the fsmeta executor consumes a narrow `TxnRunner`; the default `fsmeta/runtime/raftstore` adapter owns raftstore wiring; `meta/root` keeps only lifecycle / authority truth; the storage engine never learns that a namespace exists.

> Looking for the landing page, headline benchmarks, and the `Why NoKV vs X?` matrix? See the [homepage](/).

---

## If you read only three pages

1. **[fsmeta](./fsmeta)** — namespace metadata service (the headline). Primitives, lifecycle authority, deployment.
2. **[Architecture](./architecture)** — layered architecture. Where each module lives, what each layer is allowed to know.
3. **[Control & Execution Protocols](./control_and_execution_protocols)** — the contract between control plane (`coordinator/`), execution plane (`raftstore/`), and rooted truth (`meta/root/`).

---

## Three audiences, one substrate

|  | DFS frontend | Object storage namespace | AI dataset metadata |
|---|---|---|---|
| **Consumer shape** | FUSE / NFS / SMB driver | S3-compatible HTTP gateway | training pipeline / scheduler |
| **Primitives used** | `ReadDirPlus`, `WatchSubtree`, `SnapshotSubtree`, `RenameSubtree` | `ReadDirPlus` for LIST, `WatchSubtree` for bucket events, `SnapshotSubtree` for versions, `RenameSubtree` for prefix moves | `SnapshotSubtree` for dataset versions, `WatchSubtree` for checkpoint notification, `ReadDirPlus` for batch metadata fetch |
| **Comparable industrial pattern** | Tectonic / Colossus / 3FS / HopsFS | Tectonic / Colossus over object layer | Mooncake / Quiver / 3FS dataset layer |

All three consume the **same** rooted truth in `meta/root` and the **same** native primitives in `fsmeta` — schema is not specialized to any single consumer.

---

## Read by interest

### 🗂️ Namespace metadata service (`fsmeta`) — the primary product

| Topic | Doc |
|---|---|
| Complete reference (primitives + lifecycle + deployment) | [fsmeta](./fsmeta) |
| Rooted truth kernel (`meta/root`) | [Rooted Truth](./rooted_truth) |
| Coordinator (route / TSO / heartbeats / WatchRootEvents stream) | [Coordinator](./coordinator) |
| Snapshot / authority / quota lifecycle | [Migration](./migration) · [Recovery](./recovery) |

### 🏛️ Distributed runtime — the layer below fsmeta

| Topic | Doc |
|---|---|
| Raftstore overview (store / peer / admin) | [Raftstore](./raftstore) |
| Control plane ↔ execution plane contract | [Control & Execution Protocols](./control_and_execution_protocols) |
| Standalone → distributed migration | [Migration](./migration) |
| Recovery model | [Recovery](./recovery) |
| Percolator MVCC 2PC + AssertionNotExist | [Percolator](./percolator) |
| Runtime call chains (sequence diagrams) | [Runtime](./runtime) |

### 🔧 Storage engine internals — the foundation

The single-node substrate everything sits on. Independently usable as an embedded Go LSM, with distributed runtime built through NoKV's raftstore integration.

| Topic | Doc |
|---|---|
| High-level architecture | [Architecture](./architecture) |
| WAL discipline and replay | [WAL](./wal) |
| MemTable + ART/SkipList | [Memtable](./memtable) |
| Flush pipeline | [Flush](./flush) |
| Leveled compaction + landing buffer | [Compaction](./compaction) · [Landing Buffer](./landing_buffer) |
| Manifest semantics | [Manifest](./manifest) |
| Range filter | [Range Filter](./range_filter) |
| Block / row cache | [Cache](./cache) |
| VFS abstraction + FaultFS | [VFS](./vfs) · [File](./file) |
| Hot-key observer (Thermos) | [Thermos](./thermos) |
| Entry / error model | [Entry](./entry) · [Error Handling](./errors) |

### 🛠️ Operations and tooling

| Topic | Doc |
|---|---|
| **CLI reference** (`nokv` — stats / manifest / regions / mount / quota / migrate) | [CLI](./cli) |
| Configuration (one JSON file shared by all binaries) | [Configuration](./config) |
| Cluster demo | [Cluster Demo](./demo) |
| Scripts layout | [Scripts](./scripts) |
| Stats / expvar / metrics | [Stats & Observability](./stats) |
| Testing strategy (failpoints, chaos, restart, migration) | [Testing](./testing) |

### 🧑‍💻 Development contract

| Topic | Doc |
|---|---|
| Code ownership, file naming, type/function naming, errors, metrics, DCO, generated code, compatibility rules | [Code Contract](./development/code_contract) |
| Human and agent PR review checklist | [PR Review Checklist](./development/pr_review_checklist) |

---

## Architecture at a glance

<img src="/img/architecture.svg" alt="NoKV Architecture" />

```
Layer 1  fsmeta            ← namespace primitives
   │
Layer 2  meta/root         ← rooted authority truth
         coordinator       ← routing, TSO, store discovery, root-event publish
         raftstore         ← per-region Raft + apply observer
         percolator        ← 2PC + MVCC + AssertionNotExist + commit-ts retry
   │
Layer 3  engine            ← LSM + ART memtable + WAL + slab sidecar substrate
```

**Four boundaries enforced in code:**

1. **fsmeta-first API.** Metadata operations expose filesystem / object-namespace shapes directly, instead of forcing users to assemble them from raw KV calls.
2. **Layer separation enforced.** The fsmeta executor consumes a narrow `TxnRunner`; the default runtime adapter owns raftstore wiring; lower layers do not import fsmeta.
3. **Multi-gateway-safe.** Quota fences are rooted truth; usage counters are data-plane keys updated in the same Percolator transaction as metadata mutations. Subtree handoff uses rooted events plus runtime repair.
4. **Root-event driven lifecycle.** `coordinator.WatchRootEvents` pushes mount retire / quota fence / pending handoff changes after bootstrap; the monitor interval is reconnect backoff.

---

## Quick start

```bash
# 1. Build binaries
make build

# 2. Launch full cluster: meta-root + coordinator + 3 stores + fsmeta gateway
./scripts/dev/cluster.sh --config ./raft_config.example.json
# (Or: docker compose up -d  — includes mount-init bootstrap)

# 3. Register a mount (rooted authority)
nokv mount register --coordinator-addr 127.0.0.1:2379 \
  --mount default --root-inode 1 --schema-version 1

# 4. (Optional) Set a quota fence
nokv quota set --coordinator-addr 127.0.0.1:2379 \
  --mount default --limit-bytes 10737418240 --limit-inodes 10000000

# 5. Use fsmeta from any gRPC client (Go typed client at fsmeta/client/)

# 6. Inspect runtime state
curl http://127.0.0.1:9101/debug/vars | jq '.nokv_fsmeta_executor, .nokv_fsmeta_watch, .nokv_fsmeta_quota'
nokv stats --workdir ./artifacts/cluster/store-1
```

Full walkthrough: [Getting Started](./getting_started) · CLI reference: [CLI](./cli)

---

## Jump points

| | |
|---|---|
| **[fsmeta service](./fsmeta)** | The headline product — namespace metadata API |
| **[Formal specs (spec/)](https://github.com/feichai0017/NoKV/blob/main/spec/README.md)** | TLA+ / TLC models for transition safety |
| **[CLI surface](./cli)** | `nokv` — stats, manifest, regions, mount, quota, migrate |
| **[Topology config](./config)** | One JSON file shared by scripts, Docker, all CLI |
| **[Coordinator](./coordinator)** | Route / TSO / heartbeat / root-event subscribe |
| **[Rooted truth](./rooted_truth)** | `meta/root` typed event log |
| **[Percolator / MVCC](./percolator)** | 2PC primitives in distributed mode |
| **[Runtime call chains](./runtime)** | Function-level sequence diagrams |
| **[Testing](./testing)** | Failpoints, chaos, restart, migration |
