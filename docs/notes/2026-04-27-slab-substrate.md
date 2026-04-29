# 2026-04-27 Slab Substrate: a typed sidecar physical layout for NoKV's metadata primitives

> Status: **Phases 0–5 shipped, 6 forward-ref**. Phase 0 in PR #161; Phases 1–5 on the `feature/slab-substrate` branch: metadata default no-offload fast path (Phase 1), `engine/slab/Segment` physical layer extraction (Phase 2), persistent Negative Slab (Phase 3), SnapshotSlab spike → not building (Phase 4, separate note), DirPageSlab RFC (Phase 5, separate note) — all committed. Phase 5b (split out `engine/slab/Manager` + downgrade vlog to a wrapper) completed on the same branch. Phase 6 (UpdateSlab) is an independent RFC, **not on this branch**.
>
> This is the third version of the vlog refactor note. The first two ("MetaSlab redesign" / "Slab substrate v1") were each refined in review. The final cut is no longer "refactor vlog" — it's **upgrade slab into a typed sidecar physical layout for NoKV native metadata primitives**. Slab isn't a new vlog. It's the *primitive-aware physical layout* of fsmeta primitives (`ReadDirPlus`, `SnapshotSubtree`, `WatchSubtree`, `RenameSubtree`).

---

## 1. Repositioning: from "vlog refactor" to "primitive-aware physical layout"

The first two versions treated slab as "a more general vlog" — same idea, "values aren't size-graded, they're laid out by metadata semantics." This review pushed further: **what's actually innovative is "the physical location is determined by the NoKV native primitive."**

| Generic-KV view (old) | NoKV-native view (new) |
|---|---|
| value > threshold goes to vlog | `ReadDirPlus` large directories go to DirPage Slab |
| Generic negative cache | fsmeta-key negative slab, bound to mount/subtree |
| snapshot = LSM range scan | `SnapshotSubtree` produces a sealed slab artifact directly |
| GC by reference scanning | GC by lifecycle event (mount/subtree retire) |

Tying slab to NoKV's own primitives is what actually distinguishes us from RocksDB / Pebble / Badger / FoundationDB. Otherwise we'd just be writing yet another generic-KV optimization.

## 2. Mismatch between product positioning and current state

NoKV's customer scenarios are **metadata-first**:

| Scenario | Value size | Main primitive |
|---|---|---|
| DFS metadata (HDFS / CephFS / JuiceFS / SeaweedFS filer) | 150B-1KB | `ReadDirPlus`, `Lookup` |
| Object-store metadata (S3 manifest / bucket index) | 200B-1KB | `ListObjects`, `HeadObject` |
| AI training metadata (dataset manifest / feature schema) | 100-500B | `SnapshotSubtree`, checkpoint |
| Embedded occasional large objects | 4KB-1MB | Explicit blob API |

Problems with vlog as it stands:

1. **Mandatory on the primary write path**: `db.vlog.write()` is unavoidable in the commit pipeline.
2. **Large bug surface**: 1M+3KB bench exposed `LogFile.Write` shrinking `lf.size` when out-of-order Writes followed reservation decoupling (fix in §4).
3. **Physical layer and business semantics tangled in `engine/vlog/`**: mmap segment management, bucket routing, ValuePtr encode/decode, and GC sampling all live in one package — any other subsystem that wants to reuse the mmap physical layer has to pull in vlog business semantics.
4. **No primitive-awareness**: every large value goes to vlog; we don't differentiate dataset snapshot, dir page, negative cache, and value separation, each of which has different lifecycle and consistency needs.

## 3. Three-layer architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│  Layer 3 — Rooted Lifecycle (control-plane integration)              │
│    correctness-critical slab → meta/root lifecycle                   │
│    derived slab → not in root, lost-and-rebuild                      │
│    snapshot slab → epoch-bound, retire by file unlink                │
├──────────────────────────────────────────────────────────────────────┤
│  Layer 2 — Typed Slab Consumers (business semantics)                 │
│  ┌──────────────┬──────────────┬──────────────┬────────────────────┐│
│  │ ValueLog     │ Negative     │ DirPage      │ Snapshot          ││
│  │ (existing)   │ Slab         │ Slab         │ Slab              ││
│  │ Authoritative│ Derived      │ Derived      │ Lifecycle-bound   ││
│  └──────────────┴──────────────┴──────────────┴────────────────────┘│
├──────────────────────────────────────────────────────────────────────┤
│  Layer 1 — Slab Substrate (engine/slab/)                             │
│    BlobLog / SlabFile: append / read / seal / verify / remove        │
│    Does NOT include: ValuePtr, bucket routing, business GC,          │
│                      main-manifest integration                       │
└──────────────────────────────────────────────────────────────────────┘
```

**Key invariant**: each layer cares only about its own concerns. The physical layer doesn't know what DirPage is; the DirPage Consumer doesn't know how Snapshot seals; Rooted Lifecycle doesn't know how mmap grows.

## 4. Phase 0: vlog `LogFile.Write` high-water CAS (done)

### 4.1 Bug

1M YCSB + 3KB value (triggering value separation) crashed with `NoKV read: EOF` after ~4 seconds. Minimal repro: 200k records + 3KB + conc=16 fails reliably within 0.79s.

Debug log shows `EOF #3`: `offset+valsz > lfsz` with `offset == lfsz`, meaning the batch that wrote the entry already published the ptr but `lf.size` didn't include the tail.

### 4.2 Root cause

`engine/vlog/manager.reserve()` holds `m.filesLock` to hand each batch a disjoint offset range, **and only after releasing filesLock** does the batch contend `store.Lock` to write. Reserve order across N batches is decoupled from Write completion order: the larger-offset Write completes first (lfsz=300), the smaller-offset Write completes later (`lfsz.Store(200)` overwrites the high watermark backwards) → an already-published ptr hits EOF on Read.

### 4.3 Fix

`LogFile.Write` switched to monotonic CAS (`engine/file/vlog.go:113-129`).

### 4.4 Key invariant

> **Invariant V1**: a value pointer is only published into LSM after `db.vlog.write(reqs)` returns in full. By the time a reader sees a ptr, every Write of the corresponding batch has completed and `lf.size ≥ ptr.offset + ptr.len`.

The high-water CAS only ensures `lf.size` is monotonic — it doesn't guarantee "no holes within the watermark." Holes do exist (reserved-but-not-Written intervals), but V1 prevents readers from ever hitting them. `db.go runBurstCommit / runSingleCommit` enforces V1 by ordering `vlog.write → applyRequests`.

### 4.5 Test coverage

- `engine/file/vlog_test.go::TestLogFileWriteSizeMonotonicOutOfOrder` — covers `lf.size` monotonicity (fails without the fix).
- `engine/vlog/manager_test.go::TestManagerConcurrentAppendReadAfterWrite` — 16 worker × 64 batch × 8 entries, concurrent AppendEntries + immediate Read, explicitly covers invariant V1 (ptr publish discipline).

## 5. Consistency-class taxonomy (this is the design center, not implementation detail)

Every slab consumer must declare a consistency class. **Slabs are not all the same kind of object** — mixing them in one design produces accidents like UpdateSlab "breaking MVCC semantics."

| Class | Meaning | Example | Failure semantics |
|---|---|---|---|
| **Authoritative** | The data body is held by slab; losing it is data loss. | ValueLog (ptr → vlog payload) | Strict WAL/manifest/GC guarantees required. |
| **Lifecycle-bound** | Managed by external lifecycle events (snapshot epoch / mount retire). | Snapshot Slab | Immutable after seal; whole-file delete after retire. |
| **Derived** | LSM is authoritative; slab is cache/materialization. | Negative Slab, DirPage Slab, (future) Hot Cache | Lossy-recoverable; doesn't participate in commit. |
| **Transactional** | Integrated with MVCC version / commit; must be atomic. | (future) UpdateSlab — awaits independent design | Must be designed alongside WAL/Percolator. |

**This taxonomy itself is a design point**: the previous metaslab note treated five slabs as homogeneous because it didn't separate classes.

## 6. Three v1 consumers

### 6.1 NegativeSlab (Derived)

**NoKV primitive it serves**: fsmeta `Lookup` / `GetAttr` for non-existent paths; S3 GetObject 404; HDFS path probes.

**Mechanism**: `engine/lsm/negative_cache.go` is currently an in-memory cuckoo filter — wiped on process restart. Add a `slab.Manager` backend:
- async append miss-keys (off the main read path)
- on restart, iterate segments to rebuild the in-memory cuckoo filter
- crash losing segment data = re-warm; **read correctness is unaffected**
- **no manifest needed** (rebuild suffices)

**Why first consumer**: failure doesn't affect correctness, lowest risk, ideal for validating the slab substrate abstraction.

**Win**: full negative cache available immediately after process restart, zero warmup.

### 6.2 SnapshotSlab (Lifecycle-bound)

**NoKV primitive it serves**: `fsmeta.SnapshotSubtree`, AI dataset checkpoint, `PlanSnapshotSubtree`.

**Mechanism**: `SnapshotSubtree` currently returns a `SnapshotSubtreeToken` (MVCC read epoch); subsequent reads still go through LSM MVCC. SnapshotSlab adds "a snapshot can materialize into a sealed slab artifact":

```go
// Take a subtree snapshot, materialize into a slab.
artifact, err := db.MaterializeSnapshot(SnapshotSubtreeRequest{...})
// `artifact` is a sealed slab file containing all dentries+attrs.
// Can be exported via export / scp / S3.
// Retire = file unlink, no GC scan needed.
```

**Lifecycle**:
- write: triggered by snapshot epoch, write once.
- seal: after epoch closes, slab seals — immutable.
- retire: when the epoch retires, unlink the slab file, O(1).

**Wins**:
- AI dataset checkpoints can export as a single slab artifact (zero-copy sendfile across nodes).
- Snapshot lifecycle is strictly bound to the epoch — no separate GC.
- Aligned with `2026-03-31-sst-snapshot-install.md`: SST snapshot is raft-level physical migration; SnapshotSlab is fsmeta-level logical export.

**Phase 4 spike**: investigate the existing SST snapshot install first; only build SnapshotSlab if it doesn't duplicate raft snapshot.

### 6.3 DirPageSlab (Derived) — the most NoKV-distinctive innovation

**NoKV primitive it serves**: `fsmeta` `ReadDirPlus`-style operations (returning `DentryAttrPair`).

**Pain**: large-directory listing is the central bottleneck for metadata systems; generic KV cannot natively optimize. Today NoKV walks LSM prefix scans:
- Each ReadDirPlus walks N SSTs' prefix range.
- Block cache holds SST data blocks, not packed dirents.
- Large directories (10K+ entries) pay N IOs + N decodes per list.

**Mechanism**: DirPage Slab materializes large directories' dentry+attr into packed pages:

```
DirPage record:
  mount        uint32
  parent_inode uint64
  page_no      uint32
  frontier     uint64  // WatchSubtree event cursor; check if page is stale
  checksum     uint32
  payload      []packed DentryAttrPair
```

**Read path**:
1. `ReadDirPlus` first checks DirPageSlab: find pages for `(mount, parent_inode)`.
2. If present and `frontier ≥ current WatchSubtree epoch` → sequential page read, O(1) decode into `DentryAttrPair[]`.
3. Otherwise, fall back to LSM prefix scan; async-write a new page.

**Write path**:
- The main write path **does not change** — dentries still go to LSM (authoritative truth).
- DirPage is derived, async-materialized (compaction background / lazy build on first ReadDirPlus).
- `RenameSubtree` / `Unlink` invalidates pages for affected `(mount, parent_inode)` (mark stale); no synchronous rewrite required.

**Coupling with fsmeta primitives**:
- WatchSubtree event cursor → DirPage frontier.
- RenameSubtree → invalidate source + destination parent pages.
- SnapshotSubtree → can materialize from DirPage if frontier is fresh enough.

**Why this is the NoKV design point**:
- Generic KV (RocksDB / Pebble / Badger): doesn't know what a "directory" is, so it can't be specifically optimized.
- TiKV: depends on application-layer (CDC, TiFlash) materialization, decoupled from the KV engine.
- NoKV: fsmeta primitives are first-class; DirPage corresponds directly to `ReadDirPlus` — it's a native engine optimization.

**Wins**:
- Large-directory ReadDirPlus goes from N SST IOs + N decodes → 1 page read.
- Block cache no longer crowded out by large-directory dirents.
- WatchSubtree integration is natural — no external invalidation mechanism needed.

### 6.4 ValueLog Consumer (Authoritative, retained)

The `engine/vlog/` package **stays**, with zero API changes for `db.go`:
- bucket routing, ValuePtr encode/decode, discardStats, main-manifest integration all stay in this layer.
- physical IO delegates to `slab.Manager`.

**Demotion**: from "primary write path mandatory" to "value separation consumer". In metadata profile (value < threshold), the commit pipeline doesn't enter vlog code (Phase 1 fast path).

**Main manifest unchanged**: ValueLog is Authoritative class; segment metadata (valueLogHead / discardStats) must remain in the main manifest — that's the correctness boundary.

## 7. Update Slab: explicitly out of scope for v1

Both prior versions treated UpdateSlab as "in-place optimization." But it **breaks the invariant "one version = one LSM entry"**:
- snapshot read: get LSM historical version + slot current value → time-warp.
- Percolator commit: lock/write/data three CFs — which CF does the slot live in? How does slot CAS sequence with lock acquire?
- crash recovery: how do we order WAL replay's main-path entry vs slot record?
- Bloom + range filter: slot updates don't go through LSM; the filter doesn't change → false negative.

UpdateSlab is **Transactional class**; it must be its own RFC. If we ever build it, the design should be **versioned append/delta** (each update writes a new version into slab; the version chain aligns with MVCC), **not** in-place fixed slot.

This vlog refactor **does not include UpdateSlab**.

## 8. Innovation list (paper-grade design points)

| Design point | Meaning | Different from |
|---|---|---|
| **Primitive-aware physical layout** | Physical location determined by metadata primitive, not value-size threshold. | RocksDB / Badger are size-based. |
| **Authority-scoped lifecycle** | Slab create/seal/retire bound to mount / subtree / snapshot epoch. | Generic KVs rely on background GC scanning references. |
| **Correctness-class separation** | Authoritative / Lifecycle-bound / Derived / Transactional explicitly separated. | Most KVs treat cache and data as the same kind of thing. |
| **Directory-page materialization** | ReadDirPlus from KV prefix scan → metadata-native page read. | Generic KVs don't know what a directory is. |
| **Snapshot as storage artifact** | SnapshotSubtree produces a sealed slab, can export/transfer. | Snapshots are usually MVCC tokens, not physical objects. |
| **GC by lifecycle, not scanning** | Whole-file unlink, no reference counting. | vlog GC must sample + scan. |

Of these six, **Directory-page materialization** and **Authority-scoped lifecycle** are the "NoKV-original" pieces — the former corresponds directly to fsmeta primitives; the latter aligns with the three-plane positioning (control-plane events drive slab lifecycle).

## 9. v1 roadmap

| Phase | Action | Class | Status | Validation |
|---|---|---|---|---|
| **0** | LogFile.Write high-water CAS | — | ✓ done (PR #161, b6b0dd25) | 1M+3KB all 6 workloads first |
| **0a** | manager concurrent AppendEntries+Read tests | — | ✓ done (PR #161) | invariant V1 explicitly covered |
| **0b** | Correct design note (this) | — | ✓ done | this note |
| **1** | metadata default no-offload fast path | — | ✓ done (c0458f03) | BenchmarkDBCommitVlogFastPath inline +22%~+64% |
| **2** | Extract `engine/slab/Segment` physical layer; vlog file layer becomes a wrapper | — | ✓ done (083a71a0) | existing vlog unit tests + 1M+3KB bench all green |
| **2a** | Redo `Segment` size semantics (Open=0, LoadSizeFromFile, Capacity) | — | ✓ done | TestSegmentFreshOpenSizeIsZero / TestSegmentLoadSizeFromFile |
| **3** | NegativeSlab + top-level `NoKV.Options` propagation | Derived | ✓ done (c0dbaa35 + later) | TestNegativeCachePersistsAcrossOpen |
| **4** | SnapshotSlab spike → not building | Lifecycle-bound | ✓ done | `2026-04-27-snapshot-slab-spike.md` |
| **5** | DirPageSlab RFC (API + format + frontier) | Derived | ✓ RFC done | `2026-04-27-dirpage-slab-rfc.md` |
| **5b** | Split out `engine/slab/Manager` + downgrade vlog to wrapper | — | ✓ done | existing vlog/lsm full tests + race-clean |
| **5c-5f** | DirPageSlab implementation (page write/read, ReadDirPlus integration, invalidation, bench) | Derived | TODO | large-directory ReadDirPlus latency |
| **6** | UpdateSlab independent RFC | Transactional | independent | design first, then implement, **not on this branch** |

## 10. 1M + 3KB bench baseline (vlog path fully exercised)

After Phase 0 fix, all 6 workloads place NoKV first:

| Workload | NoKV | Badger | Pebble | NoKV vs Badger |
|---|---|---|---|---|
| A 50/50 r/u | 607K | 283K | 25K | 2.1x |
| B 95/5 r/u | 1.15M | 651K | 153K | 1.8x |
| C 100% read | 989K | 634K | 78K | 1.6x |
| D latest | 1.10M | 732K | 182K | 1.5x |
| E scan | 69K | 29K | 46K | 2.4x |
| F RMW | 510K | 225K | 26K | 2.3x |

This is the comparison starting point with vlog fully exercised (Authoritative consumer). After Phase 1 ships, run 1M+1KB (no value separation) as the metadata-profile baseline. After Phase 5 DirPageSlab ships, run a dedicated large-directory ReadDirPlus bench (fsmeta's own workload, not YCSB).

## 11. Decision log

- **Don't build a "generic vlog"**: slab is a typed sidecar physical layout for NoKV native primitives, not a BadgerDB-style generic value separation.
- **Physical layer named Slab; single file named Segment**: aligned with kernel allocator / RocksDB / Pebble terminology.
- **Keep the vlog package name**: consistent with Badger / RocksDB BlobDB conventions; users won't be confused.
- **ValueLog metadata stays in main manifest**: Authoritative class — correctness boundary.
- **Negative / DirPage use independent SlabManifest or no manifest**: Derived class; lossy-recoverable.
- **DirPage Slab is the core innovation**: directly corresponds to fsmeta `ReadDirPlus`; no other generic KV has this design point.
- **UpdateSlab out of this refactor**: Transactional class; breaks "one version = one LSM entry" invariant — must be its own RFC.
- **DeleteSlab out of this refactor**: existing LSM RangeTombstone already covers most batch-delete cases.
- **Add a spike for the Snapshot consumer**: may overlap with SST snapshot install.

## 12. Related notes

- `2026-04-27-sharded-wal-memtable.md` — primary write-path sharding; both this and Phase 1 no-offload fast path are main-path optimizations.
- `2026-04-27-parallel-l0-compaction.md` — compaction parallelization.
- `2026-03-31-sst-snapshot-install.md` — current raft-level snapshot install; reference during Phase 4 spike.
- `2026-04-25-namespace-authority-events-umbrella.md` — control-plane event delivery model; DirPage / Snapshot Slab lifecycle triggers must align.
- `2026-04-24-fsmeta-positioning.md` — fsmeta product positioning; the source of slab-consumer designs.
- `2026-04-25-snapshot-subtree-mvcc-epoch.md` — SnapshotSubtree epoch model; lifecycle foundation for SnapshotSlab.
