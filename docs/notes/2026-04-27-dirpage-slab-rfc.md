# 2026-04-27 DirPageSlab RFC: materialize ReadDirPlus into packed dirent pages

> Status: **RFC** (design proposal, not implemented). This is the Phase 5 expansion RFC for the slab substrate redesign (`2026-04-27-slab-substrate.md`). It pins down the API, physical wire format, coupling points with fsmeta primitives, and the phase roadmap.
>
> DirPageSlab is the **innovation core** of the slab substrate — unlike NegativeSlab (a "persistent cache"), it's the dedicated physical layout for a NoKV native metadata primitive (`fsmeta.ReadDirPlus`). Generic KVs (RocksDB / Pebble / Badger / FoundationDB) cannot do this because they don't know what a "directory" is.

---

## 1. The pain

### 1.1 ReadDirPlus cost today

`fsmeta.ReadDirPlus` (semantically; `PlanReadDir` + the subsequent reads) currently goes through:

1. LSM prefix scan over dentry keys (encoded `mount|parent|name`).
2. For each dentry record, LSM `Get` the matching inode record.
3. Assemble into a `DentryAttrPair[]` and return.

Fine for a single lookup. For **large directories** (HDFS experience: directories with 10K-1M entries are routine — log archives, training-dataset shards, object-store bucket indexes):

- Prefix scan: N dentries spread across many L0/L1 SSTs → N block IOs + binary search.
- Per-dentry inode lookup: N Gets → N SST probes (decode cost even when hot).
- Block cache holds SST data blocks; **dirents and inodes are physically scattered** — a 10K-entry directory can span dozens of blocks, and cache fragmentation is severe.
- After compaction, N SSTs are reshuffled and the hot directory's block locality is often destroyed.

Profiles regularly show a 10K-entry ReadDirPlus taking hundreds of ms in a cold cache. This is exactly why HDFS / CephFS struggle with large directories.

### 1.2 Why generic-KV optimizations don't help

- **Bloom filter**: only useful for existence queries, useless for prefix scans.
- **Block cache**: heavy fragmentation; hit ratio doesn't solve "still need N decodes."
- **Prefix bloom**: helps random lookup, doesn't reduce single-pass full-directory scan.
- **Shard hint cache**: previous work, single-key effective; for N dentries, still N times.
- **Negative cache**: irrelevant.

**Root issue**: generic KV treats dentry and inode as opaque key-value pairs and cannot offer "directory as an object" layout.

### 1.3 Industry comparison

- **HDFS NameNode**: pure-memory; large-directory cost ceiling is memory.
- **CephFS MDS**: fragmenting + `dirfrag` page-based directory — similar to DirPage.
- **JuiceFS / SeaweedFS filer**: relies on the backend KV's prefix scan — same pain as NoKV today.
- **TiKV-backed FS**: relies on application-layer (CDC / TiFlash) materialization, decoupled from the KV engine.

NoKV's edge: **fsmeta primitives are first-class, so DirPage can live in the engine** — no need for an application-layer cache.

## 2. Core design

### 2.1 Concept

Materialize `(mount, parent_inode)` dentry+attr lists into **packed pages**, stored in a slab consumer. LSM remains the authoritative truth; DirPage is **Derived class** — loss / staleness / corruption can all be recovered from LSM.

### 2.2 Consistency class

**Derived** (per slab-substrate note §5):
- LSM is authoritative.
- DirPage is cache / materialization.
- Crash losing DirPage = re-warm (next ReadDirPlus falls back to LSM scan and rebuilds along the way).
- Doesn't participate in commit pipeline; no WAL integration needed.
- Not in main manifest.

**Key invariants**:
- Write path (Create/Unlink/Rename) **always** writes LSM correctly.
- DirPage is purely query acceleration; any loss is best-effort.
- Frontier check (§4) ensures stale data is never returned.

## 3. Wire format

Each DirPage record (an entry within a slab segment):

```
DirPage record:
  magic        uint32  "DPSL" little-endian
  version      uint16
  mount        varint  (encoded mount ID)
  parent       uvarint (parent InodeID)
  page_no      uvarint (0-indexed within (mount, parent))
  frontier     uvarint (WatchSubtree event cursor / mutation epoch)
  entry_count  uvarint
  entries: repeated of
    name_len   uvarint
    name       [name_len]byte
    inode      uvarint  (InodeID)
    attr_blob  uvarint length-prefixed (encoded InodeRecord)
  checksum     uint32 (CRC32 of preceding bytes within this record)
```

Design choices:
- **Includes inode attr blob**: matches ReadDirPlus semantics — saves N follow-up Gets.
- **Each record carries its own frontier**: readers don't consult another table.
- **Decoupled from but reusing InodeRecord encoding**: DirPage stores `attr_blob`; callers use `fsmeta.DecodeInodeValue` to deserialize.
- **Per-record checksum**: partial corruption only loses one page.

### 3.1 Page size

Target **4-16KB** per page (one mmap page). Entry count caps at 64-256 (depending on average name length). Large directories span pages, chained by `page_no`.

### 3.2 Page lifecycle

| State | Meaning |
|---|---|
| Valid | frontier matches the directory's current mutation epoch |
| Stale | frontier is behind; readers skip it and trigger lazy rebuild |
| Garbage | segment is full or stale ratio is high; awaits compaction's whole-retire |

## 4. Coupling with fsmeta primitives

### 4.1 Frontier (mutation epoch)

Each `(mount, parent_inode)` maintains a monotonically increasing `dir_epoch`. Any operation that mutates this directory (Create / Unlink / Link / Rename involving from/to parent) **bumps epoch by 1**.

Where to store:
- Option A: add `DirEpoch uint64` to InodeRecord.
- Option B: separate `dir_epoch` key per directory inode.
- Option C: pure in-memory, infer from max DirPage frontier on restart (best-effort Derived can do this).

**Recommend Option C**: dir_epoch doesn't need persistence (DirPage is Derived); on restart, fall back to LSM rebuild once. Phase 5b decides.

### 4.2 WatchSubtree integration

`WatchSubtree` already maintains an event cursor per subtree. DirPage's frontier reuses WatchSubtree's `event_cursor`:
- Any dentry modification within the subtree → cursor++.
- DirPage write records the current cursor as frontier.
- DirPage read compares cursor; if behind, stale.

This unifies DirPage invalidation with the `WatchSubtree` event stream. The dentry change events Watch clients see and the DirPage stale signal share the same trigger.

### 4.3 RenameSubtree invalidation

`PlanRenameSubtree(req)` currently moves the subtree root dentry. Invalidation path:
- bump source parent's dir_epoch.
- bump destination parent's dir_epoch.
- subtree-internal dentries don't move → internal DirPages don't need invalidation.

`PlanRename` (same-parent rename) only bumps that parent's dir_epoch.

### 4.4 Unlink invalidation

`PlanUnlink(req)` deletes one dentry:
- bump parent's dir_epoch.

No need to scan DirPage for the specific entry to remove — next read skips the stale page and lazily rebuilds.

### 4.5 Create / Link invalidation

Same as above: bump parent's dir_epoch.

## 5. API design

### 5.1 fsmeta layer

```go
// fsmeta/dirpage.go (new file)

// DirPageReader is the Derived consumer interface DirPageSlab implements.
type DirPageReader interface {
    // Lookup returns the materialized pages for (mount, parent) if every
    // page is fresh (frontier matches the current dir_epoch). Returns
    // (nil, false) on miss / stale / corrupt — caller falls back to LSM
    // prefix scan and may opportunistically MaterializeAsync.
    Lookup(mount MountID, parent InodeID) ([]DentryAttrPair, bool)

    // MaterializeAsync schedules a background rebuild of the directory
    // page set. Caller hands over the LSM-scanned dentry+attr pairs.
    // Idempotent.
    MaterializeAsync(mount MountID, parent InodeID, frontier uint64, pairs []DentryAttrPair)

    // Invalidate bumps dir_epoch for (mount, parent). Called from
    // Create/Unlink/Link/Rename plan apply path.
    Invalidate(mount MountID, parent InodeID)
}
```

### 5.2 ReadDirPlus integration

```go
func (s *Server) ReadDirPlus(req ReadDirRequest) ([]DentryAttrPair, error) {
    // 1. fast path: DirPageSlab
    if pairs, ok := s.dirPages.Lookup(req.Mount, req.Parent); ok {
        return paginate(pairs, req.StartAfter, req.Limit), nil
    }
    // 2. fallback: LSM prefix scan + per-dentry inode Get
    pairs, frontier, err := s.scanLSM(req.Mount, req.Parent)
    if err != nil { return nil, err }
    // 3. async materialize
    s.dirPages.MaterializeAsync(req.Mount, req.Parent, frontier, pairs)
    return paginate(pairs, req.StartAfter, req.Limit), nil
}
```

### 5.3 LSM layer

```go
// engine/lsm/dirpage.go (new file)
// DirPageSlab implements fsmeta.DirPageReader on top of slab.Manager.

type DirPageSlab struct {
    mgr      *slab.Manager     // physical substrate (built in Phase 2)
    epochs   sync.Map          // (mount, parent) -> *atomic.Uint64
    cache    *dirPageCache     // in-memory page index (LRU)
}
```

## 6. Implementation phase plan

| Phase | Content | Estimated effort |
|---|---|---|
| **5a** | RFC (this) | done |
| **5b** | `engine/slab/manager.go`: extract a generic segment manager from `engine/vlog/manager.go` (rotation / GC sample / sub-manifest); DirPage and future consumers all use it | 1 week |
| **5c** | `engine/lsm/dirpage.go`: DirPageSlab implementation with standalone unit tests (page write/read/stale detect); not yet wired into fsmeta | 1 week |
| **5d** | fsmeta `ReadDirPlus` fast-path integration + LSM fallback + async materialize; end-to-end unit tests | 1 week |
| **5e** | Invalidation paths: Create/Unlink/Link/Rename/RenameSubtree call `Invalidate` | 0.5 week |
| **5f** | Large-directory bench: 10K / 100K entry ReadDirPlus latency cold/warm | 0.5 week |

Total ~4 weeks. Each phase ships as an independent PR.

## 7. Bench validation goals

Add `BenchmarkFsmetaReadDirPlus`:

| Axis | Values |
|---|---|
| Entries per dir | 1K / 10K / 100K |
| Cache state | cold / warm |
| Operation | full ReadDirPlus / paginated |
| Comparison | DirPageSlab on vs off |

Expected wins:
- 10K-entry warm cache: drop from ~10-50ms to <1ms (one mmap page read).
- 10K-entry cold (first read): on par with baseline (must do one LSM scan to rebuild).
- 100K-entry warm: one page read significantly outperforms N SST IOs.

## 8. Consistency / risk / trade-offs

### 8.1 Stale page risk

Late invalidation → reader sees stale data. Mitigations:
- Frontier comparison is per-read; a stale page falls straight back to LSM.
- Write path **must synchronously** call `Invalidate` (cannot be async) so the next read immediately falls back; *re-materialize* is the async part, not invalidation.
- Test coverage: ReadDirPlus immediately after a mutation must observe the new state.

### 8.2 Page-size tuning

Too small → large directories span too many pages, sequential read becomes random read.
Too large → one page hogs cache and evicts other hot directories.
Phase 5c starts with 4-16KB and tunes after bench.

### 8.3 Frontier persistence

Cost of Option C (no persistence): the first ReadDirPlus after restart must fall back to LSM and rebuild. Brief latency bump after restart (warm-up window). Acceptable — same semantics as NegativeSlab's "crash forces re-warm."

### 8.4 Multi-page consistency

Does ReadDirPlus across pages need atomicity? **No**. LSM scan itself isn't atomic (writes can occur during the scan). DirPage frontier is per-page; pages A and B may have different frontiers — the caller observes "two partial updates," matching current LSM scan behavior. Clients wanting atomicity should use SnapshotSubtree to hold a token.

### 8.5 Relationship with SnapshotSubtree

SnapshotSubtree returns an MVCC token; DirPage is unrelated to tokens — it always reads latest. A "frozen subtree" should go through SST snapshot install or the future SnapshotSlab, not DirPage. **Two independent axes.**

### 8.6 Memory cost

Per page 4-16KB; 100K entries / 10 entries per page = 10K pages × 16KB = 160MB per directory. LRU eviction needed. Phase 5c adds a cap option (default 256MB-1GB per LSM).

## 9. Innovation list (these are paper-grade design points)

After the metaslab umbrella was trimmed down, the genuinely "NoKV-only" design points are:

1. **Primitive-aware physical layout**: DirPage maps one-to-one to `ReadDirPlus`, not split by value size.
2. **WatchSubtree-driven frontier**: cache invalidation unifies with fsmeta's event stream — no separate invalidation protocol.
3. **Derived consistency class**: lossy-recoverable, no WAL/commit; opens DirPage design space without falling into the "another LSM" trap.
4. **Lazy rebuild on miss**: fast-path miss automatically triggers materialize — no explicit warmup API.

Generic KVs cannot do any of these four — they require the engine to know what a metadata primitive is. **This is the real design point that distinguishes NoKV from RocksDB / Pebble / Badger / FoundationDB**, not "we also have vlog."

## 10. Decision log

- **Page contains inode attr blob**: avoids ReadDirPlus's secondary Get; trades space for IO count. Per page +inode size × entry count.
- **Frontier uses WatchSubtree event cursor**: reuse the existing event stream, no separate invalidation protocol.
- **Derived class, not in main manifest**: failure-loss = re-warm; maximizes design freedom.
- **Page size 4-16KB starting point**: aligned with mmap page size; tune after bench.
- **Don't persist dir_epoch**: Option C; restart warm-up window is acceptable.
- **Don't aim for "atomic ReadDirPlus across pages"**: matches LSM scan behavior; for atomicity use SnapshotSubtree.

## 11. Related notes

- `2026-04-27-slab-substrate.md` — slab substrate redefinition; this is its Phase 5 expansion RFC.
- `2026-04-27-snapshot-slab-spike.md` — Phase 4 spike outcome.
- `2026-04-24-fsmeta-positioning.md` — fsmeta product positioning.
- `2026-04-25-snapshot-subtree-mvcc-epoch.md` — SnapshotSubtree epoch model.
- `2026-04-25-namespace-authority-events-umbrella.md` — control-plane event delivery model; protocol foundation for DirPage frontier vs WatchSubtree event stream.
