# 2026-04-27 SnapshotSlab spike: does the existing SST snapshot install need a new slab consumer?

> Status: **Spike note** (research only, not implemented). Conclusion: v1 does **not** ship SnapshotSlab; the existing SST snapshot install already covers every raft-path use case. SnapshotSlab's real niche is fsmeta `SnapshotSubtree` → AI dataset checkpoint export, and that demand has not materialized yet — wait for the demand to actually appear before building it.
>
> Trigger: the slab substrate redesign note (`2026-04-27-slab-substrate.md` §6.2) listed SnapshotSlab as a v1 candidate consumer, but required a spike first to avoid duplicating the existing SST snapshot install (`2026-03-31-sst-snapshot-install.md`). This is the result of that spike.

---

## 1. What the existing SST snapshot install already does

Code:
- `raftstore/snapshot/dir.go` — region-scoped directory format
- `raftstore/snapshot/meta.go` — interfaces and ImportResult
- `raftstore/snapshot/payload.go` — tar payload encode/decode
- `raftstore/migrate/{init,expand}.go` — call sites
- `engine/lsm/external_sst.go` — import path
- `db_snapshot.go` — DB-level wrapper

Physical layout:

```text
snapshot/
  snapshot.json     (region-scoped manifest)
  tables/
    000001.sst      (snapshot-specific SST, with values already materialized)
    000002.sst
```

Cross-node transport uses `archive/tar`. `writePayload(w io.Writer, ...)` walks the local snapshot dir while writing into `tw := tar.NewWriter(w)`, streaming via `io.Copy(tw, f)`. `unpackPayload(r io.Reader, dir string, ...)` is the reverse.

Properties already in place:
- region-scoped self-contained (snapshot.json + tables/*.sst)
- value-log independent (export materializes values inline)
- streaming export/import (no whole-payload buffering)
- staged install + rollback (`ImportResult.Rollback()`)
- temp-dir cleanup
- tar path safety (`secureSnapshotPath`)

## 2. The selling points the prior design note gave SnapshotSlab — checked one by one

| Claim (from the slab-substrate v1 note) | Status today | Still valid? |
|---|---|---|
| Physically isolated from main LSM | SST snapshot is already in independent per-region files | ❌ duplicate |
| Cross-node install via sendfile zero-copy | tar `io.Copy` already streams; sendfile gain is marginal | ❌ marginal |
| Region delete → snapshot delete, no GC needed | snapshot dir is temporary; cleaned right after install | ❌ duplicate |
| No independent GC required | same as above | ❌ duplicate |

**Every selling point in the v1 note is already covered by the SST snapshot install.** Adding a new consumer just to "use sendfile on a physically separated slab file" doesn't justify itself.

## 3. The actual niche where SnapshotSlab might still be useful

The spike did surface one use case the SST snapshot install does **not** cover:

### 3.1 fsmeta `SnapshotSubtree` → consumable storage artifact

`fsmeta/plan.go::PlanSnapshotSubtree` currently returns a `SnapshotSubtreeToken` (an MVCC read epoch). Subsequent reads still go through the LSM MVCC path. There is **no** ability to materialize a snapshot into a single-file artifact.

Potential demand:
- AI dataset checkpoint: a training job wants to freeze a dataset's directory state and export it as a single file (or multi-segment artifact) for training workers / remote caches to consume.
- Cross-cluster dataset replication: dump an fsmeta subtree into another cluster.
- Time-travel debug: materialize an epoch's subtree for offline analysis.

These are a different axis from raft snapshot install:
- raft snapshot: region-scoped, installed into the peer raft store.
- fsmeta snapshot artifact: subtree-scoped, exported for non-NoKV consumers.

### 3.2 Long-term archive

Right now raft snapshot is cleaned up immediately after install. If we want long-term archival (S3 lifecycle integration, compliance retention), we need a "persistent retention + index" mechanism. tar files suffice but lack structured metadata. SnapshotSlab could provide a sub-manifest index (slab id / class / owner / state / frontier / checksum / path), making it cleaner to integrate with archival systems.

## 4. Conclusion & recommendation

**v1 does not ship SnapshotSlab.** Reasons:

1. raft snapshot install is fully covered by the SST path.
2. fsmeta dataset artifact demand hasn't appeared yet; building it would create idle code.
3. If we do build it, SnapshotSlab's core value would be **fsmeta subtree materialization**, not physical isolation / sendfile, so we should design it when fsmeta proposes the export API.
4. Building SnapshotSlab today would create a vacillation between "SnapshotSlab vs SST snapshot install"; when fsmeta export demand actually appears later, the design might be hijacked by the existing "SnapshotSlab" naming reservation.

**Keep as a forward reference**: §6.2 of the slab substrate redesign note demotes SnapshotSlab to "future use case" with an explicit trigger: **open an independent RFC when fsmeta `SnapshotSubtree` needs to materialize into an export artifact**.

## 5. Two side roads left for the existing SST snapshot install

During the spike I noticed two possible follow-ups that **are not part of the slab substrate refactor** but are worth recording for future reference:

### 5.1 `ExportPayload` fully buffers

`ExportPayload(...)` uses `var payload bytes.Buffer` to fully buffer before returning. For very large snapshots this is memory pressure. `ExportPayloadTo(w io.Writer, ...)` is already the streaming variant; callers should prefer the `*To` form. This is a follow-up internal to SST snapshot install; not slab-related.

### 5.2 sendfile in `io.Copy(tw, f)`

`writePayload` does `io.Copy(tw, f)` where `tw` is `*tar.Writer`. `io.Copy`'s zero-copy fast paths come from `dst.ReadFrom(src)` or `src.WriteTo(dst)`; `*tar.Writer` does not implement `ReaderFrom`, so the file → conn sendfile path is **not** selected through this wrapper — the file is read into Go memory and pushed through `tar.Writer.Write`. So the current path is **not** already zero-copy under tar; treating it as such was wrong. If we ever care, the right shape is a tar-aware writer that exposes the body section directly to the conn (or skipping tar entirely for the body) so `ReadFrom` on the conn can take over. Marking this as a real follow-up to revisit, not a settled non-issue.

## 6. Decision log

- **Not building SnapshotSlab in v1**: the existing SST snapshot install already covers it.
- **Reserve the name with explicit trigger**: wait for fsmeta export artifact demand and open an independent RFC then; avoid the "build first, find a use case later" trap.
- **Mark `ExportPayload` full-buffer as follow-up**: independent of the slab refactor; owned by raftstore/snapshot.

## 7. Related notes

- `2026-04-27-slab-substrate.md` — slab substrate redefinition; this note is the conclusion of its Phase 4 spike.
- `2026-03-31-sst-snapshot-install.md` — existing SST snapshot install design.
- `2026-04-25-snapshot-subtree-mvcc-epoch.md` — fsmeta SnapshotSubtree epoch model; the future trigger source for SnapshotSlab.
