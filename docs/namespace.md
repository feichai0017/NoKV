# Namespace

The `namespace/` package provides **hierarchical, paginated listing** on top of the NoKV KV substrate. It is exposed at the top level as `DB.Namespace(...)` which returns a `NamespaceHandle`.

> This layer is optional. The core `DB` still uses flat keys; `namespace` only adds a path-aware listing surface for applications that need `Create` / `Lookup` / `List(parent)` semantics.

---

## 1. What it is, in one paragraph

Many workloads store data under path-like keys (`/a/b/c`, `bucket/object`, `run/epoch_N/shard_k`). A plain prefix scan over `M|full_path` answers `List(parent)` correctly, but pays for all descendants rather than just direct children, and offers no notion of "this page is complete vs in-flight". The namespace layer keeps `M|full_path` as the only source of truth and builds a **fence-paged read plane** alongside it, together with a tiny coverage state machine. Listing hits the read plane when the interval is certified; otherwise callers get an explicit `ErrCoverageIncomplete` and must run `RepairAndList`.

---

## 2. Key layout

Five key families live in the underlying KV:

| Family | Role |
|---|---|
| `M\|full_path` | Authoritative metadata (the only truth) |
| `LR\|parent` | Read-plane root: page directory + per-interval coverage state |
| `LP\|parent\|fence` | Ordered micro-pages (children in fence interval) |
| `LD\|parent\|shard\|child` | Bootstrap delta log (for not-yet-materialized parents) |
| `LDP\|parent\|page\|#seq` | Page-local delta log (for recent mutations on materialized parents) |

`M` is read on every `Lookup`. `LR/LP` are read on every strict `List`. `LD/LDP` are consumed by `RepairAndList` / `MaterializeDeltaPages`.

Encoding lives in [`namespace/codec.go`](../namespace/codec.go).

---

## 3. Three-state coverage model

Each page interval in `LR` carries a `PageCoverageState` (`coordinator/protocol/controlplane` terminology doesn't apply here — this is namespace-local):

- **Covered** — the interval has a published page, no pending delta, and generation matches `LR`'s root generation. Strict `List` is allowed.
- **Uncovered** — no page published yet, or publish is incomplete. Strict `List` returns `ErrCoverageIncomplete`.
- **Dirty** — a page exists but a page-local `LDP` entry has landed after publication. Must be folded before strict `List` can answer again.

Strict `List` only reads covered pages. Anything else forces the caller to choose between fail-stop and explicit `RepairAndList`.

---

## 4. API surface

All methods are on `DB.Namespace(...)` which wraps [`namespace.Store`](../namespace/store.go).

### Writes

```go
h := db.Namespace(NamespaceOptions{Shards: 16})
defer h.Close()

h.Create([]byte("/a/b/file"), namespace.EntryKindFile, metaBytes)
h.Delete([]byte("/a/b/file"))
```

`Create` / `Delete` update `M` first, then append to the appropriate delta family:

- If parent is **not yet materialized**: write goes to `LD|parent|shard|child` (bootstrap delta)
- If parent **is materialized**: write goes to `LDP|parent|page|#seq` (page-local delta) and marks the affected page as `Dirty`

### Point reads

```go
meta, err := h.Lookup([]byte("/a/b/file"))
```

`Lookup` is a direct `M|full_path` read. It never touches the read plane.

### Listing

```go
// Strict — fails if interval is uncovered/dirty
entries, next, stats, err := h.List(parent, cursor, limit)

// Explicit repair then list — allowed to consult M + deltas to fix the interval
entries, next, stats, err := h.RepairAndList(parent, cursor, limit)
```

`stats` reports whether the result came from the strict read plane or from repair, and how many pages/deltas were touched.

### Maintenance

```go
// Fold all page-local deltas back into pages (global)
stats, err := h.Materialize(parent)

// Fold up to N dirty pages (bounded work)
stats, err := h.MaterializeDeltaPages(parent, maxDeltaPages)

// Rebuild entire read plane from M (expensive, rare)
stats, err := h.Rebuild(parent)

// Return internal read plane view for diagnostics
view, ok, err := h.LoadReadPlaneView(parent)

// Runtime stats for observability
ls, err := h.Stats(parent)
```

### Verification

```go
vs, err := h.Verify(parent)
```

`Verify` walks `M`, `LR`, `LP`, `LD`, `LDP` and reports:

- **Membership drift** — children in `M` that are missing from pages, or vice versa
- **Certificate inconsistency** — page generation mismatch, wrong fence keys, wrong counts
- **Publication mismatch** — root generation inconsistent with pages' generations, frontier rollback, root-page cohort mismatch

These three reports live under `VerifyStats.Membership` / `.Certificate` / `.Publication`.

---

## 5. Why not just prefix scan `M|`?

Prefix scan on `M|parent/...` works correctly but has two cost issues:

1. **It reads all descendants.** If `parent` has 1K direct children but each has 100 grandchildren, the scan touches 100K keys to answer a 1K-item `List`.
2. **It has no answerability state.** There is no way to express "we currently have a complete, consistent page ready to serve" vs "we're mid-rewrite". Callers have to trust whatever comes back.

The read plane costs extra writes (dual-writing `M` + `LDP`/`LD`) and extra maintenance (`Materialize` / repair). In exchange, `List` becomes page-granular, and the coverage state machine gives you a clean place to fail-stop.

---

## 6. Cost vs. a durable parent-child secondary index

A simpler alternative is a plain durable index: `S|parent|child -> {}` updated on every `Create`/`Delete`. That gives you fast `List` without fences, pages, or coverage state — but every `List` is best-effort: if the index is slightly stale or mid-rebuild, the caller either gets partial data silently or has to re-derive from `M`.

`namespace/` pays more maintenance cost in exchange for:

- **Page granularity** — deltas and repairs are scoped to one fence interval, not the whole parent
- **Explicit coverage** — strict `List` never silently falls back; the caller decides whether to repair

If you don't need that distinction, a flat secondary index is simpler and probably faster.

---

## 7. Example

```go
package main

import (
    "fmt"

    NoKV "github.com/feichai0017/NoKV"
    ns "github.com/feichai0017/NoKV/namespace"
)

func main() {
    opt := NoKV.NewDefaultOptions()
    opt.WorkDir = "./ns-demo"
    db, _ := NoKV.Open(opt)
    defer db.Close()

    h := db.Namespace(NoKV.NamespaceOptions{Shards: 16})
    defer h.Close()

    h.Create([]byte("/a/b/c"), ns.EntryKindFile, []byte(`{"kind":"file"}`))
    h.Create([]byte("/a/b/d"), ns.EntryKindFile, nil)

    // Materialize the parent so strict List becomes possible.
    _, _ = h.Materialize([]byte("/a/b"))

    entries, next, stats, err := h.List([]byte("/a/b"), ns.Cursor{}, 100)
    if err != nil {
        fmt.Println("list err:", err)
        return
    }
    fmt.Printf("got %d entries, read_pages=%d, next=%+v\n",
        len(entries), stats.ReadPlanePages, next)
}
```

---

## 8. Source map

| File | Responsibility |
|---|---|
| [`namespace/store.go`](../namespace/store.go) | Main `Store` with `Create`/`Delete`/`Lookup`/`List`/`RepairAndList`/`Materialize` |
| [`namespace/readplane.go`](../namespace/readplane.go) | `ReadRoot`, `ReadPage`, `PageCertificate`, strict-read evaluator |
| [`namespace/delta.go`](../namespace/delta.go), [`namespace/delta_state.go`](../namespace/delta_state.go) | `LD` / `LDP` encoding + per-page delta state |
| [`namespace/page_merge.go`](../namespace/page_merge.go) | Fold page-local deltas into pages; split/merge fences |
| [`namespace/codec.go`](../namespace/codec.go) | Key + value encoding for all five families |
| [`namespace/types.go`](../namespace/types.go) | `Entry`, `Cursor`, `*Stats` public types |
| [`namespace.go`](../namespace.go) (root) | Thin `DB.Namespace` wrapper exposing the module as a first-class handle |

Tests: `namespace/*_test.go` cover create/delete/list, repair transitions, dirty-page handling, bootstrap, materialize, rebuild, and verification.
