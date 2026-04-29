# Design Notes and Implementation Records

This section is NoKV's long-form documentation area.

The main documentation leans toward reference manual; `notes` is more like an engineering log and technical essay collection — its job is to explain:

- Why a particular boundary exists.
- Why an implementation had to be torn down and redone.
- Which "looks simple" approaches turned out to be wrong.
- What the codebase has learned along the way.

<div class="blog-hero">
  <div class="blog-hero-copy">
    <span class="masthead-kicker">Engineering log</span>
    <h2>How NoKV got built</h2>
    <p>These posts record design tradeoffs, implementation lessons, debugging traces, and the reasons behind the boundaries you find in the code.</p>
  </div>
  <div class="blog-hero-meta">
    <div class="tag-pill">Design</div>
    <div class="tag-pill">Implementation</div>
    <div class="tag-pill">Distributed systems</div>
    <div class="tag-pill">Storage kernel</div>
  </div>
</div>

## Recommended reading

<div class="blog-grid">
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-standalone-to-distributed-bridge.html">Bridging standalone and distributed</a></h3>
    <p>Why NoKV treats single-node and distributed as one system, and why migration must be a protocol rather than a dump/import tool.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-coordinator-and-execution-layering.html">Coordinator and execution-plane layering</a></h3>
    <p>Why control plane, truth kernel, and data-plane executor must be separated, and why Coordinator must not write local truth directly.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-migration-mode-and-snapshot.html">Mode and snapshot semantics in migration</a></h3>
    <p>Why migration is fundamentally a directory lifecycle protocol and snapshot layering — not a few extra CLI commands.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-distributed-testing-and-failpoints.html">Distributed testing and failpoints</a></h3>
    <p>Why NoKV uses live integration, testcluster, and narrow-boundary failpoints simultaneously — and how restrained failpoints should be.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-31</span>
    <h3><a href="2026-03-31-sst-snapshot-install.html">SST-based snapshot install</a></h3>
    <p>Why NoKV picked region-scoped, self-contained, vlog-independent SST snapshot install.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-03</span>
    <h3><a href="2026-04-03-delos-lite-metadata-root-roadmap.html">Rooted metadata, Delos-lite, and VirtualLog</a></h3>
    <p>Full description of NoKV's current metadata truth, Coordinator isolation, VirtualLog contract, local/replicated backends, and why this mainline is a good research platform.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-05</span>
    <h3><a href="2026-04-05-range-filter-from-grf.html">Range filter: inspired by GRF, not a clone of GRF</a></h3>
    <p>Why NoKV needs read-path pruning, what GRF actually contributes as inspiration, why we picked a more conservative in-memory advisory implementation, and how it relates to the LSM read path.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-12</span>
    <h3><a href="2026-04-12-coordinator-meta-separation.html">Separated deployment for Coordinator and meta/root</a></h3>
    <p>Boundaries of co-located vs separated control-plane deployment, TSO/ID window, Coordinator lease, freshness semantics, and the order to land them.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-24</span>
    <h3><a href="2026-04-24-fsmeta-positioning.html">fsmeta positioning: a metadata substrate for distributed filesystems</a></h3>
    <p>Why fsmeta does not take a JuiceFS TKV-compatibility route and instead makes metadata-native APIs NoKV's cloud-native niche.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-25</span>
    <h3><a href="2026-04-25-namespace-authority-events-umbrella.html">Namespace authority events umbrella</a></h3>
    <p>Unifies the RootEvent naming, payload, and runtime view boundaries across mount, subtree, snapshot, and quota primitives.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-04-25</span>
    <h3><a href="2026-04-25-snapshot-subtree-mvcc-epoch.html">SnapshotSubtree: subtree-scoped MVCC epoch</a></h3>
    <p>Why SnapshotSubtree publishes only a read epoch — it doesn't copy the directory tree and doesn't write dentry lists into meta/root.</p>
  </div>
</div>

## What belongs here

- Design tradeoffs that don't fit in the reference docs.
- Debugging records with concrete symptoms, wrong assumptions, and the eventual fix.
- Performance investigations with benchmark context and design explanation.
- Refactoring writeups that explain why a package boundary moved.

## Writing style

Each note should read like a small technical blog post but keep an engineering perspective:

1. Start from a concrete problem or design question.
2. Pin down the system boundary first.
3. Walk through tradeoffs and the rejected options.
4. Add diagrams, call chains, object relationships, and commands where useful.
5. Close by stating what the code already changed and what's still unresolved.

The point of `notes` is to explain design and evolution — not to act as a file index. Unless a particular module or file name is genuinely necessary for understanding, don't force-list code paths. What matters more than "where is this function" is "why this layering, why we rejected the alternative, where the boundary actually is".

For recent design-flavored notes, add a short TL;DR block at the top, including at least:

- 🧭 Topic
- 🧱 Core objects
- 🔁 Call chain
- 📚 References

## Suggested template

```md
# Title

## TL;DR

- 🧭 Topic:
- 🧱 Core objects:
- 🔁 Call chain:
- 📚 References:

## Why this matters

## Current system boundary

## Paths that look simple but are wrong

## The design we ended up with

## Key objects and boundaries

## Diagrams and call logic

## Design principles

## References

## What changed

## What we haven't solved
```

## Adding a new note

1. Create `docs/notes/YYYY-MM-DD-short-title.md`.
2. Add it to `docs/SUMMARY.md`.
3. Prioritize clear diagrams, object relationships, call chains, and design rationale — keep generic narration short.
4. `notes` are written in English; keep technical terms as-is, but don't ship half-finished mixed-language prose.
