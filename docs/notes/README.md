# Notes & Essays

This section is the long-form writing side of NoKV.

The main docs are a reference manual. The notes section is closer to an
engineering blog: design tradeoffs, implementation lessons, debugging
narratives, and the reasons behind the architectural boundaries in the code.

<div class="blog-hero">
  <div class="blog-hero-copy">
    <span class="masthead-kicker">Engineering Journal</span>
    <h2>How NoKV is being built</h2>
    <p>These posts explain why certain boundaries exist, what broke during implementation, what looked like an easy shortcut, and what the codebase learned from it.</p>
  </div>
  <div class="blog-hero-meta">
    <div class="tag-pill">Design</div>
    <div class="tag-pill">Implementation</div>
    <div class="tag-pill">Distributed Systems</div>
    <div class="tag-pill">Storage Internals</div>
  </div>
</div>

## Featured Posts

<div class="blog-grid">
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-standalone-to-distributed-bridge.html">Standalone to Distributed Bridge</a></h3>
    <p>Why NoKV treats standalone and distributed mode as one system, and why migration had to become a protocol instead of a dump/import tool.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-pd-and-raftadmin-layering.html">PD and RaftAdmin Layering</a></h3>
    <p>Why control plane and execution plane are deliberately separated, and why PD should not become a writer of local truth.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-migration-mode-and-snapshot.html">Migration Mode and Snapshot</a></h3>
    <p>The migration story is really about lifecycle and snapshot semantics, not just adding more CLI commands.</p>
  </div>
  <div class="blog-card">
    <span class="blog-date">2026-03-30</span>
    <h3><a href="2026-03-30-distributed-testing-and-failpoints.html">Distributed Testing and Failpoints</a></h3>
    <p>Why NoKV uses both live integration tests and narrow boundary failpoints, and how to keep failpoint usage disciplined.</p>
  </div>
</div>

## What belongs here

- design decisions that deserve more narrative than the reference docs
- debugging writeups with a concrete symptom, failed assumptions, and final fix
- performance investigations with benchmark setup and code-path analysis
- refactor notes that explain how package boundaries changed and why

## Writing style

Treat each note as a small technical blog post:

1. Start with the concrete problem or design question.
2. Show the system boundary involved.
3. Explain the tradeoffs and rejected alternatives.
4. Include diagrams, code references, and command snippets where useful.
5. End with what changed in the codebase and what still remains open.

## Suggested post template

```md
# Title

## Why this matters

## The system boundary

## What looked easy but was wrong

## The design we chose

## Code path

## Diagram

## What this changes

## What remains unsolved
```

## Add a new post

1. Create `docs/notes/YYYY-MM-DD-short-title.md`.
2. Add it to `docs/SUMMARY.md`.
3. Prefer diagrams, code snippets, and explicit file references over generic prose.
