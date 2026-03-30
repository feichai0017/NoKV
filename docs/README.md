# Overview

<div class="hero">
  <div class="hero-top">
    <div class="hero-brand">
      <div class="hero-logo">
        <img src="assets/logo.svg" alt="NoKV logo">
      </div>
      <div>
        <p class="hero-kicker">Distributed KV, Built As One System</p>
        <h1>NoKV</h1>
      </div>
    </div>
  </div>

  <p class="hero-lead">
    NoKV starts as a serious standalone engine and grows into a multi-Raft distributed
    KV database without swapping out its storage core. That is the hook: WAL, LSM,
    MVCC, migration, replication, and control-plane behavior are treated as one system,
    not a pile of loosely connected features.
  </p>

  <div class="hero-summary">
    <div class="hero-stat">
      <strong>Standalone to Cluster</strong>
      <span>Seed a distributed region from an existing workdir and keep the same storage substrate.</span>
    </div>
    <div class="hero-stat">
      <strong>Correctness First</strong>
      <span>Mode gates, logical region snapshots, recovery metadata, and execution/control-plane split.</span>
    </div>
    <div class="hero-stat">
      <strong>Tested as a System</strong>
      <span>Migration flow, restart recovery, PD degradation, transport chaos, and publish-boundary failpoints.</span>
    </div>
  </div>

  <div class="badge-row">
    <a href="https://github.com/feichai0017/NoKV/actions">
      <img alt="CI" src="https://img.shields.io/github/actions/workflow/status/feichai0017/NoKV/go.yml?branch=main" />
    </a>
    <a href="https://codecov.io/gh/feichai0017/NoKV">
      <img alt="Coverage" src="https://img.shields.io/codecov/c/gh/feichai0017/NoKV" />
    </a>
    <a href="https://goreportcard.com/report/github.com/feichai0017/NoKV">
      <img alt="Go Report Card" src="https://img.shields.io/badge/go%20report-A+-brightgreen" />
    </a>
    <a href="https://pkg.go.dev/github.com/feichai0017/NoKV">
      <img alt="Go Reference" src="https://img.shields.io/badge/go.dev-reference-007d9c?logo=go&logoColor=white" />
    </a>
    <a href="https://dbdb.io/db/nokv">
      <img alt="DBDB.io" src="https://img.shields.io/badge/dbdb.io-listed-2f80ed" />
    </a>
    <a href="https://deepwiki.com/feichai0017/NoKV">
      <img alt="DeepWiki" src="https://img.shields.io/badge/DeepWiki-Ask-6f42c1" />
    </a>
  </div>

  <div class="cta-row">
    <a class="cta-button primary" href="getting_started.html">Quick Start</a>
    <a class="cta-button secondary" href="architecture.html">Architecture</a>
    <a class="cta-button secondary" href="migration.html">Migration</a>
  </div>

  <p class="hero-footer">
    Built around Go, WAL + LSM storage, Percolator-style MVCC, multi-Raft replication,
    PD-lite control plane, and a formal standalone-to-cluster migration path.
  </p>
</div>

<div class="project-masthead">
  <div class="masthead-panel">
    <span class="masthead-kicker">What You Can Actually Do</span>
    <h3>Use NoKV in three different ways</h3>
    <ul>
      <li>Embed it locally through <code>NoKV.Open</code>.</li>
      <li>Start a multi-node cluster with <code>scripts/dev/cluster.sh</code>.</li>
      <li>Take an existing standalone workdir and migrate it into a replicated region.</li>
    </ul>
  </div>
  <div class="masthead-panel">
    <span class="masthead-kicker">What To Look For</span>
    <h3>What makes this project worth reading</h3>
    <ul>
      <li>One storage substrate instead of separate standalone and distributed engines.</li>
      <li>Formal lifecycle and migration protocol instead of dump/import glue.</li>
      <li>System-level verification under restart, degraded PD, chaos, and failpoints.</li>
    </ul>
  </div>
</div>

<span class="section-kicker">What Matters</span>

## Why NoKV

<div class="feature-grid">
  <div class="feature-card">
    <h3>One data plane, two deployment shapes</h3>
    <p>NoKV does not fork into separate standalone and distributed engines. The distributed layer grows on top of the same underlying DB workdir.</p>
    <small>That is why migration can be a protocol instead of a dump/import afterthought.</small>
  </div>
  <div class="feature-card">
    <h3>Replication with clear ownership</h3>
    <p><code>Store</code> owns the node runtime, <code>Peer</code> owns a region replica runtime, <code>RaftAdmin</code> is the execution plane, and PD stays in the control plane.</p>
    <small>The system avoids mixing local truth, local recovery metadata, and cluster control metadata.</small>
  </div>
  <div class="feature-card">
    <h3>Logical region snapshots</h3>
    <p>Raft durable snapshot metadata is split from logical region state snapshots, which keeps migration, add-peer install, and recovery semantics clean.</p>
    <small>This is a correctness-first design, not a one-shot performance shortcut.</small>
  </div>
  <div class="feature-card">
    <h3>System-level validation</h3>
    <p>The project is tested beyond unit semantics: migration flow, restart safety, degraded PD behavior, transport chaos, and context propagation are all exercised.</p>
    <small>The goal is to verify lifecycle and recovery behavior, not just happy-path RPCs.</small>
  </div>
</div>

<div class="benchmark-note">
  Benchmark methodology and result snapshots live in <a href="../benchmark/README.md"><code>../benchmark/README.md</code></a>. The docs site keeps architecture and operating guidance separate from benchmark storytelling.
</div>

<span class="section-kicker">Fastest Path</span>

## Try NoKV In Five Minutes

If you only want one practical loop, use this:

```bash
# 1. Start a local cluster from the shared topology file
./scripts/dev/cluster.sh --config ./raft_config.example.json

# 2. In another terminal, front it with the Redis-compatible gateway
go run ./cmd/nokv-redis \
  --addr 127.0.0.1:6380 \
  --raft-config ./raft_config.example.json

# 3. Talk to NoKV with any Redis client
redis-cli -p 6380 set hello world
redis-cli -p 6380 get hello
```

Then inspect what is happening:

```bash
go run ./cmd/nokv stats --expvar http://127.0.0.1:9100
go run ./cmd/nokv regions --workdir ./artifacts/cluster/store-1
```

<span class="section-kicker">Read This Next</span>

## Documentation Guide

If you only read three pages, read these first:

1. <a href="getting_started.html"><strong>Getting Started</strong></a> for the shortest path to a running cluster.
2. <a href="raftstore.html"><strong>Raftstore</strong></a> for runtime ownership and distributed boundaries.
3. <a href="migration.html"><strong>Migration</strong></a> for the standalone → cluster bridge that makes NoKV distinct.

<div class="doc-grid">
  <div class="doc-card">
    <h3><a href="getting_started.html">Getting Started</a></h3>
    <p>Run NoKV locally, understand the topology file, and boot your first store or local cluster.</p>
  </div>
  <div class="doc-card">
    <h3><a href="raftstore.html">Raftstore</a></h3>
    <p>Read the distributed runtime layout: server wiring, store ownership, peer lifecycle, snapshots, and recovery surfaces.</p>
  </div>
  <div class="doc-card">
    <h3><a href="migration.html">Migration</a></h3>
    <p>Follow the standalone → seeded → cluster path, including logical snapshot export/install and membership rollout.</p>
  </div>
  <div class="doc-card">
    <h3><a href="testing.html">Testing</a></h3>
    <p>See how deterministic integration, failpoints, restart recovery, and distributed fault matrix coverage are organized.</p>
  </div>
</div>

<span class="section-kicker">Choose Your Route</span>

## Read By Interest

<div class="path-grid">
  <div class="path-card">
    <h3>Storage Engine</h3>
    <p>Read this route if you care about WAL discipline, MemTable/flush, manifest semantics, and ValueLog GC.</p>
    <p><a href="architecture.html">Architecture</a> · <a href="wal.html">WAL</a> · <a href="flush.html">Flush</a> · <a href="vlog.html">Value Log</a></p>
  </div>
  <div class="path-card">
    <h3>Distributed Runtime</h3>
    <p>Read this route if Store/Peer ownership, transport, snapshots, and PD-lite are the parts you want to reason about.</p>
    <p><a href="raftstore.html">Raftstore</a> · <a href="pd.html">PD-lite</a> · <a href="runtime.html">Runtime</a></p>
  </div>
  <div class="path-card">
    <h3>Migration & Operations</h3>
    <p>Read this route if the bridge from standalone workdir to replicated region is the part you want to demo or operate.</p>
    <p><a href="migration.html">Migration</a> · <a href="scripts.html">Scripts</a> · <a href="cli.html">CLI</a></p>
  </div>
  <div class="path-card">
    <h3>Testing & Validation</h3>
    <p>Read this route if you want to see how NoKV verifies correctness under restart, degraded PD, chaos, and failpoint boundaries.</p>
    <p><a href="testing.html">Testing</a> · <a href="notes/README.html">Notes</a></p>
  </div>
</div>

<span class="section-kicker">Common Paths</span>

## Jump Points

<div class="quicklink-grid">
  <a class="quicklink" href="cli.html">CLI surface</a>
  <a class="quicklink" href="config.html">Topology config</a>
  <a class="quicklink" href="scripts.html">Scripts layout</a>
  <a class="quicklink" href="pd.html">PD-lite</a>
  <a class="quicklink" href="percolator.html">Percolator / MVCC</a>
  <a class="quicklink" href="runtime.html">Runtime call chains</a>
</div>

<span class="section-kicker">Layer View</span>

## Architecture Sketch

```mermaid
%%{init: {
  "themeVariables": { "fontSize": "18px" },
  "flowchart": { "nodeSpacing": 45, "rankSpacing": 62, "curve": "basis" }
}}%%
graph TD
    Client["Client / App / Redis"] -->|RPC / RESP| Server["Node Server"]
    Client -->|Route / TSO / control queries| PD["PD-lite"]

    subgraph "Distributed Runtime"
        Server --> Store["Store runtime root"]
        Store --> Peer["Peer runtime"]
        Store --> Admin["RaftAdmin"]
        Store --> Meta["Local recovery metadata"]
        Peer --> Raft["Raft durable state"]
        Peer --> Snap["Logical region snapshot"]
    end

    subgraph "Shared Data Plane"
        Peer --> DB["NoKV DB"]
        Snap --> DB
        DB --> LSM["LSM / WAL / VLog / MVCC"]
    end
```

<div class="arch-callout">
  The central design choice is simple: NoKV is not a separate standalone engine and distributed product glued together later. The distributed system is built over the same storage core, with migration and snapshot semantics made explicit instead of implicit.
</div>
