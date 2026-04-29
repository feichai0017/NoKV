# Summary

- [Overview](README.md)
- [Getting Started](getting_started.md)

# Architecture

- [Architecture](architecture.md)
- [Runtime Call Chains](runtime.md)
- [Control and Execution Plane Protocols](control_and_execution_protocols.md)

# Storage Engine

- [WAL](wal.md)
- [Memtable](memtable.md)
- [Flush](flush.md)
- [Compaction](compaction.md)
- [Landing Buffer](landing_buffer.md)
- [Value Log](vlog.md)
- [Manifest](manifest.md)
- [VFS](vfs.md)
- [File](file.md)
- [Cache](cache.md)
- [Range Filter](range_filter.md)
- [Thermos](thermos.md)
- [Entry](entry.md)
- [Error Handling](errors.md)

# Distributed Runtime

- [Raftstore](raftstore.md)
- [Coordinator](coordinator.md)
- [Rooted Truth (meta/root)](rooted_truth.md)
- [Percolator](percolator.md)
- [FSMetadata](fsmeta.md)
- [Migration](migration.md)
- [Recovery](recovery.md)

# Operations & Tooling

- [Configuration](config.md)
- [CLI](cli.md)
- [eunomia-audit](eunomia-audit.md)
- [Cluster Demo](demo.md)
- [Scripts](scripts.md)
- [Stats & Observability](stats.md)
- [Testing](testing.md)

# Design Notes

- [Design Notes and Implementation Records](notes/README.md)
  - [2026-01-16 The mmap choice](notes/2026-01-16-mmap-choice.md)
  - [2026-01-16 Thermos design](notes/2026-01-16-thermos-design.md)
  - [2026-02-01 Compaction and Landing](notes/2026-02-01-compaction-and-landing.md)
  - [2026-02-05 Value Log design and GC](notes/2026-02-05-vlog-design-and-gc.md)
  - [2026-02-09 Memory Kernel: Arena and Adaptive Index](notes/2026-02-09-memory-kernel-arena-and-adaptive-index.md)
  - [2026-02-09 Write Pipeline: MPSC and Adaptive Batching](notes/2026-02-09-write-pipeline-mpsc-and-adaptive-batching.md)
  - [2026-02-15 VFS abstraction and deterministic reliability](notes/2026-02-15-vfs-abstraction-and-deterministic-reliability.md)
  - [2026-03-30 Bridging standalone and distributed](notes/2026-03-30-standalone-to-distributed-bridge.md)
  - [2026-03-30 Coordinator and execution-plane layering](notes/2026-03-30-coordinator-and-execution-layering.md)
  - [2026-03-30 Mode and snapshot semantics in migration](notes/2026-03-30-migration-mode-and-snapshot.md)
  - [2026-03-30 Distributed testing and failpoints](notes/2026-03-30-distributed-testing-and-failpoints.md)
  - [2026-03-31 SST-based snapshot install](notes/2026-03-31-sst-snapshot-install.md)
  - [2026-04-03 Rooted Metadata, Delos-lite, and VirtualLog](notes/2026-04-03-delos-lite-metadata-root-roadmap.md)
  - [2026-04-05 Range Filter: inspired by GRF, not a clone of GRF](notes/2026-04-05-range-filter-from-grf.md)
  - [2026-04-12 Separated deployment for Coordinator and meta/root](notes/2026-04-12-coordinator-meta-separation.md)
  - [2026-04-24 fsmeta positioning: a metadata substrate for distributed filesystems](notes/2026-04-24-fsmeta-positioning.md)
  - [2026-04-25 Namespace Authority Events Umbrella](notes/2026-04-25-namespace-authority-events-umbrella.md)
  - [2026-04-25 SnapshotSubtree: subtree-scoped MVCC epoch](notes/2026-04-25-snapshot-subtree-mvcc-epoch.md)
