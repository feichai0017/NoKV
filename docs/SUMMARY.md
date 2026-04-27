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
- [Staging Buffer](staging_buffer.md)
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
- [Cluster Demo & Dashboard](demo.md)
- [Scripts](scripts.md)
- [NoKV Redis](nokv-redis.md)
- [Stats & Observability](stats.md)
- [Testing](testing.md)

# Design Notes

- [设计笔记与实现记录](notes/README.md)
  - [2026-01-16 mmap 选择](notes/2026-01-16-mmap-choice.md)
  - [2026-01-16 Thermos 设计](notes/2026-01-16-thermos-design.md)
  - [2026-02-01 Compaction 与 Staging](notes/2026-02-01-compaction-and-staging.md)
  - [2026-02-05 Value Log 设计与 GC](notes/2026-02-05-vlog-design-and-gc.md)
  - [2026-02-09 Memory Kernel：Arena 与 Adaptive Index](notes/2026-02-09-memory-kernel-arena-and-adaptive-index.md)
  - [2026-02-09 Write Pipeline：MPSC 与 Adaptive Batching](notes/2026-02-09-write-pipeline-mpsc-and-adaptive-batching.md)
  - [2026-02-15 VFS 抽象与确定性可靠性](notes/2026-02-15-vfs-abstraction-and-deterministic-reliability.md)
  - [2026-03-30 从单机到分布式的桥接](notes/2026-03-30-standalone-to-distributed-bridge.md)
  - [2026-03-30 Coordinator 与执行面分层](notes/2026-03-30-coordinator-and-execution-layering.md)
  - [2026-03-30 迁移里的 mode 与 snapshot 语义](notes/2026-03-30-migration-mode-and-snapshot.md)
  - [2026-03-30 分布式测试与 failpoint](notes/2026-03-30-distributed-testing-and-failpoints.md)
  - [2026-03-31 基于 SST 的 Snapshot Install](notes/2026-03-31-sst-snapshot-install.md)
  - [2026-04-03 Rooted Metadata、Delos-lite 与 VirtualLog](notes/2026-04-03-delos-lite-metadata-root-roadmap.md)
  - [2026-04-05 Range Filter：从 GRF 得到启发，但不照搬 GRF](notes/2026-04-05-range-filter-from-grf.md)
  - [2026-04-12 Coordinator 和 meta/root 分离部署设计](notes/2026-04-12-coordinator-meta-separation.md)
  - [2026-04-24 fsmeta 定位：面向分布式文件系统的元数据底座](notes/2026-04-24-fsmeta-positioning.md)
  - [2026-04-25 Namespace Authority Events Umbrella](notes/2026-04-25-namespace-authority-events-umbrella.md)
  - [2026-04-25 SnapshotSubtree：subtree-scoped MVCC epoch](notes/2026-04-25-snapshot-subtree-mvcc-epoch.md)
