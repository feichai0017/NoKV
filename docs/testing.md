# Testing & Validation Matrix

This document inventories NoKV's automated coverage and provides guidance for extending tests. It aligns module-level unit tests, integration suites, and benchmarking harnesses with the architectural features described elsewhere.

---

## 1. Quick Commands

```bash
# All unit + integration tests (uses local module caches)
GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache go test ./...

# Focused transaction suite
go test ./... -run '^TestTxn|TestConflict|TestTxnIterator'

# Crash recovery scenarios
RECOVERY_TRACE_METRICS=1 ./scripts/recovery_scenarios.sh

# gRPC transport chaos tests + watchdog metrics
CHAOS_TRACE_METRICS=1 ./scripts/transport_chaos.sh

# Sample timestamp allocator (TSO) for multi-client transaction tests
go run ./scripts/tso --addr 127.0.0.1:9494 --start 100

# Local three-node cluster (includes manifest bootstrap + optional TSO)
./scripts/run_local_cluster.sh --config ./raft_config.example.json
# Tear down with Ctrl+C

# Docker-compose sandbox (3 nodes + TSO)
docker compose up --build
docker compose down -v

# Performance baseline (NoKV vs Badger, optional RocksDB)
go test ./benchmark -run TestBenchmarkResults -count=1
# With RocksDB comparison (requires CGO and gorocksdb)
go test -tags benchmark_rocksdb ./benchmark -run TestBenchmarkResults -count=1
# True cold-start run (drops OS caches; requires sudo privileges)
go test ./benchmark -run TestBenchmarkResults -count=1 -- \
  -mode cold -drop_cache sudo
```

> Tip: Pin `GOCACHE`/`GOMODCACHE` in CI to keep build artefacts local and avoid permission issues.

---

## 2. Module Coverage Overview

| Module | Tests | Coverage Highlights | Gaps / Next Steps |
| --- | --- | --- | --- |
| WAL | `wal/manager_test.go` | Segment rotation, sync semantics, replay tolerance for truncation, directory bootstrap. | Add IO fault injection, concurrent append stress. |
| LSM / Flush / Compaction | `lsm/lsm_test.go`, `lsm/compact_test.go`, `lsm/flush/*_test.go` | Memtable correctness, iterator merging, flush pipeline metrics, compaction scheduling. | Extend backpressure assertions, test cache hot/cold split. |
| Manifest | `manifest/manager_test.go`, `manifest/levels_test.go` | CURRENT swap safety, rewrite crash handling, vlog metadata persistence. | Simulate partial edit corruption, column family extensions. |
| ValueLog | `vlog/vlog_test.go`, `vlog/gc_test.go` | ValuePtr encoding/decoding, GC rewrite, concurrent iterator safety. | Long-running GC with transactions, discard ratio edge cases. |
| Transactions / Oracle | `txn_test.go`, `txn_iterator_test.go`, `txn_metrics_test.go` | MVCC timestamps, conflict detection, iterator snapshots, metrics accounting. | Mixed workload fuzzing, managed transactions with TTL. |
| DB Integration | `db_test.go`, `db_recovery_test.go`, `db_recovery_managed_test.go` | End-to-end writes, recovery, managed vs. unmanaged transactions, throttle behaviour. | Combine ValueLog GC + compaction stress, multi-DB interference. |
| CLI & Stats | `cmd/nokv/main_test.go`, `stats_test.go` | Golden JSON output, stats snapshot correctness, hot key ranking. | CLI error handling, expvar HTTP integration tests. |
| Redis Gateway | `cmd/nokv-redis/backend_embedded_test.go`, `cmd/nokv-redis/server_test.go`, `cmd/nokv-redis/backend_raft_test.go` | Embedded backend semantics (NX/XX, TTL, counters), RESP parser, raft backend config wiring & TSO discovery. | End-to-end multi-region CRUD with raft backend, TTL lock cleanup under failures. |
| Scripts & Tooling | `scripts/scripts_test.go`, `cmd/nokv-config/main_test.go` | `serve_from_config.sh` address scoping (host/docker) and manifest skipping, `nokv-config` JSON/simple formats, manifest logging CLI. | Golden coverage for `run_local_cluster.sh`, failure-path diagnostics. |
| Benchmark | `benchmark/benchmark_test.go`, `benchmark/rocksdb_benchmark_test.go` | Comparative throughput/latency vs Badger/RocksDB across workloads. | Add long-tail latency reporting, multi-threaded contention. |

---

## 3. System Scenarios

| Scenario | Coverage | Focus |
| --- | --- | --- |
| Crash recovery | `db_recovery_test.go`, `scripts/recovery_scenarios.sh` | WAL replay, missing SST cleanup, vlog GC restart, manifest rewrite safety. |
| WAL pointer desync | `raftstore/engine/wal_storage_test.go::TestWALStorageDetectsTruncatedSegment` | Detects manifest pointer offsets beyond truncated WAL tails to avoid silent corruption. |
| Transaction contention | `TestConflict`, `TestTxnReadAfterWrite`, `TestTxnDiscard` | Oracle watermark handling, conflict errors, managed commit path. |
| Value separation + GC | `vlog/gc_test.go`, `db_recovery_test.go::TestRecoveryRemovesStaleValueLogSegment` | GC correctness, manifest integration, iterator stability. |
| Iterator consistency | `txn_iterator_test.go`, `lsm/iterator_test.go` | Snapshot visibility, merging iterators across levels and memtables. |
| Throttling / backpressure | `lsm/compact_test.go`, `db_test.go::TestWriteThrottle` | L0 backlog triggers, flush queue growth, metrics observation. |
| Distributed TinyKv client | `raftstore/client/client_test.go::TestClientTwoPhaseCommitAndGet`, `raftstore/transport/grpc_transport_test.go::TestGRPCTransportManualTicksDriveElection` | Region-aware routing, NotLeader retries, manual tick-driven elections, cross-region 2PC sequencing. |
| Performance regression | `benchmark` package | Compare NoKV vs Badger/RocksDB, produce human-readable reports under `benchmark/benchmark_results`. |

---

## 4. Observability in Tests

- **RECOVERY_METRIC logs** – produced when `RECOVERY_TRACE_METRICS=1`; consumed by recovery script and helpful when triaging CI failures.
- **TRANSPORT_METRIC logs** – emitted by `scripts/transport_chaos.sh` when `CHAOS_TRACE_METRICS=1`, capturing gRPC watchdog counters during network partitions and retries.
- **Stats snapshots** – `stats_test.go` verifies JSON structure so CLI output remains backwards compatible.
- **Benchmark artefacts** – stored under `benchmark/benchmark_results/*.txt` for historical comparison. Aligns with README instructions.

---

## 5. Extending Coverage

1. **Property-based testing** – integrate `testing/quick` or third-party generators to randomise transaction sequences (Badger uses similar fuzz tests for transaction ordering).
2. **Stress harness** – add a Go-based stress driver to run mixed read/write workloads for hours, capturing metrics akin to RocksDB's `db_stress` tool.
3. **Distributed readiness** – when Raft or replication is introduced, craft tests that validate WAL shipping combined with manifest updates.
4. **CLI smoke tests** – simulate corrupted directories to ensure CLI emits actionable errors.

Keep this matrix updated when adding new modules or scenarios so documentation and automation remain aligned.
