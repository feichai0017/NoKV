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

# Build RocksDB locally (installs into ./third_party/rocksdb/dist by default)
./scripts/build_rocksdb.sh
# YCSB baseline (records=1e6, ops=1e6, warmup=1e5, conc=16)
./scripts/run_benchmarks.sh
# YCSB with RocksDB (requires CGO, `benchmark_rocksdb`, and the RocksDB build above)
LD_LIBRARY_PATH="$(pwd)/third_party/rocksdb/dist/lib:${LD_LIBRARY_PATH}" \
CGO_CFLAGS="-I$(pwd)/third_party/rocksdb/dist/include" \
CGO_LDFLAGS="-L$(pwd)/third_party/rocksdb/dist/lib -lrocksdb -lz -lbz2 -lsnappy -lzstd -llz4" \
YCSB_ENGINES="nokv,badger,rocksdb" ./scripts/run_benchmarks.sh
# One-click script (auto-detect RocksDB, supports `YCSB_*` env vars to override defaults)
./scripts/run_benchmarks.sh
# Quick smoke run (smaller dataset)
NOKV_RUN_BENCHMARKS=1 YCSB_RECORDS=10000 YCSB_OPS=50000 YCSB_WARM_OPS=0 \
./scripts/run_benchmarks.sh -ycsb_workloads=A -ycsb_engines=nokv
```

> Tip: Pin `GOCACHE`/`GOMODCACHE` in CI to keep build artefacts local and avoid permission issues.

---

## 2. Module Coverage Overview

| Module | Tests | Coverage Highlights | Gaps / Next Steps |
| --- | --- | --- | --- |
| WAL | `wal/manager_test.go` | Segment rotation, sync semantics, replay tolerance for truncation, directory bootstrap. | Add IO fault injection, concurrent append stress. |
| LSM / Flush / Compaction | `lsm/lsm_test.go`, `lsm/compaction_test.go`, `lsm/compact/*_test.go`, `lsm/flush/manager_test.go` | Memtable correctness, iterator merging, flush pipeline metrics, compaction scheduling. | Extend backpressure assertions, test cache hot/cold split. |
| Manifest | `manifest/manager_test.go`, `lsm/manifest_test.go` | CURRENT swap safety, rewrite crash handling, vlog metadata persistence. | Simulate partial edit corruption, column family extensions. |
| ValueLog | `vlog/manager_test.go`, `vlog/io_test.go`, `vlog_test.go` | ValuePtr encoding/decoding, GC rewrite/rewind, concurrent iterator safety. | Long-running GC with transactions, discard-ratio edge cases. |
| Transactions / Oracle | `txn_test.go`, `txn_iterator_test.go`, `stats_test.go` | MVCC timestamps, conflict detection, iterator snapshots, metrics accounting. | Mixed workload fuzzing, managed transactions with TTL. |
| DB Integration | `db_test.go`, `db_write_bench_test.go` | End-to-end writes, recovery, managed vs. unmanaged transactions, throttle behaviour. | Combine ValueLog GC + compaction stress, multi-DB interference. |
| CLI & Stats | `cmd/nokv/main_test.go`, `stats_test.go` | Golden JSON output, stats snapshot correctness, hot key ranking. | CLI error handling, expvar HTTP integration tests. |
| Redis Gateway | `cmd/nokv-redis/backend_embedded_test.go`, `cmd/nokv-redis/server_test.go`, `cmd/nokv-redis/backend_raft_test.go` | Embedded backend semantics (NX/XX, TTL, counters), RESP parser, raft backend config wiring & TSO discovery. | End-to-end multi-region CRUD with raft backend, TTL lock cleanup under failures. |
| Scripts & Tooling | `cmd/nokv-config/main_test.go`, `cmd/nokv/serve_test.go` | `nokv-config` JSON/simple formats, manifest logging CLI, serve bootstrap behavior. | Add direct shell-script golden tests (currently not present) and failure-path diagnostics for `run_local_cluster.sh`. |
| Benchmark | `benchmark/ycsb_test.go`, `benchmark/ycsb_runner.go` | YCSB throughput/latency comparisons across engines (A-G) with detailed percentile + operation mix reporting. | Automate multi-node deployments and add longer-running, multi-GB stability baselines. |

---

## 3. System Scenarios

| Scenario | Coverage | Focus |
| --- | --- | --- |
| Crash recovery | `db_test.go`, `scripts/recovery_scenarios.sh` | WAL replay, missing SST cleanup, vlog GC restart, manifest rewrite safety. |
| WAL pointer desync | `raftstore/engine/wal_storage_test.go::TestWALStorageDetectsTruncatedSegment` | Detects manifest pointer offsets beyond truncated WAL tails to avoid silent corruption. |
| Transaction contention | `TestConflict`, `TestTxnReadAfterWrite`, `TestTxnDiscard` | Oracle watermark handling, conflict errors, managed commit path. |
| Value separation + GC | `vlog/manager_test.go`, `db_test.go::TestRecoveryRemovesStaleValueLogSegment` | GC correctness, manifest integration, iterator stability. |
| Iterator consistency | `txn_iterator_test.go`, `lsm/iterator_test.go` | Snapshot visibility, merging iterators across levels and memtables. |
| Throttling / backpressure | `lsm/compaction_test.go`, `db_test.go::TestWriteThrottle` | L0 backlog triggers, flush queue growth, metrics observation. |
| Distributed TinyKv client | `raftstore/client/client_test.go::TestClientTwoPhaseCommitAndGet`, `raftstore/transport/grpc_transport_test.go::TestGRPCTransportManualTicksDriveElection` | Region-aware routing, NotLeader retries, manual tick-driven elections, cross-region 2PC sequencing. |
| Performance regression | `benchmark` package | Compare NoKV vs Badger/Pebble by default (RocksDB optional), produce human-readable reports under `benchmark/benchmark_results`. |

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
3. **Distributed readiness** – strengthen raftstore fault-injection and long-run tests (leader transfer, transport chaos, snapshot catch-up) with reproducible CI artifacts.
4. **CLI smoke tests** – simulate corrupted directories to ensure CLI emits actionable errors.

Keep this matrix updated when adding new modules or scenarios so documentation and automation remain aligned.
