# Testing & Validation Matrix

This document inventories NoKV's automated coverage and provides guidance for extending tests. It aligns module-level unit tests, integration suites, and benchmarking harnesses with the architectural features described elsewhere.

---

## 1. Quick Commands

```bash
# All unit + integration tests (uses local module caches)
GOCACHE=$PWD/.gocache GOMODCACHE=$PWD/.gomodcache go test ./...

# Focused distributed transaction suite
go test ./percolator/... ./raftstore/client/... -run 'Test.*(Commit|Prewrite|TwoPhaseCommit)'

# Focused distributed migration / membership / restart suite
go test ./raftstore/integration -count=1

# Crash recovery scenarios
RECOVERY_TRACE_METRICS=1 \
go test ./... -run 'TestRecovery(RemovesStaleValueLogSegment|CleansMissingSSTFromManifest|ManifestRewriteCrash|SlowFollowerSnapshotBacklog|SnapshotExportRoundTrip|WALReplayRestoresData)' -count=1 -v

# Protobuf schema hygiene
make proto-check

# gRPC transport chaos tests + watchdog metrics
CHAOS_TRACE_METRICS=1 \
go test -run 'TestGRPCTransport(HandlesPartition|MetricsWatchdog|MetricsBlockedPeers)' -count=1 -v ./raftstore/transport

# Sample PD-lite service for shared TSO / routing in distributed tests
go run ./cmd/nokv pd --addr 127.0.0.1:2379 --id-start 1 --ts-start 100 --workdir ./artifacts/pd

# Local three-node cluster (includes catalog bootstrap + PD-lite)
./scripts/run_local_cluster.sh --config ./raft_config.example.json
# Tear down with Ctrl+C

# Docker-compose sandbox (3 nodes + PD-lite)
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
| LSM / Flush / Compaction | `lsm/lsm_test.go`, `lsm/picker_test.go`, `lsm/planner_test.go`, `lsm/compaction_test.go`, `lsm/flush_runtime_test.go` | Memtable correctness, iterator merging, flush pipeline metrics, compaction scheduling. | Extend backpressure assertions and workload-shape coverage. |
| Manifest | `manifest/manager_test.go`, `lsm/manifest_test.go` | CURRENT swap safety, rewrite crash handling, vlog metadata persistence. | Simulate partial edit corruption, column family extensions. |
| ValueLog | `vlog/manager_test.go`, `vlog/io_test.go`, `vlog_test.go` | ValuePtr encoding/decoding, GC rewrite/rewind, concurrent iterator safety. | Long-running GC, discard-ratio edge cases. |
| Percolator / Distributed Txn | `percolator/*_test.go`, `raftstore/client/client_test.go`, `stats_test.go` | Prewrite/Commit/ResolveLock flows, 2PC retries, timestamp-driven MVCC behaviour, metrics accounting. | Mixed multi-region fuzzing with lock TTL and leader churn. |
| DB Integration | `db_test.go`, `db_write_bench_test.go` | End-to-end writes, recovery, and throttle behaviour. | Combine ValueLog GC + compaction stress, multi-DB interference. |
| CLI & Stats | `cmd/nokv/main_test.go`, `stats_test.go` | Golden JSON output, stats snapshot correctness, hot key ranking. | CLI error handling, expvar HTTP integration tests. |
| Redis Gateway | `cmd/nokv-redis/backend_embedded_test.go`, `cmd/nokv-redis/server_test.go`, `cmd/nokv-redis/backend_raft_test.go` | Embedded backend semantics (NX/XX, TTL, counters), RESP parser, raft backend config wiring, and PD-backed routing/TSO discovery. | End-to-end multi-region CRUD with raft backend, TTL lock cleanup under failures. |
| Scripts & Tooling | `cmd/nokv-config/main_test.go`, `cmd/nokv/serve_test.go` | `nokv-config` JSON/simple formats, catalog bootstrap CLI, serve bootstrap behavior. | Add direct shell-script golden tests (currently not present) and failure-path diagnostics for `run_local_cluster.sh`. |
| Distributed Migration & Membership | `raftstore/integration/*_test.go`, `raftstore/migrate/*_test.go`, `raftstore/admin/service_test.go` | Standalone -> seeded -> cluster flow, snapshot install, add/remove peer, leader transfer, restart/dehost recovery, PD outage after startup, quorum-loss context propagation, partitioned follower catch-up, and snapshot-install interruption before publish. | Keep expanding publish-boundary coverage and larger fault matrices around runtime/transport interleavings. |
| Benchmark | `benchmark/ycsb_test.go`, `benchmark/ycsb_runner.go` | YCSB throughput/latency comparisons across engines (A-F) with detailed percentile + operation mix reporting. | Automate multi-node deployments and add longer-running, multi-GB stability baselines. |

---

## 3. System Scenarios

| Scenario | Coverage | Focus |
| --- | --- | --- |
| Crash recovery | `db_test.go` | WAL replay, missing SST cleanup, vlog GC restart, manifest rewrite safety. |
| WAL pointer desync | `raftstore/engine/wal_storage_test.go::TestWALStorageDetectsTruncatedSegment` | Detects store-local raft pointer offsets beyond truncated WAL tails to avoid silent corruption. |
| Distributed transaction contention | `raftstore/client/client_test.go::TestClientTwoPhaseCommitAndGet`, `percolator/*_test.go` | Lock conflicts, retries, and 2PC sequencing under region routing. |
| Value separation + GC | `vlog/manager_test.go`, `db_test.go::TestRecoveryRemovesStaleValueLogSegment` | GC correctness, manifest integration, iterator stability. |
| Iterator consistency | `lsm/iterator_test.go` | Snapshot visibility, merging iterators across levels and memtables. |
| Throttling / backpressure | `lsm/compaction_test.go`, `db_test.go::TestWriteThrottle` | L0 backlog triggers, flush queue growth, metrics observation. |
| Distributed NoKV client | `raftstore/client/client_test.go::TestClientTwoPhaseCommitAndGet`, `raftstore/transport/grpc_transport_test.go::TestGRPCTransportManualTicksDriveElection` | Region-aware routing, NotLeader retries, manual tick-driven elections, cross-region 2PC sequencing. |
| Migration & membership orchestration | `raftstore/integration/migration_flow_test.go`, `raftstore/integration/restart_recovery_test.go`, `raftstore/integration/pd_degraded_test.go`, `raftstore/integration/snapshot_interruption_test.go`, `raftstore/integration/context_propagation_test.go`, `raftstore/integration/transport_chaos_test.go` | Seed bootstrap, multi-peer rollout, leader transfer, peer removal, restarted follower recovery, removed-peer dehost after restart, PD outage after startup, quorum-loss read/write timeouts, partitioned follower catch-up, transfer-leader retry after partition recovery, and snapshot-install interruption before publish. |
| Performance regression | `benchmark` package | Compare NoKV vs Badger/Pebble by default (RocksDB optional), produce human-readable reports under `benchmark/benchmark_results`. |

---

## 4. Observability in Tests

- **RECOVERY_METRIC logs** – produced when `RECOVERY_TRACE_METRICS=1`; helpful when triaging targeted recovery suites and CI failures.
- **TRANSPORT_METRIC logs** – emitted by transport chaos tests when `CHAOS_TRACE_METRICS=1`, capturing gRPC watchdog counters during network partitions and retries.
- **Stats snapshots** – `stats_test.go` verifies JSON structure so CLI output remains backwards compatible.
- **Benchmark artefacts** – stored under `benchmark/benchmark_results/*.txt` for historical comparison. Aligns with README instructions.

---

## 5. Extending Coverage

1. **Property-based testing** – integrate `testing/quick` or third-party generators to randomise distributed 2PC sequences (prewrite/commit/rollback ordering).
2. **Stress harness** – add a Go-based stress driver to run mixed read/write workloads for hours, capturing metrics akin to RocksDB's `db_stress` tool.
3. **Distributed readiness** – strengthen raftstore fault-injection and long-run tests (leader transfer, transport chaos, snapshot catch-up) with reproducible CI artifacts.
4. **CLI smoke tests** – simulate corrupted directories to ensure CLI emits actionable errors.

## 6. Distributed Test Layers

- **Protocol unit tests**: package-local tests under `raftstore/peer`, `raftstore/store`, `raftstore/admin`, `raftstore/snapshot`, and `raftstore/migrate` validate one protocol surface at a time.
- **Node-local integration tests**: store/admin tests verify snapshot install, membership application, and region runtime publication without booting a full cluster.
- **Multi-node deterministic integration tests**: `raftstore/integration` uses the shared `raftstore/testcluster` harness to boot real stores, wire transports, and drive migration/member flows against live runtimes.
- **Restart and recovery suites**: `raftstore/integration/restart_recovery_test.go` covers restarted followers, removed-peer dehost persistence, and leader restart with subsequent membership changes.
- **Control-plane degradation and publish-boundary tests**: `raftstore/integration/pd_degraded_test.go` and `raftstore/integration/snapshot_interruption_test.go` cover live PD outage after startup and failpoint-driven snapshot interruption before peer publication.

When adding new distributed tests, prefer reusing `raftstore/testcluster` instead of embedding cluster bootstrap helpers into feature-specific test files.

## 7. Distributed Fault Matrix

| Fault Class | Current Coverage | Primary Tests | Notes |
| --- | --- | --- | --- |
| Snapshot export/install failure | Covered | `raftstore/migrate/expand_test.go`, `raftstore/store/peer_lifecycle_test.go`, `raftstore/admin/service_test.go` | Covers leader export failure, target install failure, and corrupt payload rejection without partially hosted peers. |
| Membership wait timeouts | Covered | `raftstore/migrate/expand_test.go`, `raftstore/migrate/remove_peer_test.go`, `raftstore/migrate/transfer_leader_test.go` | Verifies timeout surfaces when leader metadata does not publish, target never hosts, peer removal never converges, or leader transfer stalls. |
| Follower restart after snapshot install | Covered | `raftstore/integration/restart_recovery_test.go::TestExpandedPeerRestartPreservesRegionAndData` | Ensures installed peer persists region metadata and data after restart. |
| Removed peer restart | Covered | `raftstore/integration/restart_recovery_test.go::TestRemovedPeerRestartDoesNotRehost` | Ensures dehosted peers do not come back after restart. |
| Leader restart with follow-up membership change | Covered | `raftstore/integration/restart_recovery_test.go::TestLeaderRestartStillAllowsMembershipChanges` | Exercises leadership churn before a later remove-peer operation. |
| Control-plane degraded / PD unavailable | Covered | `pd/adapter/region_sink_test.go`, `raftstore/store/command_service_test.go::TestStoreProposeCommandSurvivesSchedulerUnavailable`, `raftstore/integration/pd_degraded_test.go::TestClusterSurvivesPDUnavailableAfterStartup` | Covers both local degraded scheduler semantics and live multi-node PD outage after route cache warmup; new cold-route misses still fail with `RouteUnavailable` as expected. |
| Scheduler queue overflow / dropped operations | Covered | `raftstore/store/scheduler_runtime_test.go::TestStoreSchedulerStatusTracksQueueDrop` | Validates local degraded status and dropped operation accounting. |
| Snapshot install interrupted before publish | Covered | `raftstore/integration/snapshot_interruption_test.go::TestExpandSnapshotInstallInterruptedBeforePublish`, `raftstore/store/peer_lifecycle_test.go::TestStoreInstallRegionSnapshotRejectsCorruptPayload` | Uses failpoint injection to verify target install aborts without leaving a hosted peer or polluted region metadata, then retries cleanly after restart. |
| Request cancel / deadline propagation | Covered | `raftstore/client/client_test.go::TestClientGetHonorsCanceledContextDuringRouteLookup`, `raftstore/client/client_test.go::TestClientGetHonorsCanceledContextDuringRPC`, `raftstore/client/client_test.go::TestClientPutHonorsCanceledContextDuringRouteLookup`, `raftstore/client/client_test.go::TestClientPutHonorsCanceledContextDuringRPC`, `raftstore/integration/context_propagation_test.go::TestClientReadWriteHonorContextUnderQuorumLoss` | Verifies both read and write paths surface caller cancellation through route lookup and RPC boundaries, and that live quorum-loss reads/writes return deadline errors instead of hanging behind background contexts. |
| Transport partition / interleave recovery | Covered | `raftstore/transport/grpc_transport_test.go::TestGRPCTransportHandlesPartition`, `raftstore/integration/transport_chaos_test.go::TestPartitionedFollowerCatchesUpAfterRecovery`, `raftstore/integration/transport_chaos_test.go::TestTransferLeaderRecoversAfterPartitionedTargetReturns` | Covers low-level gRPC link blocking plus live cluster recovery after follower isolation/restart and transfer-leader timeout/retry under transport partitions. |
| Split/merge restart safety | Covered | `raftstore/store/store_test.go::TestStoreRestartPreservesSplitMergeLocalMeta`, `raftstore/integration/split_merge_recovery_test.go::TestSplitMergeRestartSafetyAcrossStores` | Covers store-local recovery plus live multi-store split -> restart -> merge -> restart flow after making split/merge admin replay idempotent across restart. |

Next fault-matrix additions should focus on:

- more publish-boundary failpoints around snapshot install and migration init
- deeper transport/interleave chaos beyond partition + recovery, especially concurrent membership changes under repeated link flaps

Keep this matrix updated when adding new modules or scenarios so documentation and automation remain aligned.
