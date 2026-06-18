<!--
Copyright 2024-2026 The NoKV Authors.
SPDX-License-Identifier: Apache-2.0
-->

# AI-Native DFS Progress

This is the implementation ledger for the AI-native NoKV DFS direction. Each
step records the shipped state, validation evidence, and the next concrete gap.

## Direction

NoKV optimizes for AI training throughput first:

- checkpoint publish is a native generation/version operation;
- dataset reads use shard catalogs, batch opens, range plans, and hot NVMe
  cache;
- POSIX remains a compatibility profile, not the throughput center;
- metadata HA uses shard-owner leases, epoch fencing, checkpoint images, and a
  logical shared log;
- local NVMe placement stays out of metadata truth.

## Status Table

| Phase | Status | Evidence | Next Gap |
| --- | --- | --- | --- |
| P0 docs and stale-claim cleanup | Done | `docs/metadata-ha-fast-path.md`, this ledger, README links and stale-claim cleanup | Keep this ledger updated after every implementation step |
| P1 AI catalog foundation | Foundation Done | Existing `RecordFamily::PathIndex`, same-version batch `OpenPathReadPlan` | Add durable dataset/checkpoint catalog records only when scan/retention shape requires it |
| P2 Training API | Large-Batch Read Done | `OpenPathReadPlanBatch`, chunked `MetadataClient::open_path_read_plan_batch`, `NoKvFsClient::read_paths` with distinct-file parallel object reads, and `NoKvFsClient::read_path_ranges` with coalesced shard range reads | Add dataset snapshot and checkpoint catalog RPCs |
| P3 `nokv-control` | Live Etcd Smoke Passed | `crates/nokv-control` in-memory store, optional `Server::open_with_control`, automatic server lease renewal, optional `EtcdControlStore` with durable shard record plus lease-backed session keys, `nokv serve --control-backend etcd` config wiring, and live Docker etcd HA smoke | Add CI/prod env gate and document multi-node runbook |
| P4 epoch fencing | Stale-Owner Chaos Smoke Passed | `NoKvFs` owner epoch fence rejects stale commits; server guard installs and renews control lease epochs; `scripts/run-metadata-ha-smoke.sh` verifies epoch-2 failover and stale-owner write rejection through CLI against RustFS plus etcd | Add multi-machine failover timing and network-partition hardening |
| P5 logical shared log | Grouped Sync ACK Foundation Done | `MetadataLogEntry` chain digest, `MetadataLogSegment` durable codec, object-store segment archive/load, restore-time replay, sync archive hook, grouped independent-batch segment archive, and server `LogRef` publish before RPC ACK | Move append ahead of Holt apply when Holt exposes prepare/apply split; add async flush policy and durability benchmark rows |
| P6 failover | Local Chaos Gate Passed | Controlled server failover acquires a bumped epoch, restores checkpoint, replays shared log, starts serving, continues logging, `nokv-control` has etcd session TTL primitives, server config has an env-gated etcd session-expiry smoke, and `scripts/run-metadata-ha-smoke.sh` covers deployable RustFS+etcd owner failover, replay overwrite checks, timing metrics, and SIGSTOP/SIGCONT stale-owner fencing | Add true multi-machine chaos gate |
| P7 benchmarks | Mounted P4 Cold Lead | Existing AI workloads cover checkpoint/training/native layout; `ai-dataset-batch-read` exercises SDK batch layout-open plus parallel object reads; `ai-shard-range-read` now uses `NoKvFsClient::read_path_ranges_batch` so a training batch can batch-open all shard range windows before parallel object reads; `crates/nokv-python` exposes that primitive to Python/fsspec without rebuilding range planning above POSIX; `NoKvFsClient::read_path_ranges_batch_packed` and `read_ranges_batch_packed` expose a contiguous per-shard return shape for future dataloader/GPU staging, though current standard Python rows show no warm-path speedup from packed bytes alone; `NoKvFsClient::read_path_ranges_batch_into` and `read_ranges_batch_into` let callers reuse a contiguous staging buffer, and `nokv.ReadBuffer` now provides a Rust-owned reusable staging buffer that the SDK fills while the Python GIL is released; `ReadBuffer` now sits behind an explicit staging-memory allocator boundary and supports `memory_kind="system"` plus Unix `memory_kind="page_locked"` host pages; `PreparedPathRangeBatch` and Python `RangeBatchPlan` let dataloaders reuse native normalized range requests, packed output layout, coalesced read windows, and ordered metadata batch-open requests before repeated `ReadBuffer` reads while still batch-opening metadata each read for generation fencing; the prepared executor now borrows static path/offset/window layout during the hot loop instead of cloning owned range-batch tasks per read; `RangeBatchReader` packages the prepared plan with a reusable NoKV-owned staging buffer for long-lived dataloader workers; `ReadBuffer.export()` adds a read-only export token that blocks resize/refill while views are alive under the current `abi3-py39` package; exact single-range windows use object executor direct-write into staging buffers; guarded scatter direct-write is enabled only when it does not expand the physical block plan, and cache-aware scatter direct-fill now splits coalesced gap windows only when every semantic range is already hot in the block cache; `bench/drivers/native_fsspec_bench.py` emits canonical `boundary=L1` rows for that Python native path and now records SDK object/cache/prefetch/read-plan/data-fabric deltas plus `read_buffer_memory_kind`, `range_batch_plan=true`, and `range_batch_reader=true`; `scripts/run-python-fsspec-smoke.sh` builds the Python package, verifies live RustFS-backed fsspec batch reads for range, packed, bytearray, `ReadBuffer`, `RangeBatchPlan`, `RangeBatchReader`, staging memory-kind, page-locked host memory when available, and active-export guard shapes, and writes optional L1 CSV; `scripts/run-ai-dataplane-layered-matrix.sh` now combines Rust SDK L1, Python/fsspec L1, and mounted L2 NoKV/JuiceFS rows without merging boundaries or cases; standard layered evidence now separates native SDK headroom from mounted NoKV/JuiceFS rows; FUSE read handles keep a small local read-plan cache seeded from published full-file plans, reducing p=4 sparse-window pressure on the shared read-plan cache; object read pipeline now admits 1MiB segment warmup after a proven sparse-forward sample stream, trading extra background bytes for far fewer foreground object GETs in AI shard reads; memory block cache now installs exact-range aliases for repeated sparse hot reads without duplicating 16KiB sample bytes; hot benchmark rows report NoKV FUSE-read coverage so kernel-cache-served medians are separated from real NoKV hot-path diagnostics; FUSE read-only open reuses cached attrs and no-prefetch handles can bypass the read-pipeline state machine; mounted L2 rows still separate POSIX planned pread windows from requests that reach NoKV; macOS direct-io large reads still split into 16KiB FUSE callbacks, so FUSE remains a compatibility path | Add CUDA host registration/RDMA registration and deeper Rust-native dataloader iteration; add true memoryview only behind Python 3.11+ or non-abi3 build |

## Step Log

### 2026-06-13 P0 Start

- Created the active Codex goal for AI-native DFS implementation.
- Re-read the repository code contract and PR checklist.
- Confirmed the worktree was clean on `main`.
- Started architecture and progress docs.
- Added `docs/metadata-ha-fast-path.md`.
- Added this progress ledger.
- Removed README claims that described removed OpenRaft/`nokv-cluster` support as
  current shipped behavior.
- Linked the HA/fast-path design from README documentation.

### 2026-06-13 P3 Control Store Foundation

- Added `crates/nokv-control`.
- Added `ShardRecord`, `ShardLease`, checkpoint/log pointer types, and
  `ShardState`.
- Added `ControlStore` and `InMemoryControlStore`.
- Covered fresh acquire, duplicate owner rejection, failover epoch bump, stale
  lease rejection, mark-serving, and release behavior with unit tests.

### 2026-06-13 P1/P2 Training Batch Open

- Added `OpenPathReadPlanRequest` and `NoKvFs::open_path_read_plan_batch`.
- Added `OpenPathReadPlanBatch` metadata RPC request/result.
- Added `MetadataClient::open_path_read_plan_batch`.
- Added `NoKvFsClient::read_paths` for training-style multi-file reads through
  one metadata batch-open RPC.
- Kept the implementation on the existing path index and immutable read-plan
  pipeline; no new metadata truth or compatibility shim was introduced.
- Empty batch-open/read-path requests return locally without a metadata
  read-version lookup or RPC.

### 2026-06-13 P5 Logical Log Entry

- Added `MetadataLogEntry`, `MetadataLogError`, and
  `METADATA_LOG_ZERO_DIGEST` in `nokv-meta`.
- Added SHA-256 digest sealing over shard id, epoch, LSN, previous digest,
  `MetadataCommand`, and `CommitResult`.
- Added digest verification and chain-following checks for shard id, LSN,
  previous digest, and epoch regression.
- Kept this as a logical command log; it does not expose Holt's private WAL.

### 2026-06-13 P5 Logical Log Segment Codec

- Added `MetadataLogSegment`.
- Added durable binary segment encoding for shard id, epoch/LSN range, boundary
  digests, segment digest, and full logical `MetadataCommand` entries.
- Added segment decoding with command validation, entry digest verification, and
  segment header/digest checks.
- Added `metadata_log_replay_entries` to validate segment continuity after a
  checkpoint LSN/digest and return the ordered logical entries to replay.
- At this point the segment is still only an in-memory durable codec; object
  archive and control-plane pointer publication are added in the next step.

### 2026-06-13 P5 Logical Log Segment Archive

- Added `MetadataLogArchiveConfig`.
- Added `MetadataLogSegmentArchiveOutcome`.
- Added `NoKvFs::archive_metadata_log_segment` to write encoded logical log
  segments to the object store under deterministic digest/LSN keys, then read
  the object back and verify decode equality.
- Added `NoKvFs::load_metadata_log_segment` for recovery-path segment reads.
- Extended server shard-owner state with checkpoint/log pointer fields and
  `durable_lsn`.
- Added `Server::publish_shard_owner_log_ref` so a server owner can publish a
  `nokv-control::LogRef` after a segment is archived.
- Remaining P5 work at this point was restore-time command apply and a sync
  shared-log ACK mode.

### 2026-06-13 P5 Restore-Time Log Replay

- Added `MetadataLogRestoreOutcome`.
- Added `NoKvFs::restore_metadata_with_log_segments` for checkpoint install
  plus ordered logical replay through the metadata service commit boundary.
- Added `NoKvFs::restore_metadata_with_archived_log_segments` to load segment
  objects before replay.
- Segment continuity is verified against checkpoint LSN/digest before the
  checkpoint image is installed.
- Replay uses `NoKvFs::commit_metadata`, so owner-epoch fencing is not bypassed
  during recovery.
- Tightened `MetadataLogEntry` sealing: result commit version, applied mutation
  count, and watch event count must exactly match the command.
- Remaining P5 gap at this point was the production `sync-shared-log` ACK path
  that archives a segment and publishes `LogRef` before acknowledging metadata
  visibility.

### 2026-06-13 P5 Sync Shared-Log ACK Foundation

- Added `MetadataLogSyncConfig` and `MetadataLogSyncSnapshot`.
- Added `NoKvFs::enable_sync_metadata_log`, `disable_sync_metadata_log`, and
  `sync_metadata_log_snapshot`.
- The service commit boundary now archives a one-command logical log segment for
  every successful single metadata command when sync logging is enabled.
- Independent metadata batches archive all successful commands as one ordered
  segment, preserving LSN order and the digest chain.
- Restore-time replay uses `commit_metadata_without_sync_log` so recovery does
  not append replayed entries back into the shared log.
- `ServerShardOwnerOptions` can enable a shared-log archive prefix through
  `ServerSharedLogOptions`.
- Controlled servers initialize the sync log from the current control-plane
  durable LSN/digest, preserving the digest chain across failover.
- Metadata RPC writes publish the latest archived `LogRef` to `nokv-control`
  before returning a successful ACK. Read RPCs do not touch the control plane.
- `nokv-control::mark_serving` now preserves existing checkpoint/log refs when
  the caller does not replace them, avoiding startup-time recovery pointer loss.
- The current sync mode archives after Holt local apply and before RPC ACK.
  Moving append before Holt apply requires a later Holt prepare/apply split.

### 2026-06-13 P5 Grouped Sync Log Segments

- Added `NoKvFs::record_committed_metadata_commands` for multi-command sync-log
  archive.
- Independent metadata batches now archive successful commands as one ordered
  segment instead of one object per command.
- Successes in a batch return ACK errors if segment archive fails; failed
  commands keep their original errors.
- Added recovery coverage proving one grouped segment replays both batch-created
  files.
- This reduces sync shared-log object PUT amplification for training-style
  batch metadata operations.

### 2026-06-13 P7 Metadata Durability Benchmark Workload

- Added `nokv-bench --workload metadata-durability-batch`.
- The workload times batch file creation across many shard directories, which
  exercises the AI-training metadata fast path rather than POSIX rename-heavy
  compatibility paths.
- It emits two comparable phases over the same shape: `local-only` and
  `sync-shared-log`.
- The sync phase starts a controlled server with an in-memory owner store and
  shared-log ACK mode.
- Benchmark rows now expose `metadata_log_segments_archived`,
  `metadata_log_entries_archived`, and `metadata_log_archive_bytes` so grouped
  segment amplification is visible without overloading ordinary file-object
  counters.
- The CSV caveat explicitly says this is not etcd quorum HA.

### 2026-06-13 P6 Controlled Failover Smoke

- Added server failover restore wiring for `ServerShardAcquisition::Failover`.
- A failover owner now opens a fresh metadata service, restores the checkpoint
  referenced by the control record, replays the referenced post-checkpoint log
  segment, and only then marks the shard serving.
- Controlled server startup preserves checkpoint/log refs and validates that the
  restored checkpoint key and durable LSN match the control record.
- `run_manual_backup` now publishes a `CheckpointRef` when the server owns a
  shard, using the current sync-log LSN/digest as the checkpoint replay
  boundary.
- Added an in-process `ConfiguredObjectStore::Memory` variant so server smoke
  tests can share object/checkpoint/log state without fake S3 failures.
- Added a server RPC smoke that writes before checkpoint, backs up, writes after
  checkpoint, starts a failover owner, verifies both paths are visible, and then
  writes again through the new owner.

### 2026-06-13 P4 Owner Epoch Fence

- Added a service-local required owner epoch to `NoKvFs`.
- Added `install_owner_epoch`, `observe_required_owner_epoch`, and
  `required_owner_epoch`.
- Added stale-owner rejection before single-command commits, independent-batch
  commits, and allocator reservation commits.
- Added typed `StaleOwnerEpoch` errors through metadata service, server wire
  encoding, and client wire decoding.
- This is the commit-boundary fence foundation. The server shard-owner loop now
  drives these epoch methods during automatic lease renewal.

### 2026-06-13 P3/P4 Server Shard Owner Guard

- Added optional `Server::open_with_control`.
- Added `ServerShardOwnerOptions`, `ServerShardAcquisition`, and
  `ServerShardOwnerState`.
- Server startup can acquire a fresh shard lease or fail over from a previous
  epoch, install the lease epoch into `NoKvFs`, and mark the shard serving.
- `Server::renew_shard_owner_lease` renews the current lease. If the lease is
  stale, the server observes the newer control-plane epoch so metadata commits
  are fenced.
- Server stats now report shard-owner state when the guard is enabled.
- Default `Server::open` stays single-node and leaves the guard disabled.

### 2026-06-13 P3/P4 Server Shard Owner Auto Renewal

- Added `ServerShardOwnerRenewalOptions`.
- Controlled server opens now start a background shard-owner renewal worker by
  default.
- The renewal worker uses the same stop/join lifecycle pattern as GC and
  metadata backup workers.
- Renewal success clears the worker error state; stale renewals observe the
  newer control-plane epoch and fence later metadata commits from the old owner.
- Server stats now include shard-owner renewal iterations and last error.

### 2026-06-13 P3/P6 Etcd Control Store Backend

- Added `EtcdControlStoreOptions` for endpoint list, key prefix, and owner lease
  TTL.
- Added serde-based `ShardRecord` durable encoding with an explicit codec
  version.
- Added optional `nokv-control/etcd` feature and `EtcdControlStore`.
- The etcd backend stores durable shard records under
  `/nokv/control/shards/{hex(shard_id)}` and ephemeral owner sessions under
  `/nokv/control/sessions/{hex(shard_id)}/{epoch}/{lease_id}`.
- Durable shard records are not lease-attached, so checkpoint/log pointers and
  durable LSN survive owner loss.
- Fresh acquire, failover acquire, mark-serving, and release use etcd
  transactions with shard-record revision checks and session-key lease checks.
- Renew validates both durable owner state and the lease-backed session key
  before issuing lease keepalive.
- This step was library-level control-plane support. Server config/CLI wiring
  and local multi-process HA smoke were added in later steps.

### 2026-06-14 P3/P6 Etcd Server/CLI Wiring

- Added `nokv-server/etcd` and `nokv/etcd` features that forward to
  `nokv-control/etcd`.
- Added `ServerControlOptions` and `ServerControlStoreOptions` so
  `Server::open` can construct an etcd-backed control store from startup
  options.
- Kept `Server::open_with_control` for tests and direct in-process wiring.
- Added `nokv` CLI options for `--control-backend etcd`,
  `--control-etcd-endpoints`, key prefix, lease TTL, shard id, node id,
  failover epoch, owner renewal, and sync shared-log prefix.
- Shared-log CLI configuration now requires a control backend.
- Added an env-gated `nokv-server --features etcd` smoke that opens the first
  owner through configured etcd options, lets its lease-backed session expire,
  and opens a failover owner at the next epoch.
- Fixed FUSE errno mapping for owner-epoch fencing: stale owners return
  `ESTALE`; invalid owner epoch remains an internal `EIO` path with logging.

### 2026-06-14 P6 Local Metadata HA Smoke Script

- Added `scripts/run-metadata-ha-smoke.sh`.
- The script starts temporary RustFS and, when `NOKV_HA_ETCD_ENDPOINTS` is not
  provided, temporary etcd.
- It builds `nokv` with `--features etcd`, starts owner A with etcd control,
  sync shared-log, and checkpoint archive options, writes data before and after
  checkpoint, kills owner A, waits for lease expiry, and starts owner B with
  `--failover-from-epoch 1`.
- The pass condition verifies owner B stats show `node-b` at epoch 2, both
  checkpoint-restored and shared-log-replayed objects are readable, a new write
  succeeds without clobbering replayed data, and `fsck` reports no dangling
  objects.
- The script can use an external etcd cluster through `NOKV_HA_ETCD_ENDPOINTS`;
  otherwise it requires an `etcd` binary.

### 2026-06-14 P6 Replay Allocator High-Water Fix

- A live RustFS+Docker-etcd HA smoke exposed an allocator recovery bug:
  checkpoint restore plus shared-log replay restored `/runs/post.bin` at
  `inode=1026`, then owner B reused `inode=1026` for `/after-failover`.
- Fixed replay recovery to fold inode high-water from replayed metadata commands
  before refreshing allocator state, instead of relying only on the checkpoint's
  older allocator reservation record.
- Added a metadata-service regression test for checkpoint-plus-replay followed
  by a new write.
- Strengthened the server failover test and HA smoke script to re-read replayed
  data after the post-failover write.

### 2026-06-14 P6/P7 HA Timing Metrics

- `scripts/run-metadata-ha-smoke.sh` now emits `HA_SMOKE_METRICS` JSON on
  success and can persist the same JSON through `NOKV_HA_METRICS_JSON`.
- The metrics include `failover_observed_ms` from owner A kill to owner B ready,
  `lease_wait_ms`, `owner_b_startup_ms`, `verify_after_ready_ms`, checkpoint
  commit version, replayed inode, and post-failover inode.
- The script also has an explicit inode monotonicity assertion:
  `/after-failover` must allocate an inode greater than the replayed
  `/runs/post.bin` inode.
- This turns the local HA smoke into a benchmark-ingestible RTO row while still
  keeping the caveat that the local single-run number is not a production SLA.

### 2026-06-14 P4/P6 Stale-Owner Chaos Smoke

- `scripts/run-metadata-ha-smoke.sh` now has `NOKV_HA_STALE_OWNER_CHAOS=1`.
- In that mode owner A and owner B use separate metadata server binds. The
  script pauses owner A with `SIGSTOP`, waits for the etcd lease to expire,
  starts owner B with `--failover-from-epoch 1`, then resumes owner A with
  `SIGCONT`.
- The pass condition requires owner A's renewal worker to observe the epoch-2
  control record and reject a stale write with `owner epoch 1 is stale; required
  owner epoch is 2`.
- The same run still verifies checkpoint restore, shared-log replay,
  post-failover inode monotonicity, replayed data readability, and `fsck`.
- This is a local process-stall chaos gate. It is not a substitute for a
  multi-machine network partition test.

### 2026-06-14 P2/P7 SDK Batch Data Read

- `NoKvFsClient::read_paths` now keeps one metadata batch-open RPC and reads
  distinct file plans concurrently through the object data path.
- The per-file read pipeline remains ordered by `path#generation`; duplicate
  pipeline keys in the same batch fall back to sequential execution to avoid
  sharing mutable readahead state between workers.
- The implementation uses a private bounded worker chunk and does not add a new
  public tuning knob.
- `ServiceBenchClient::read_paths` now calls the SDK batch path, so
  `ai-dataset-batch-read` measures real batch layout-open plus parallel object
  reads instead of benchmark-side `read_path` loops.
- Added a client regression test that observes concurrent object `get_many`
  calls for two distinct sample files in one training batch.

### 2026-06-14 P2/P7 SDK Packed-Shard Range Read

- Added `NoKvFsClient::read_path_ranges` for packed shard reads.
- The SDK coalesces adjacent or overlapping byte ranges into one layout-open and
  object-read window, then slices the returned bytes back into the caller's
  original range order.
- Range windows share the normal per-file `path#generation` read pipeline and
  preserve generation fencing: if the caller does not pass an expected
  generation, the first opened generation is reused for later windows.
- `ServiceBenchClient::read_ranges` now calls this SDK path, so
  `ai-shard-range-read` measures product range coalescing instead of
  benchmark-side per-sample `read_path` loops.
- Added a client regression test that proves two contiguous ranges on one shard
  issue one metadata open and one object batch get.

### 2026-06-14 P2/P7 Large Training Batch Layout-Open

- `MetadataClient::open_path_read_plan_batch` now chunks large training batches
  at the same `MAX_BATCH_RPC_REQUESTS` boundary used by other metadata batch
  calls.
- The method validates that each metadata response returns one plan per request
  chunk and preserves the original result order across chunks.
- This keeps `NoKvFsClient::read_paths` usable for large dataloader batches
  without sending one oversized metadata RPC.
- Added a client regression test with `129` read-plan requests, proving the SDK
  fetches two metadata chunks and returns all plans in order.

### 2026-06-14 P7 Sparse Shard Gap-Coalescing Benchmark

- Added benchmark-only `--range-stride` and `--range-coalesce-gap-bytes`.
- `ai-shard-range-read` can now model sparse packed-shard sample readers by
  selecting every Nth sample while leaving the shard payload unchanged.
- The benchmark passes the configured gap budget to
  `NoKvFsClient::read_path_ranges`, exposing the tradeoff between fewer object
  GETs and extra bytes read across skipped samples.
- No product metadata, RPC, or client API surface was added in this step.

### 2026-06-14 P2/P7 SDK Shard Read-Ahead

- `NoKvFsClient::read_path_ranges` now schedules one coalesced range window of
  read-ahead after the first path open pins the shard inode and generation.
- The read-ahead path uses the existing `ReadBodyPlan` RPC, block cache,
  object prefetcher, and singleflight read coordinator. It does not add a
  metadata command, RPC type, public tuning knob, or alternate cache layer.
- Foreground reads still open each range window through the path API with the
  pinned expected generation, so a replaced shard is still rejected by the
  existing stale-body-generation fence.
- Added a client regression test that observes the sequence:
  foreground `OpenPathReadPlan`, next-window `ReadBodyPlan` with the pinned
  generation, then foreground `OpenPathReadPlan` for the next window.

### 2026-06-14 P2/P7 SDK Shard Read-Ahead Admission

- Added an internal read-ahead admission check for packed-shard range windows.
- Next-window read-ahead now requires at least `DEFAULT_BLOCK_SIZE / 4` bytes,
  matching the object pipeline's existing small-inner-read scale.
- This keeps 512-byte sparse sample smoke reads on the direct foreground path
  while retaining generation-fenced read-ahead for MB-scale shard windows.
- Added client regression coverage for both paths: small next windows skip the
  speculative `ReadBodyPlan`, while large next windows still prefetch through
  the pinned inode and generation.

### 2026-06-14 P7 Large-Window Read-Ahead Policy Smoke

- Reused the existing `ai-shard-range-read` workload to validate the read-ahead
  admission boundary on MB-scale shard windows.
- The smoke uses `--sample-bytes 1048576 --range-stride 32`, so each shard has
  two selected 1 MiB windows and the second window is eligible for generation-
  fenced body-plan prefetch.
- This is a policy smoke for `prefetch_enqueued` and cache-hit behavior. It is
  not a full throughput-limit run because the smoke profile still writes only
  eight shard files and selects sixteen samples total.

### 2026-06-14 P7 AI Shard Range Matrix Script

- Added `scripts/run-ai-shard-range-matrix.sh` as the reusable NoKV/RustFS
  matrix for packed-shard range reads.
- The matrix runs three isolated cases through disposable RustFS instances:
  `small-exact`, `small-gap`, and `large-window`.
- `small-exact` keeps 512-byte sparse windows unmerged, `small-gap` validates
  gap coalescing across skipped 512-byte samples, and `large-window` validates
  read-ahead admission on 1 MiB selected windows.
- `scripts/run-rustfs-e2e.sh` now honors `NOKV_E2E_CARGO_TARGET_DIR` by passing
  it to Cargo as `CARGO_TARGET_DIR`, so matrix runs can reuse or isolate build
  artifacts intentionally.
- The matrix script now writes per-case raw logs plus one combined CSV with a
  leading `matrix_case` column. `NOKV_AI_SHARD_MATRIX_OUTPUT_DIR` controls the
  artifact directory and `NOKV_AI_SHARD_MATRIX_CSV` can pin the CSV path.

### 2026-06-14 P7 AI Data-Plane L2 Runner

- Added `scripts/run-ai-dataplane-l2.sh` as the focused mounted NoKV-vs-JuiceFS
  AI data-plane gate.
- The wrapper reuses `scripts/run-fs-benchmark.sh` and narrows the L2 surface to
  product workloads `checkpoint,training_read,ai_shard_range_read`, with
  primitive workloads and real tools skipped by default.
- `bench/drivers/posix_bench.py` now has an L2 `ai_shard_range_read` workload.
  It seeds packed shard files through the mount, reads selected sample offsets
  with POSIX `pread`, emits cold/warm rows, and records physical pread windows,
  physical bytes, and read amplification in `cost_breakdown`.
- `scripts/run-fs-benchmark.sh` now has `--skip-real-tools`, so narrow product
  gates do not have to run `fio`, `mdtest`, or `juicefs bench`.
- `scripts/fs-bench-env.py` records the `real_tools` mode plus the
  packed-shard `range_stride` and `range_coalesce_gap_bytes` in the environment
  JSON, making full matrix runs and focused AI data-plane runs distinguishable
  from the artifact alone.

## Validation Commands

### 2026-06-13 P3 Control Store Foundation

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-control-target cargo test -p nokv-control`: passed
  with 5 unit tests.

### 2026-06-13 P1/P2 Training Batch Open

- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client open_path_read_plan_batch`: passed with the protocol and metadata batch-open tests selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-server rpc_open_read_plan_batch_returns_one_result_per_request`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-client metadata_client_open_read_plan_batch_returns_plans`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-client service_file_client_read_paths_uses_single_batch_open`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.

### 2026-06-13 P5 Logical Log Entry

- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-meta log::tests`: passed with 5 unit tests.
- `git diff --check`: passed.

### 2026-06-13 P5 Logical Log Segment Codec

- `cargo fmt --all -- --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta log::tests`: passed with 10 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo clippy -p nokv-meta --all-targets -- -D warnings`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client open_path_read_plan_batch`: passed.

### 2026-06-13 P5 Logical Log Segment Archive

- `cargo fmt --all -- --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta metadata_log_segment`: passed with 2 service-level archive tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta log::tests`: passed with 10 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server shard_owner_log_ref_publish_updates_control_record`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server`: passed with 43 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-control`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo clippy -p nokv-meta -p nokv-server -p nokv-control --all-targets -- -D warnings`: passed.

### 2026-06-13 P5 Restore-Time Log Replay

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta log::tests`: passed with 11 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta restore_metadata_with`: passed with 3 selected service tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta metadata_log_segment`: passed with 2 service-level archive tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo clippy -p nokv-meta --all-targets -- -D warnings`: passed.

### 2026-06-13 P5 Sync Shared-Log ACK Foundation

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-meta sync_metadata_log_archives_commit_before_recovery_ack`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-meta restore_metadata_with`: passed with 3 selected service tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-meta metadata_log_segment`: passed with 2 service-level archive tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-control mark_serving_preserves_recovery_refs_when_not_replaced`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-server rpc_write_publishes_sync_shared_log_before_ack`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-server shard_owner_log_ref_publish_updates_control_record`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo clippy -p nokv-meta -p nokv-control -p nokv-server --all-targets -- -D warnings`: passed.

### 2026-06-13 P6 Controlled Failover Smoke

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-server controlled_failover_restores_checkpoint_and_replays_shared_log`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-server controlled_`: passed with 3 selected controlled-owner tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-server rpc_write_publishes_sync_shared_log_before_ack`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-meta restore_metadata_with`: passed with 3 selected service tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-control mark_serving_preserves_recovery_refs_when_not_replaced`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo test -p nokv-object`: passed with 94 unit tests and doc tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p6-target cargo clippy -p nokv-object -p nokv-meta -p nokv-control -p nokv-server --all-targets -- -D warnings`: passed.

### 2026-06-13 P5 Grouped Sync Log Segments

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo test -p nokv-meta sync_metadata_log_archives_independent_batch_as_one_segment`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo test -p nokv-meta sync_metadata_log_archives_commit_before_recovery_ack`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo test -p nokv-meta restore_metadata_with`: passed with 3 selected service tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo test -p nokv-server rpc_write_publishes_sync_shared_log_before_ack`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo clippy -p nokv-meta --all-targets -- -D warnings`: passed.

### 2026-06-13 P7 Metadata Durability Benchmark Workload

- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo test -p nokv-bench`: passed with 82 tests across both benchmark binaries.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p7-target cargo clippy -p nokv-meta -p nokv-server -p nokv-bench --all-targets -- -D warnings`: passed.
- Local RustFS smoke using `/opt/homebrew/bin/rustfs` passed for
  `metadata-durability-batch` smoke profile: `local-only` 512 ops in
  0.040948 s (12503.61 ops/s); `sync-shared-log` 512 ops in 0.197752 s
  (2589.11 ops/s), with 4 archived metadata log segments, 512 archived entries,
  and 287396 encoded archive bytes. This is a local L1 smoke row, not a
  JuiceFS/L2 comparison.

### 2026-06-13 P4 Owner Epoch Fence

- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-meta owner_epoch`: passed with 3 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-server rpc_reports_stale_owner_epoch_as_typed_error`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-client metadata_client_preserves_stale_owner_epoch_error`: passed.

### 2026-06-13 P4 Combined Gate

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-control-target cargo test -p nokv-control`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-meta owner_epoch`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-server rpc_reports_stale_owner_epoch_as_typed_error`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-client metadata_client_preserves_stale_owner_epoch_error`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-meta log::tests`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client open_path_read_plan_batch`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo clippy -p nokv-control -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client --all-targets -- -D warnings`: passed.
- After clippy cleanup, `cargo fmt --all -- --check`, `git diff --check`, `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-meta owner_epoch`, `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-server rpc_reports_stale_owner_epoch_as_typed_error`, and `CARGO_TARGET_DIR=/tmp/nokv-p4-target cargo test -p nokv-client metadata_client_preserves_stale_owner_epoch_error` passed.

### 2026-06-13 P3/P4 Server Shard Owner Guard

- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server controlled_`: passed with 2 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server stale_owner_renew_observes_new_epoch_and_fences_commits`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-control`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server rpc_reports_stale_owner_epoch_as_typed_error`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta owner_epoch`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-meta log::tests`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client open_path_read_plan_batch`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-client service_file_client_read_paths_uses_single_batch_open`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo clippy -p nokv-control -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client --all-targets -- -D warnings`: passed.

### 2026-06-13 P3/P4 Server Shard Owner Auto Renewal

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server shard_owner_auto_renewal`: passed with 2 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server controlled_`: passed with 2 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-server`: passed with 42 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo test -p nokv-control`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p5-target cargo clippy -p nokv-control -p nokv-server --all-targets -- -D warnings`: passed.

### 2026-06-13 P3/P6 Etcd Control Store Backend

- `cargo fmt --all -- --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-target cargo test -p nokv-control`: passed
  with 10 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-etcd-target cargo check -p nokv-control --features etcd`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-etcd-target cargo test -p nokv-control --features etcd`: passed
  with 10 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-target cargo clippy -p nokv-control --all-targets -- -D warnings`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-etcd-target cargo clippy -p nokv-control --features etcd --all-targets -- -D warnings`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-target cargo test -p nokv-server controlled_`: passed
  with 3 selected controlled-owner/failover tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p8-target cargo test -p nokv-server shard_owner_auto_renewal`: passed
  with 2 selected renewal tests.
- `git diff --check`: passed.

### 2026-06-14 P3/P6 Etcd Server/CLI Wiring

- `CARGO_TARGET_DIR=/tmp/nokv-p9-target cargo test -p nokv parse_`: passed
  with 20 selected CLI parser tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-target cargo test -p nokv-server controlled_`: passed
  with 3 selected controlled-owner/failover tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-target cargo test -p nokv-server shard_owner_auto_renewal`: passed
  with 2 selected renewal tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-target cargo test -p nokv-server`: passed
  with 45 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-target cargo test -p nokv-fuse`: passed
  with 42 unit tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-etcd-target cargo test -p nokv-server --features etcd configured_etcd_control_store_expires_session_and_allows_failover`: passed.
  `NOKV_ETCD_ENDPOINTS` was not set in this local environment, so this covered
  the feature build and env-gated skip path rather than a live etcd failover.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-etcd-target cargo check -p nokv --features etcd`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-etcd-target cargo test -p nokv --features etcd parse_`: passed
  with 20 selected CLI parser tests.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-target cargo clippy -p nokv-server -p nokv -p nokv-fuse --all-targets -- -D warnings`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p9-etcd-target cargo clippy -p nokv-server -p nokv --features etcd --all-targets -- -D warnings`: passed.

### 2026-06-14 P6 Local Metadata HA Smoke Script

- `bash -n scripts/run-metadata-ha-smoke.sh`: passed.
- `scripts/run-metadata-ha-smoke.sh --help`: passed.
- `scripts/run-metadata-ha-smoke.sh` missing-dependency path: passed by exiting
  with status 127 and `error: required command not found: etcd`.
- `CARGO_TARGET_DIR=/tmp/nokv-p10-etcd-target cargo check -p nokv --features etcd`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cd docs && npm run build`: passed.
- No local `etcd` binary was found, so the live RustFS+etcd failover body of
  the script was not executed in this environment.

### 2026-06-14 P6 Replay Allocator High-Water Fix

- `CARGO_TARGET_DIR=/tmp/nokv-p11-target cargo test -p nokv-meta restore_metadata_with_sync_log_advances_allocator_after_replay`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p11-target cargo test -p nokv-server controlled_failover_restores_checkpoint_and_replays_shared_log`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p11-etcd-target cargo check -p nokv --features etcd`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- Live RustFS+Docker-etcd smoke passed with:
  `NOKV_HA_ETCD_ENDPOINTS=http://127.0.0.1:12379 NOKV_HA_CARGO_TARGET_DIR=/tmp/nokv-p11-ha-target scripts/run-metadata-ha-smoke.sh`.
  The run restored `/runs/post.bin inode=1026`, then owner B created
  `/after-failover inode=1027`, re-read the replayed file, and `fsck` reported
  `dangling_count:0`.

### 2026-06-14 P6/P7 HA Timing Metrics

- `bash -n scripts/run-metadata-ha-smoke.sh`: passed.
- `scripts/run-metadata-ha-smoke.sh --help`: passed and documents
  `NOKV_HA_METRICS_JSON`.
- Live RustFS+Docker-etcd smoke passed with:
  `NOKV_HA_ETCD_ENDPOINTS=http://127.0.0.1:12379 NOKV_HA_CARGO_TARGET_DIR=/tmp/nokv-p12-ha-target NOKV_HA_METRICS_JSON=/tmp/nokv-ha-smoke-metrics.json scripts/run-metadata-ha-smoke.sh`.
- The emitted metrics JSON validated with `python3 -m json.tool` and reported
  `lease_ttl_seconds=3`, `failover_observed_ms=5350`, `lease_wait_ms=5047`,
  `owner_b_startup_ms=303`, `verify_after_ready_ms=133`,
  `checkpoint_commit_version=1026`, `post_checkpoint_inode=1026`, and
  `after_failover_inode=1027`.

### 2026-06-14 P4/P6 Stale-Owner Chaos Smoke

- `bash -n scripts/run-metadata-ha-smoke.sh`: passed.
- `scripts/run-metadata-ha-smoke.sh --help`: passed and documents
  `NOKV_HA_STALE_OWNER_CHAOS`, `NOKV_HA_OWNER_A_BIND`, and
  `NOKV_HA_OWNER_B_BIND`.
- Normal live RustFS+Docker-etcd smoke passed after the script refactor with:
  `NOKV_HA_ETCD_ENDPOINTS=http://127.0.0.1:12379 NOKV_HA_CARGO_TARGET_DIR=/tmp/nokv-p13-ha-target NOKV_HA_METRICS_JSON=/tmp/nokv-ha-smoke-normal.json scripts/run-metadata-ha-smoke.sh`.
- Stale-owner live RustFS+Docker-etcd smoke passed with:
  `NOKV_HA_STALE_OWNER_CHAOS=1 NOKV_HA_ETCD_ENDPOINTS=http://127.0.0.1:12379 NOKV_HA_CARGO_TARGET_DIR=/tmp/nokv-p13-ha-target NOKV_HA_METRICS_JSON=/tmp/nokv-ha-smoke-stale-owner.json scripts/run-metadata-ha-smoke.sh`.
- The stale-owner metrics JSON validated with `python3 -m json.tool` and
  reported `lease_ttl_seconds=3`, `failover_observed_ms=5375`,
  `lease_wait_ms=5069`, `owner_b_startup_ms=306`,
  `verify_after_ready_ms=222`, `stale_owner_detect_after_resume_ms=36`,
  `stale_owner_fence_after_detect_ms=33`, `post_checkpoint_inode=1026`, and
  `after_failover_inode=1027`.

### 2026-06-14 P2/P7 SDK Batch Data Read

- `CARGO_TARGET_DIR=/tmp/nokv-p14-target cargo test -p nokv-client service_file_client_read_paths`: passed with the batch-open and parallel distinct-plan read tests selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p14-target cargo test -p nokv-bench ai_dataset`: passed with the `ai_dataset_batch_read_defaults_to_tiered_hot_root` test selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p14-target cargo clippy -p nokv-client -p nokv-bench --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cd docs && npm run build`: passed.
- RustFS smoke passed with:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-dataset-batch-read NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p14-target scripts/run-rustfs-e2e.sh`.
- The smoke emitted `256` sample reads per phase at `batch_size=4`:
  cold `0.358820s`, `713.45 samples/s`, `0.3484 MiB/s`, `256` object fallbacks;
  warm `0.009084s`, `28182.06 samples/s`, `13.7608 MiB/s`, `256` cache hits.

### 2026-06-14 P2/P7 SDK Packed-Shard Range Read

- Baseline RustFS smoke before this step:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-shard-range-read NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p15-target scripts/run-rustfs-e2e.sh`.
- Baseline emitted `512` sample reads per phase over `8` shards at
  `64` samples/shard: cold `0.152299s`, `3361.81 samples/s`,
  `1.6415 MiB/s`, `41` object gets; warm `0.011721s`,
  `43681.66 samples/s`, `21.3289 MiB/s`, `520` cache hits.
- `CARGO_TARGET_DIR=/tmp/nokv-p15-target cargo test -p nokv-client service_file_client_read_path_ranges`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p15-target cargo test -p nokv-client service_file_client_read_paths`: passed with the batch-open and parallel distinct-plan read tests selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p15-target cargo test -p nokv-bench ai_shard`: passed with the `ai_shard_range_read_defaults_to_tiered_hot_root` test selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p15-target cargo clippy -p nokv-client -p nokv-bench --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cd docs && npm run build`: passed.
- Post-change RustFS smoke passed with the same command and emitted:
  cold `0.135576s`, `3776.48 samples/s`, `1.8440 MiB/s`, `8` object gets;
  warm `0.000388s`, `1319305.41 samples/s`, `644.1921 MiB/s`, `8` cache hits.

### 2026-06-14 P2/P7 Large Training Batch Layout-Open

- `CARGO_TARGET_DIR=/tmp/nokv-p16-target cargo test -p nokv-client metadata_client_open_read_plan_batch`: passed with the normal and chunked batch-open tests selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p16-target cargo test -p nokv-client service_file_client_read_paths`: passed with the SDK batch-read tests selected.
- `CARGO_TARGET_DIR=/tmp/nokv-p16-target cargo clippy -p nokv-client -p nokv-bench --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cd docs && npm run build`: passed.
- Large-batch RustFS smoke passed with:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-dataset-batch-read NOKV_E2E_OBJECT_CONCURRENCY=256 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p16-target scripts/run-rustfs-e2e.sh`.
- The smoke emitted `256` sample reads in one logical batch: cold `0.218760s`,
  `1170.23 samples/s`, `0.5714 MiB/s`, `256` object gets; warm `0.004392s`,
  `58285.04 samples/s`, `28.4595 MiB/s`, `256` cache hits.

### 2026-06-14 P7 Sparse Shard Gap-Coalescing Benchmark

- `CARGO_TARGET_DIR=/tmp/nokv-p17-target cargo test -p nokv-bench parse_object_size_concurrency_and_cache_options`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p17-target cargo test -p nokv-bench ai_shard`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p17-target cargo clippy -p nokv-bench --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `cd docs && npm run build`: passed.
- Sparse exact RustFS smoke passed with:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-shard-range-read NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p17-target scripts/run-rustfs-e2e.sh --range-stride 2 --range-coalesce-gap-bytes 0`.
- Sparse exact emitted `256` selected sample reads: cold `0.134066s`,
  `1909.51 samples/s`, `0.9324 MiB/s`, `65` object gets; warm `0.005565s`,
  `46004.20 samples/s`, `22.4630 MiB/s`, `256` cache hits.
- Sparse gap-coalesced RustFS smoke passed with the same command except
  `--range-coalesce-gap-bytes 512`.
- Sparse gap-coalesced emitted: cold `0.119168s`,
  `2148.23 samples/s`, `1.0489 MiB/s`, `12` object gets; warm `0.000301s`,
  `851205.32 samples/s`, `415.6276 MiB/s`, `8` cache hits and `2` object gets.

### 2026-06-14 P2/P7 SDK Shard Read-Ahead

- `CARGO_TARGET_DIR=/tmp/nokv-p18-target cargo test -p nokv-client service_file_client_read_path_ranges -- --nocapture`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p18-target cargo clippy -p nokv-client --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- Sparse exact RustFS smoke passed with:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-shard-range-read NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p18-target scripts/run-rustfs-e2e.sh --range-stride 2 --range-coalesce-gap-bytes 0`.
- Sparse exact emitted `256` selected sample reads: cold `0.161924s`,
  `1580.99 samples/s`, `0.7720 MiB/s`, `32` object gets; warm `0.006802s`,
  `37636.22 samples/s`, `18.3771 MiB/s`, `504` cache hits and `0` object gets.
- Sparse gap-coalesced RustFS smoke passed with the same command except
  `--range-coalesce-gap-bytes 512`.
- Sparse gap-coalesced emitted: cold `0.137916s`,
  `1856.21 samples/s`, `0.9064 MiB/s`, `11` object gets; warm `0.000582s`,
  `439768.09 samples/s`, `214.7305 MiB/s`, `8` cache hits and `2` object gets.
- On this smoke shape, gap coalescing remains the larger packed-shard win.
  Read-ahead lowers exact sparse cold object GET count but adds visible
  prefetch/cache-fill overhead, so larger shard profiles are still needed before
  treating it as a default throughput win.

### 2026-06-14 P2/P7 SDK Shard Read-Ahead Admission

- `CARGO_TARGET_DIR=/tmp/nokv-p19-target cargo test -p nokv-client service_file_client_read_path_ranges -- --nocapture`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p19-target cargo clippy -p nokv-client --all-targets -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- Sparse exact RustFS smoke passed with:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-shard-range-read NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p19-target scripts/run-rustfs-e2e.sh --range-stride 2 --range-coalesce-gap-bytes 0`.
- Sparse exact emitted `256` selected sample reads: cold `0.164664s`,
  `1554.68 samples/s`, `0.7591 MiB/s`, `60` object gets; warm `0.004342s`,
  `58962.40 samples/s`, `28.7902 MiB/s`, `256` cache hits and `0` object gets.
- Sparse gap-coalesced RustFS smoke passed with the same command except
  `--range-coalesce-gap-bytes 512`.
- Sparse gap-coalesced emitted: cold `0.124632s`,
  `2054.05 samples/s`, `1.0030 MiB/s`, `11` object gets; warm `0.000405s`,
  `631708.82 samples/s`, `308.4516 MiB/s`, `8` cache hits and `1` object get.
- The admission gate prevents small 512-byte range windows from taking the
  range read-ahead path. On the smoke workload, that restores the warm sparse
  exact row from the previous no-admission `37636.22 samples/s` to
  `58962.40 samples/s`.

### 2026-06-14 P7 Large-Window Read-Ahead Policy Smoke

- Large-window RustFS smoke passed with:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-shard-range-read NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=/tmp/nokv-p20-target scripts/run-rustfs-e2e.sh --sample-bytes 1048576 --range-stride 32 --range-coalesce-gap-bytes 0`.
- The smoke emitted `16` selected 1 MiB sample reads: cold `0.137781s`,
  `116.13 samples/s`, `116.1264 MiB/s`, `17` object gets,
  `16` prefetch enqueued, and `4` prefetch completed by phase end.
- Warm emitted `0.001316s`, `12156.90 samples/s`, `12156.9000 MiB/s`,
  `16` cache hits, `0` foreground object gets, and `4` prefetch attempts that
  were dropped because the target ranges were already cached or pending.
- This validates the admission boundary: 512-byte windows stay direct, while
  admitted 1 MiB windows produce visible prefetch work through the existing
  object prefetcher.

### 2026-06-14 P7 AI Shard Range Matrix Script

- `bash -n scripts/run-ai-shard-range-matrix.sh scripts/run-rustfs-e2e.sh`: passed.
- Full NoKV/RustFS shard-range matrix passed with:
  `NOKV_AI_SHARD_MATRIX_CARGO_TARGET_DIR=/tmp/nokv-p21-target scripts/run-ai-shard-range-matrix.sh`.
- `small-exact` emitted `256` selected 512-byte reads: cold `0.135768s`,
  `1885.57 samples/s`, `0.9207 MiB/s`, `64` object gets; warm `0.006168s`,
  `41503.98 samples/s`, `20.2656 MiB/s`, `256` cache hits and `0` object gets.
- `small-gap` emitted `256` selected 512-byte reads: cold `0.112671s`,
  `2272.10 samples/s`, `1.1094 MiB/s`, `11` object gets; warm `0.000372s`,
  `688249.75 samples/s`, `336.0594 MiB/s`, `8` cache hits and `2` object gets.
- `large-window` emitted `16` selected 1 MiB reads: cold `0.136650s`,
  `117.09 samples/s`, `117.0876 MiB/s`, `18` object gets, and
  `16` prefetch enqueued; warm `0.001707s`, `9373.86 samples/s`,
  `9373.8557 MiB/s`, `16` cache hits and `0` foreground object gets.
- The matrix is the local NoKV/RustFS L1 evidence gate for this data path. It is
  not a mounted JuiceFS comparison and should not be reported as an L2 result.

### 2026-06-14 P7 AI Shard Range Combined CSV

- `bash -n scripts/run-ai-shard-range-matrix.sh scripts/run-rustfs-e2e.sh`: passed.
- Combined CSV smoke passed with:
  `NOKV_AI_SHARD_MATRIX_CARGO_TARGET_DIR=/tmp/nokv-p22-target NOKV_AI_SHARD_MATRIX_OUTPUT_DIR=/tmp/nokv-shard-matrix-combined-smoke scripts/run-ai-shard-range-matrix.sh small-gap`.
- The smoke wrote `/tmp/nokv-shard-matrix-combined-smoke/matrix.csv` with one
  `matrix_case,...` header and two `small-gap` data rows for cold and warm
  phases.
- The same run kept `/tmp/nokv-shard-matrix-combined-smoke/small-gap.log` as
  the raw per-case log. Observed small-gap results were cold `0.128222s`,
  `1996.54 samples/s`, `11` object gets; warm `0.000503s`,
  `508735.95 samples/s`, `8` cache hits and `3` object gets.

### 2026-06-14 P7 AI Data-Plane L2 Smoke

- `bash -n scripts/run-fs-benchmark.sh scripts/run-ai-dataplane-l2.sh scripts/lib/fs-bench-common.sh`: passed.
- `python3 scripts/fs-bench-env.py --selftest`: passed.
- Wrapper help checks passed for `scripts/run-ai-dataplane-l2.sh --help` and
  `scripts/run-fs-benchmark.sh --help`.
- Mounted NoKV-vs-JuiceFS AI data-plane smoke passed with:
  `NOKV_AI_L2_RESULT_DIR=/tmp/nokv-ai-l2-smoke NOKV_AI_L2_STAMP=smoke NOKV_AI_L2_HOST_LABEL=local scripts/run-ai-dataplane-l2.sh`.
- The run wrote raw, aggregate, environment, and decompose artifacts under
  `/tmp/nokv-ai-l2-smoke`. The environment JSON recorded
  `product_workloads=checkpoint,training_read`, `primitive_workloads=""`, and
  `real_tools=skip`.
- Smoke result, L2 mounted boundary, profile `smoke`, p=1:
  checkpoint write was NoKV `333.09 ops/s` vs JuiceFS `305.47 ops/s`
  (`1.09x` NoKV/JuiceFS); training read cold was NoKV `3247.07 ops/s` vs
  JuiceFS `994.58 ops/s` (`3.26x`); training read warm was NoKV
  `5111.41 ops/s` vs JuiceFS `1692.14 ops/s` (`3.02x`).
- The decompose sidecar captured NoKV object-writeback, local-hot put, object
  GET, cache-hit, and block-cache-hit deltas for the combined
  `checkpoint,training_read` phase.

### 2026-06-14 P7 Mounted Packed-Shard L2 Smoke

- `python3 -m py_compile bench/drivers/posix_bench.py scripts/fs-bench-env.py scripts/fs-bench-summary.py scripts/aggregate-fs-benchmark.py`: passed.
- Local driver smoke passed for `ai_shard_range_read` against a temporary local
  filesystem with `--dataset-dirs 2 --files-per-dir 4 --sample-bytes 16
  --range-stride 2 --range-coalesce-gap-bytes 16`, emitting cold/warm rows with
  logical samples, physical pread windows, physical bytes, and read
  amplification.
- Mounted NoKV-vs-JuiceFS smoke passed with:
  `NOKV_AI_L2_RESULT_DIR=/tmp/nokv-ai-l2-smoke NOKV_AI_L2_STAMP=shard-smoke NOKV_AI_L2_HOST_LABEL=local scripts/run-ai-dataplane-l2.sh`.
- The environment JSON recorded `product_workloads=checkpoint,training_read,ai_shard_range_read`,
  `range_stride=2`, `range_coalesce_gap_bytes=512`, and `real_tools=skip`.
- Smoke result, L2 mounted boundary, profile `smoke`, p=1:
  checkpoint write was NoKV `289.49 ops/s` vs JuiceFS `317.43 ops/s`;
  training read cold was NoKV `2642.88 ops/s` vs JuiceFS `1011.80 ops/s`
  (`2.61x` NoKV/JuiceFS); training read warm was NoKV `3504.70 ops/s` vs
  JuiceFS `1666.75 ops/s` (`2.10x`).
- The new mounted packed-shard row showed the opposite: `ai_shard_range_read`
  cold was NoKV `39987.50 samples/s` vs JuiceFS `87553.82 samples/s`
  (`2.19x` JuiceFS/NoKV); warm was NoKV `45996.63 samples/s` vs JuiceFS
  `103213.65 samples/s` (`2.24x` JuiceFS/NoKV). Both systems read the same
  `256` logical samples through `8` physical POSIX pread windows with
  `258048` physical bytes and `1.9688x` read amplification.
- This identifies the next concrete data-plane optimization target: NoKV's
  mounted large-range read path over packed shard files, separate from the
  already-fast L1 SDK coalesced range path.

### 2026-06-14 P7 FUSE Read-Open Attr Cache

- Added a FUSE-layer attr cache for read-only opens, populated from
  lookup/readdirplus/getattr/update/publish paths.
- Kept write opens on live metadata attr lookup so write generation and parent
  name state are not trusted from a read-side hot cache.
- Connected local mutation invalidation for dirty writes, truncate, unlink,
  rmdir, rename replacement, and publish result refresh.
- Connected watch replay to local attr invalidation through
  `FuseInvalidationWorker`, so external metadata changes drop cached attrs in
  addition to kernel inode invalidation.
- Added unit coverage for entry attr caching, attr-only forget, full inode
  forget, and watch local invalidation.
- `cargo test -p nokv-fuse`: passed, 44 tests.
- Focused mounted L2 smoke passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=smoke NOKV_AI_L2_REPEATS=1 NOKV_AI_L2_CONCURRENCY=1 scripts/run-ai-dataplane-l2.sh`.
- Artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T165647Z.*`.
- Smoke result, L2 mounted boundary, profile `smoke`, p=1:
  `ai_shard_range_read` cold was NoKV `80683.92 samples/s` vs JuiceFS
  `58300.53 samples/s` (`1.38x` NoKV/JuiceFS); warm was NoKV
  `210598.34 samples/s` vs JuiceFS `70540.42 samples/s`
  (`2.99x` NoKV/JuiceFS).
- Both systems read the same `256` logical samples through `8` physical POSIX
  pread windows with `258048` physical bytes and `1.9688x` read amplification.
- The NoKV decompose sidecar recorded `28` object GETs, `21` cache hits,
  `14` block-cache hits, and `14` prefetch object GETs for the combined cold
  and warm shard-range phase.
- This closes the immediate mounted packed-shard regression found in the prior
  smoke. The next benchmark step is repeated `standard` L2 shard-range rows
  before treating the ratio as a stable performance claim.

### 2026-06-14 P7 Standard L2 Packed-Shard Repeats

- Repeated mounted L2 NoKV-vs-JuiceFS standard profile passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=standard NOKV_AI_L2_REPEATS=3 NOKV_AI_L2_CONCURRENCY=1 scripts/run-ai-dataplane-l2.sh`.
- Artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T165845Z.*`.
- Shape: `32` shard files, `256` samples per shard, `128` selected samples per
  shard, `16384` bytes per sample, `4096` logical sample reads, `4096`
  physical POSIX pread windows, `67108864` physical bytes, and `1.0000x` read
  amplification.
- Median cold row: NoKV `218995.47 samples/s`, `3421.8043 MiB/s`, p99
  `18.38us`; JuiceFS `140276.58 samples/s`, `2191.8215 MiB/s`, p99 `4.92us`
  (`1.56x` NoKV/JuiceFS by throughput).
- Median warm row: NoKV `283935.88 samples/s`, `4436.4982 MiB/s`, p99
  `3.21us`; JuiceFS `148249.36 samples/s`, `2316.3962 MiB/s`, p99 `5.79us`
  (`1.92x` NoKV/JuiceFS by throughput).
- Caveat: NoKV cold-read tail variance is still visible. The three NoKV cold
  p99 values were `3.04us`, `53.16us`, and `18.38us`; the slow repeat dropped
  to `65784.53 samples/s` while JuiceFS cold remained tighter across repeats.
- Decompose showed the benchmark wrote `32` local-hot shard objects per repeat.
  Read-side object/cache deltas varied by repeat (`0`, `137`, and `6` object
  GETs reported), so the next optimization pass should isolate cold local-hot
  read admission, stats attribution, and FUSE `F_NOCACHE` interaction before
  claiming stable tail-latency superiority.

### 2026-06-14 P7 Cache-State Filtered L2 Decompose

- Added `--cache-states` to `bench/drivers/posix_bench.py`, with default
  `cold,warm`.
- Wired `NOKV_BENCH_CACHE_STATES` through `scripts/run-fs-benchmark.sh`,
  `scripts/lib/fs-bench-common.sh`, `scripts/fs-bench-env.py`, and the focused
  `NOKV_AI_L2_CACHE_STATES` wrapper env in `scripts/run-ai-dataplane-l2.sh`.
- This keeps default benchmark rows unchanged while allowing cold-only or
  warm-only runs whose NoKV decompose sidecar wraps one read cache state instead
  of combining both.
- Validation passed:
  `python3 -m py_compile bench/drivers/posix_bench.py scripts/fs-bench-env.py`,
  `bash -n scripts/run-fs-benchmark.sh scripts/run-ai-dataplane-l2.sh scripts/lib/fs-bench-common.sh`,
  `scripts/run-ai-dataplane-l2.sh --help`, and
  `python3 scripts/fs-bench-env.py --selftest`.
- Local driver smoke with `--cache-states cold` emitted only one cold
  `ai_shard_range_read` row.
- Mounted cold-only L2 smoke passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=smoke NOKV_AI_L2_REPEATS=1 NOKV_AI_L2_CONCURRENCY=1 NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T170320Z.*`; `env.json` records
  `cache_states=cold`.
- Cold-only smoke result: NoKV `95793.47 samples/s`, `46.7742 MiB/s`, p99
  `320.78us`; JuiceFS `76982.85 samples/s`, `37.5893 MiB/s`, p99 `107.63us`
  (`1.24x` NoKV/JuiceFS by throughput).
- Cold-only NoKV decompose recorded `28` object GETs, `573440` object bytes,
  `14` prefetch object GETs, and `344064` prefetch bytes, without warm cache-hit
  counters mixed into the sidecar.

### 2026-06-14 P7 True-Cold Seed Cache Fix

- Cold-only standard L2 repeats before this fix passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=standard NOKV_AI_L2_REPEATS=3 NOKV_AI_L2_CONCURRENCY=1 NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T170608Z.*`.
- The median row looked competitive but was not a trustworthy cold result:
  NoKV `97836.83 samples/s`, p99 `49.00us`; JuiceFS
  `138038.34 samples/s`, p99 `5.59us`.
- The NoKV sidecar showed inconsistent read counters across repeats, including
  one cold read with zero object/cache-read deltas. That exposed benchmark seed
  pollution from write-side page cache residency rather than a stable data-path
  behavior.
- `bench/drivers/posix_bench.py` now lets seed writes opt out of cache
  residency. The training and shard-range seed paths fsync seed data and apply
  write-side cache-drop hints when the requested cache states include `cold`.
- Local smokes passed:
  `python3 -m py_compile bench/drivers/posix_bench.py`,
  `ai_shard_range_read --cache-states cold`, and
  `training_read --cache-states cold`.
- Re-running cold-only standard L2 after the seed fix wrote artifacts under
  `bench/results/ai-dataplane-l2-Mac-20260613T170900Z.*`.
- Corrected median cold row: NoKV `40504.31 samples/s`,
  `632.8798 MiB/s`, p99 `168.05us`; JuiceFS `17914.41 samples/s`,
  `279.9127 MiB/s`, p99 `88.61us` (`2.26x` NoKV/JuiceFS by throughput).
- The corrected result made the real bottleneck visible: the first small read at
  offset 0 could start sequential read-ahead for every shard, pulling an extra
  one-window prefetch that raised NoKV cold p99.

### 2026-06-14 P7 Initial Read-Ahead Admission Fix

- Tightened `crates/nokv-object/src/pipeline/reader.rs` so a small first read at
  offset 0 does not start sequential read-ahead by itself. Continued contiguous
  reads still start and grow the window, and large initial reads still admit
  prefetch.
- Updated the object pipeline tests to prove first-small-read suppression,
  second-contiguous-read admission, configured-window sizing, and active-window
  throttling.
- Added more NoKV decompose fields for future read-path attribution:
  `prefetch_enqueued`, `prefetch_dropped`, `prefetch_completed`,
  `prefetch_failed`, `prefetch_cache_hits`, `prefetch_cache_hit_bytes`, and
  `read_plan_cache_hits`.
- Validation passed:
  `cargo test -p nokv-object file_read_pipeline`,
  `python3 -m py_compile bench/drivers/posix_bench.py bench/drivers/decompose.py scripts/fs-bench-env.py`,
  and `python3 bench/drivers/decompose.py --selftest`.
- Re-running cold-only standard L2 after the read-ahead fix wrote artifacts
  under `bench/results/ai-dataplane-l2-Mac-20260613T171536Z.*`.
- Fixed median cold row: NoKV `42014.67 samples/s`, `656.4793 MiB/s`, p99
  `77.07us`; JuiceFS `16913.21 samples/s`, `264.2690 MiB/s`, p99 `78.86us`
  (`2.48x` NoKV/JuiceFS by throughput).
- NoKV prefetch object GETs dropped from `64` to `32`, and prefetch bytes
  dropped from about `267.9 MiB` to `134.2 MiB`. The remaining prefetch is the
  expected block-warmup path for small inner-block reads, not the earlier
  sequential read-ahead misfire.
- `cargo test -p nokv-fuse` passed after the object-pipeline change, covering
  the FUSE-side read-pipeline and stats conversion boundary.

### 2026-06-14 P7 Concurrent L2 Shard-Range Sweep

- `bench/drivers/posix_bench.py` now runs `ai_shard_range_read` shard files
  through a thread pool when `--concurrency > 1`. The row records the real
  concurrency and adds `shard_read_workers=N` to the shape.
- `scripts/run-fs-benchmark.sh` now treats `ai_shard_range_read` as a throughput
  product workload: metadata/checkpoint/training rows still run once at p=1,
  while shard-range rows follow the configured concurrency sweep. This makes
  `NOKV_AI_L2_CONCURRENCY="1 4"` meaningful for the focused L2 wrapper.
- Local driver smoke against a temporary directory passed with
  `--workloads ai_shard_range_read --concurrency 4 --cache-states cold`,
  emitting a p=4 row with `shard_read_workers=4`.
- Mounted smoke L2 p=1/p=4 cold run passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=smoke NOKV_AI_L2_REPEATS=1 NOKV_AI_L2_CONCURRENCY="1 4" NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Smoke artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T172258Z.*`. The p=1 row favored
  NoKV (`115780.31` vs JuiceFS `54123.10 samples/s`, `2.14x`), while p=4 was
  too small to be useful (`8` physical windows total) and slightly favored
  JuiceFS (`1.03x`). Treat this only as a scheduling smoke.
- Mounted standard L2 p=1/p=4 cold repeats passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=standard NOKV_AI_L2_REPEATS=2 NOKV_AI_L2_CONCURRENCY="1 4" NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Standard artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T172317Z.*`.
- Median p=1 cold row: NoKV `42135.70 samples/s`, `658.3704 MiB/s`, p99
  `81.28us`; JuiceFS `18178.35 samples/s`, `284.0367 MiB/s`, p99 `83.73us`
  (`2.32x` NoKV/JuiceFS by throughput).
- Median p=4 cold row: NoKV `67953.74 samples/s`, `1061.7770 MiB/s`, p99
  `468.00us`; JuiceFS `47048.01 samples/s`, `735.1253 MiB/s`, p99 `172.32us`
  (`1.44x` NoKV/JuiceFS by throughput).
- Decompose shows p=4 still has foreground miss pressure while block warmup is
  pending: NoKV p=1 reported `452` object GETs per repeat, while p=4 reported
  `532` and `475`; prefetch stayed at `32` completed object GETs. The next real
  optimization should reduce foreground miss fan-out without making the first
  block warmup synchronous.
- A synchronous block-fill experiment was measured and rejected. Artifacts under
  `bench/results/ai-dataplane-l2-Mac-20260613T172909Z.*` showed p=4 throughput
  fell to `60823.48 samples/s` and p99 worsened to `872.37us`, so that code path
  was not kept. The current direction remains asynchronous block warmup with a
  better pending-prefetch coordination policy.

### 2026-06-14 P7 Background Prefetch Worker Tuning

- Tested foreground covering-range wait through the object read coordinator and
  rejected it. It reduced foreground p=4 object GETs to `105` per repeat, but
  made foreground exact reads wait on full-block warmup. Artifacts under
  `bench/results/ai-dataplane-l2-Mac-20260613T174130Z.*` showed p=4 p99
  worsened to `1033.18us` and throughput dropped to `60859.11 samples/s`.
- Kept the existing asynchronous warmup strategy and changed
  `ObjectPrefetchOptions::default().workers` from `1` to `2`. This affects the
  SDK client and FUSE mount defaults through the existing options path; no new
  public API or metadata surface was added.
- Also tested `--prefetch-workers 3` and `--prefetch-workers 4`. In this local
  standard p=4 shape, `2` workers was the best tradeoff: `74499.83 samples/s`
  with p99 `534.88us`; `3` workers was `71493.49 samples/s` with p99
  `640.79us`; `4` workers was `76065.72 samples/s` with p99 `600.43us`.
- Default two-worker mounted standard L2 p=1/p=4 cold repeats passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=standard NOKV_AI_L2_REPEATS=2 NOKV_AI_L2_CONCURRENCY="1 4" NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Default artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T175033Z.*`.
- Median p=1 cold row: NoKV `40763.08 samples/s`, `636.9231 MiB/s`, p99
  `106.66us`; JuiceFS `18065.79 samples/s`, `282.2781 MiB/s`, p99 `77.69us`
  (`2.26x` NoKV/JuiceFS by throughput).
- Median p=4 cold row: NoKV `73620.83 samples/s`, `1150.3255 MiB/s`, p99
  `554.82us`; JuiceFS `47558.19 samples/s`, `743.0968 MiB/s`, p99 `130.16us`
  (`1.55x` NoKV/JuiceFS by throughput).
- Compared with the one-worker p=4 artifact
  `bench/results/ai-dataplane-l2-Mac-20260613T172317Z.*`, two-worker prefetch
  raised NoKV p=4 throughput from `67953.74` to `73620.83 samples/s` and cut
  foreground object GETs from `475-532` to `215-285` per repeat. Tail latency
  remains worse (`554.82us` vs the prior `468.00us` and JuiceFS `130.16us`), so
  the next data-plane task is not more prefetch concurrency; it is reducing
  FUSE/object scheduling tail while preserving the foreground GET reduction.

### 2026-06-14 P7 Segment Warmup Tail Tuning

- Changed small exact-read cache warmup from full 4 MiB object-block warmup to a
  1 MiB segment warmup (`DEFAULT_BLOCK_SIZE / 4`) aligned by object offset. This
  keeps foreground reads exact and asynchronous while letting the block cache
  serve later sparse sample reads through covering-range hits.
- Extended the warmup trigger to block-start small exact reads, so packed shard
  readers start warming the first segment on the first sample instead of waiting
  for the first inner-block sample.
- Rejected a foreground block-fill variant. Artifacts under
  `bench/results/ai-dataplane-l2-Mac-20260613T180434Z.*` reduced p=4 foreground
  object GETs to `32`, but moved 4 MiB object reads onto foreground sample
  latency: NoKV p=4 fell to `62632.41 samples/s` and p99 worsened to
  `853.13us`.
- Re-tested four prefetch workers after segment warmup. Artifacts under
  `bench/results/ai-dataplane-l2-Mac-20260613T181150Z.*` showed p=4 NoKV
  `73841.63 samples/s` and p99 `210.19us`. That p99 was slightly lower than the
  two-worker default, but throughput was lower, so the default remains two
  workers.
- Default two-worker mounted standard L2 p=1/p=4 cold repeats passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=standard NOKV_AI_L2_REPEATS=2 NOKV_AI_L2_CONCURRENCY="1 4" NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Default artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T181029Z.*`.
- Median p=1 cold row: NoKV `42266.54 samples/s`, `660.4147 MiB/s`, p99
  `105.20us`; JuiceFS `18684.26 samples/s`, `291.9416 MiB/s`, p99 `75.88us`
  (`2.26x` NoKV/JuiceFS by throughput).
- Median p=4 cold row: NoKV `76209.53 samples/s`, `1190.7740 MiB/s`, p99
  `214.24us`; JuiceFS `46833.65 samples/s`, `731.7757 MiB/s`, p99 `208.12us`
  (`1.63x` NoKV/JuiceFS by throughput).
- Compared with the previous full-block warmup default artifact
  `bench/results/ai-dataplane-l2-Mac-20260613T175033Z.*`, segment warmup raised
  p=4 throughput from `73620.83` to `76209.53 samples/s` and cut p99 from
  `554.82us` to `214.24us`. Decompose now shows more, smaller foreground exact
  misses (`466-494` foreground object GETs) and more segment prefetch requests
  (`134-136` completed), instead of waiting on fewer 4 MiB background warmups.
  The next tail task is profiling foreground exact-miss overhead and FUSE request
  batching, not increasing prefetch workers.

### 2026-06-14 P7 Block Cache Range Index

- Added a structured range index to the in-memory and disk block caches for
  `object_key:offset:len` entries. Covering-range hits now find candidate
  offsets through the index before reading bytes, instead of scanning and
  reparsing all string keys under an object prefix.
- Kept the index as soft cache state only. It is updated on put, replace,
  expiry, file-missing cleanup, and eviction; metadata truth and object layout
  are unchanged.
- Fixed disk block-cache same-key replacement while touching this lifecycle:
  replacement now removes the old cache file before writing and renaming the new
  payload, so the cleanup step cannot delete the freshly written file.
- Added regression tests for memory and disk range-index eviction cleanup, disk
  same-key replacement, and existing covering-range reuse.
- Current-code mounted standard L2 p=1/p=4 cold repeats passed with:
  `NOKV_AI_L2_WORKLOADS=ai_shard_range_read NOKV_AI_L2_PROFILE=standard NOKV_AI_L2_REPEATS=2 NOKV_AI_L2_CONCURRENCY="1 4" NOKV_AI_L2_CACHE_STATES=cold scripts/run-ai-dataplane-l2.sh`.
- Current-code artifacts were written under
  `bench/results/ai-dataplane-l2-Mac-20260613T182109Z.*`.
- Median p=1 cold row: NoKV `42648.41 samples/s`, `666.3814 MiB/s`, p99
  `99.64us`; JuiceFS `17858.85 samples/s`, `279.0445 MiB/s`, p99 `115.88us`
  (`2.39x` NoKV/JuiceFS by throughput).
- Median p=4 cold row: NoKV `75098.74 samples/s`, `1173.4178 MiB/s`, p99
  `211.62us`; JuiceFS `46982.24 samples/s`, `734.0975 MiB/s`, p99 `190.23us`
  (`1.60x` NoKV/JuiceFS by throughput).
- Compared with the segment-warmup artifact
  `bench/results/ai-dataplane-l2-Mac-20260613T181029Z.*`, this is a small
  cache-hit-path cleanup rather than a new throughput step: p=4 p99 moved from
  `214.24us` to `211.62us`, while p=4 throughput moved from `76209.53` to
  `75098.74 samples/s`. Keep the range index for the algorithmic hit-path and
  disk-cache lifecycle fix, not as a headline benchmark win.

### 2026-06-14 P7 AI L2 Seed-Fsync/Cache Matrix

- Added `scripts/run-ai-dataplane-l2-matrix.sh` as a thin wrapper over the
  mounted NoKV-vs-JuiceFS L2 runner.
- The matrix loops over benchmark seed `fsync=0/1` and isolated `cold` /
  `warm` cache-state variants. Each case writes the normal raw, aggregate,
  environment, and decompose artifacts, then appends rows to combined
  `matrix.raw.csv` and `matrix.aggregate.csv` with `matrix_case`, `fsync`, and
  `cache_state_scope` columns.
- This keeps NoKV `/stats` decompose deltas scoped to one cache state instead
  of wrapping cold and warm reads in the same object/cache counter window.
- The `fsync` column is the POSIX seed-data fsync used by the benchmark driver;
  it is not a metadata-tier switch. The NoKV rows in this matrix still use
  `nokv-direct-wal-async`.
- Validation passed:
  `bash -n scripts/run-ai-dataplane-l2-matrix.sh` and
  `scripts/run-ai-dataplane-l2-matrix.sh --help`.
- Minimal mounted smoke passed with:
  `NOKV_AI_L2_MATRIX_PROFILE=smoke NOKV_AI_L2_MATRIX_REPEATS=1 NOKV_AI_L2_MATRIX_CONCURRENCY=1 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=cold NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
- Smoke artifacts were written under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T183136Z/`.
- Smoke p=1 cold row: NoKV `133879.59 samples/s`, `65.3709 MiB/s`, p99
  `165.73us`; JuiceFS `48483.32 samples/s`, `23.6735 MiB/s`, p99 `329.76us`
  (`2.76x` NoKV/JuiceFS by throughput). This is a script validation run, not a
  standard-profile performance claim.
- Full mounted standard matrix passed with:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=2 NOKV_AI_L2_MATRIX_CONCURRENCY="1 4" NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES="cold warm" NOKV_AI_L2_MATRIX_FSYNC="0 1" scripts/run-ai-dataplane-l2-matrix.sh`.
- Standard artifacts were written under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T183432Z/`.
- `fsync0-cold`: p=1 NoKV `43971.40 samples/s`, p99 `99.28us` vs JuiceFS
  `18720.83 samples/s`, p99 `74.17us` (`2.35x`); p=4 NoKV
  `72880.23 samples/s`, p99 `206.20us` vs JuiceFS `47498.27 samples/s`, p99
  `166.93us` (`1.53x`).
- `fsync1-cold`: p=1 NoKV `42643.32 samples/s`, p99 `102.12us` vs JuiceFS
  `17894.65 samples/s`, p99 `76.98us` (`2.38x`); p=4 NoKV
  `69294.08 samples/s`, p99 `227.14us` vs JuiceFS `45343.32 samples/s`, p99
  `202.50us` (`1.53x`).
- `fsync0-warm`: p=1 NoKV `284259.24 samples/s`, p99 `3.44us` vs JuiceFS
  `169793.84 samples/s`, p99 `3.98us` (`1.67x`); p=4 NoKV
  `216054.46 samples/s`, p99 `75.43us` vs JuiceFS `233058.43 samples/s`, p99
  `67.50us` (`0.93x`).
- `fsync1-warm`: p=1 NoKV `266107.41 samples/s`, p99 `3.77us` vs JuiceFS
  `164930.30 samples/s`, p99 `3.94us` (`1.61x`); p=4 NoKV
  `228477.29 samples/s`, p99 `74.44us` vs JuiceFS `231556.30 samples/s`, p99
  `69.21us` (`0.99x`).
- Decompose shows cold p=4 still issues roughly `461-473` foreground object
  GETs with `128` prefetch object GETs per repeat, while warm p=4 can become
  cache/FUSE overhead bound. The next optimization target is warm/concurrent
  FUSE cache-read scheduling, not more background prefetch workers.

### 2026-06-14 P7 Hot Cache-State Probe

- Added `hot` as a benchmark cache state in `bench/drivers/posix_bench.py`.
  It runs a buffered warm-up pass, then measures with the same kernel-cache
  bypass mode used by cold rows. This keeps filesystem/client caches warm while
  forcing the measured pass through the FUSE implementation instead of letting
  the kernel page cache hide NoKV `/stats` deltas.
- This was the initial buffered-warmup `hot` definition. It was later corrected
  in `2026-06-14 P7 Client-Hot Semantics Fix` because a buffered warm-up can be
  served entirely by the kernel page cache and fail to warm the filesystem
  client cache.
- Wired `hot` through the cache-state validation in
  `scripts/run-ai-dataplane-l2-matrix.sh`; `scripts/run-ai-dataplane-l2.sh` and
  `scripts/run-fs-benchmark.sh` help text now list `cold,warm,hot`.
- Local driver validation passed with:
  `python3 bench/drivers/posix_bench.py --system local --mount /tmp --metadata-tier local --object-backend local --profile smoke --concurrency 1 --workloads ai_shard_range_read --dataset-dirs 2 --files-per-dir 8 --sample-bytes 512 --checkpoint-bytes 4096 --checkpoint-steps 1 --range-stride 2 --range-coalesce-gap-bytes 512 --cache-states hot --fsync 0 --emit-header 1`.
- Invalid cache-state validation also passed: `--cache-states invalid` exits
  with `--cache-states must be a non-empty subset of: cold,warm,hot`.
- Mounted standard p=1/p=4 hot run passed with:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=2 NOKV_AI_L2_MATRIX_CONCURRENCY="1 4" NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T184400Z/`.
- Mounted standard p=4-only hot run passed with:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=2 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T184511Z/`.
- p=4-only hot row: NoKV `208445.95 samples/s`, `3256.9680 MiB/s`, p99
  `73.61us`; JuiceFS `227322.64 samples/s`, `3551.9163 MiB/s`, p99 `66.42us`
  (`0.92x` NoKV/JuiceFS by throughput).
- Decompose for the p=4-only hot row still shows real NoKV read-path work:
  `read_plan_cache_hits=512/3200`, `block_cache_hits=470/2940`, and
  foreground object GETs ranging from `58` to `360` across the two repeats.
  That makes this a better optimization target than the earlier warm row, whose
  p=4 decompose could be entirely hidden by kernel page cache.

### 2026-06-14 P7 P4 Hot-Path Cleanup

- Changed `MemoryBlockCache` cached bytes from `Vec<u8>` to `Arc<[u8]>`, so
  cache-hit lookups clone the shared backing buffer while holding the cache
  mutex and copy the returned `Vec<u8>` after releasing the lock. The public
  `BlockCache` API still returns owned `Vec<u8>`.
- Changed the FUSE backend read-plan cache from one global
  `Mutex<ObjectReadPlanCache>` to fixed shards. This keeps the existing cache
  behavior and stats while reducing shared lock contention across concurrent
  shard-range readers.
- Changed `ObjectReadPlanCache` LRU touch from `VecDeque::retain` on every hit
  to an append-only recency queue with lazy eviction. Repeated exact read-plan
  hits are now amortized O(1) instead of scanning the shard-local LRU queue.
- Changed read handles to advance the per-fd `FileReadPipeline` in place under
  one lock instead of clone/read/writeback. This removes two lock operations
  and avoids losing pipeline state when the same handle is read concurrently.
- Added a narrow single-block cache-hit fast path in
  `read_object_blocks_with_cache_options`: when one block covers the whole
  output and the block cache hits, the reader returns the cached bytes directly
  without allocating an output buffer, sorting pending reads, or entering the
  fetch/coalesce path.
- Validation passed:
  `cargo test -p nokv-object`,
  `cargo test -p nokv-fuse`,
  targeted cache/read-handle tests, and repeated `cargo fmt --all`.
- Focused mounted p=4 hot runs:
  - pre-cleanup p=4-only hot baseline:
    `bench/results/ai-dataplane-l2-matrix-Mac-20260613T184511Z/`.
    NoKV `208445.95 samples/s`, `3256.9680 MiB/s`, p99 `73.61us`; JuiceFS
    `227322.64 samples/s`, `3551.9163 MiB/s`, p99 `66.42us`.
  - after block-cache/read-plan cleanup, two-repeat run:
    `bench/results/ai-dataplane-l2-matrix-Mac-20260613T185721Z/`.
    NoKV `222509.33 samples/s`, `3476.7083 MiB/s`, p99 `74.65us`; JuiceFS
    `245083.00 samples/s`, `3829.4219 MiB/s`, p99 `57.03us`.
  - final five-repeat run:
    `bench/results/ai-dataplane-l2-matrix-Mac-20260613T190036Z/`.
    NoKV `212523.00 samples/s`, `3320.6719 MiB/s`, p99 `75.89us`; JuiceFS
    `222301.12 samples/s`, `3473.4550 MiB/s`, p99 `73.07us`.
- Interpretation: the cleanup is measurable but modest on mounted L2. Compared
  with the p=4-only hot baseline, NoKV median throughput improved about `2%`;
  in the five-repeat comparison JuiceFS remains about `1.05x` faster. The next
  target should be the remaining FUSE/syscall shape and prefetch variability,
  not another block-cache micro-tweak.

### 2026-06-14 P7 Client-Hot Semantics Fix

- Corrected `hot` benchmark semantics in `bench/drivers/posix_bench.py`.
  `hot` now runs a kernel-cache-bypassed warm-up pass and then measures another
  kernel-cache-bypassed pass. The emitted read mode is
  `f-nocache after_f-nocache_warmup=1` on macOS. This avoids the old buffered
  warm-up ambiguity where the kernel page cache could hide the mounted
  filesystem and leave NoKV/JuiceFS client caches cold.
- Updated `docs/benchmarks.md` to define `hot` as client-hot rather than
  buffered-warmup hot.
- Local driver smoke passed:
  `python3 bench/drivers/posix_bench.py --system local --mount /tmp --metadata-tier local --object-backend local --profile smoke --concurrency 1 --workloads ai_shard_range_read --dataset-dirs 2 --files-per-dir 8 --sample-bytes 512 --checkpoint-bytes 4096 --checkpoint-steps 1 --range-stride 2 --range-coalesce-gap-bytes 512 --cache-states hot --fsync 0 --emit-header 1`.
- Focused mounted p=4 client-hot with prefetch enabled:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=5 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T191042Z/`.
  NoKV `149241.22 samples/s`, `2331.8941 MiB/s`, p99 `102.47us`; JuiceFS
  `221743.52 samples/s`, `3464.7425 MiB/s`, p99 `71.55us`
  (`1.49x` JuiceFS/NoKV).
- Focused mounted p=4 client-hot with NoKV prefetch disabled:
  `NOKV_BENCH_NOKV_MOUNT_OPTIONS=--no-prefetch NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=5 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T191223Z/`.
  NoKV `187429.58 samples/s`, `2928.5872 MiB/s`, p99 `90.40us`; JuiceFS
  `230687.99 samples/s`, `3604.4999 MiB/s`, p99 `70.13us`
  (`1.23x` JuiceFS/NoKV).
- Interpretation: the corrected client-hot benchmark is stricter and exposes a
  real mounted p4 gap. Background prefetch worsens this sparse-small-read shape,
  but disabling it does not close the gap. The next product change should make
  small sparse shard reads avoid background-prefetch interference by default and
  reduce per-read FUSE/read-plan overhead under repeated `pread` windows.

### 2026-06-14 P7 Small Exact-Read Admission and Cache-Hit Fast Path

- Changed `FileReadPipeline` cache-warmup admission so a small exact read only
  submits background `cache_warmup` after the same read handle has proven a
  continued stream. Initial random/sparse shard reads stay exact foreground
  reads and do not enqueue prefetch work.
- Kept sequential behavior for real continued streams: tests now assert the
  first small read does not warm, a continued small read does warm, and short
  final blocks only cache the actual object bytes.
- Changed memory and disk block-cache primary maps from `BTreeMap` to
  `HashMap`, because the primary cache-key lookup is unordered hot-path state.
  The ordered range index remains only for covering-range fallback.
- Added an exact-key fast path to `get_block_range`: when the cache contains the
  precise `object_key:offset:len` entry, the reader bypasses the covering-range
  BTreeMap scan and returns the requested window directly.
- Validation passed:
  `cargo test -p nokv-object` and `cargo test -p nokv-fuse`.
- Focused mounted p=4 client-hot after small exact-read admission:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=5 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T192015Z/`.
  NoKV `161492.44 samples/s`, `2523.3193 MiB/s`, p99 `83.91us`; JuiceFS
  `222893.37 samples/s`, `3482.7089 MiB/s`, p99 `68.63us`
  (`1.38x` JuiceFS/NoKV). Decompose shows `prefetch_enqueued=0`, so this row
  confirms the admission gate removed background-prefetch interference, but the
  run remained noisy.
- Current-code `--no-prefetch` control run:
  `NOKV_BENCH_NOKV_MOUNT_OPTIONS=--no-prefetch NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=5 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T192336Z/`.
  NoKV `147892.50 samples/s`, `2310.8204 MiB/s`, p99 `92.62us`; JuiceFS
  `219608.47 samples/s`, `3431.3824 MiB/s`, p99 `72.14us`
  (`1.48x` JuiceFS/NoKV). This did not reproduce the older `--no-prefetch`
  advantage, so the remaining gap is not solely the prefetcher switch.
- Focused mounted p=4 client-hot after `HashMap` primary cache and exact-hit
  fast path:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=5 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T192936Z/`.
  NoKV `179592.67 samples/s`, `2806.1355 MiB/s`, p99 `78.04us`; JuiceFS
  `232744.90 samples/s`, `3636.6391 MiB/s`, p99 `66.37us`
  (`1.30x` JuiceFS/NoKV). Individual NoKV repeats were `136486.70`,
  `222462.61`, `179592.67`, `168586.55`, and `218875.03 samples/s`, so the
  next task is repeat stability and remaining FUSE/cache-copy overhead, not
  re-enabling broad background prefetch.

### 2026-06-14 P7 Read-Plan Materialization for Hot Sparse Windows

- Changed block-cache hot-hit object-key validation to use borrowed raw key
  validation before lookup. Cache hits no longer allocate an owned `ObjectKey`
  only to validate it, and a regression test still rejects `../bad` even when a
  matching cache entry exists.
- Changed `ObjectReadPlanCache` covering hits to materialize the sliced sparse
  request under its exact read-plan key. A full-file plan published by FUSE
  writeback can now seed the first sparse sample window, and later epoch reads
  hit the exact plan without re-scanning the covering full-file plan.
- Increased the FUSE read-plan cache budget from `4096` total plans to
  `128 * 1024`, split across the existing `16` shards. The standard p4 hot
  shape needs room for full-file plans, warm-up sparse-window plans, measured
  sparse-window plans, and uneven inode-to-shard placement.
- Validation passed:
  `cargo test -p nokv-object block_cache_hit_still_validates_object_key`,
  `cargo test -p nokv-object block_cache_reuses_object_reads`,
  `cargo test -p nokv-object block_cache_reuses_covering_ranges`,
  `cargo test -p nokv-object object_read_plan_cache_reuses_covering_plan`,
  `cargo test -p nokv-object object_read_plan_cache_repeated_hits_keep_lru_order`,
  `cargo test -p nokv-fuse client_backend_caches_published_staged_read_plan`,
  `cargo test -p nokv-object`, `cargo test -p nokv-fuse`, and
  `cargo fmt --all`.
- Borrowed-key validation alone did not move the headline p4 client-hot row:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T193900Z/` reported NoKV
  `162731.26 samples/s` vs JuiceFS `227724.24 samples/s` (`1.40x`
  JuiceFS/NoKV). Treat it as low-risk allocation cleanup, not a standalone
  performance win.
- After read-plan materialization and the larger cache budget, the focused
  mounted p=4 client-hot run
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=5 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`
  wrote artifacts under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T194332Z/`.
  Median NoKV was `204800.85 samples/s`, `3200.0133 MiB/s`, p99 `76.12us`;
  JuiceFS was `221202.14 samples/s`, `3456.2834 MiB/s`, p99 `71.05us`
  (`1.08x` JuiceFS/NoKV).
- Individual NoKV repeats were `96544.38`, `214538.25`, `204800.85`,
  `15365.86`, and `217579.64 samples/s`. The severe fourth-repeat outlier had
  `read_plan_cache_hits=1024` and `read_plan_cache_misses=6656`, so the next
  optimization target is read-plan cache miss stability under mounted p4
  client-hot pressure before claiming stable parity.
- After raising the read-plan cache budget to `128 * 1024`, the same focused
  mounted p=4 client-hot run wrote artifacts under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T194929Z/`.
  Median NoKV was `212339.38 samples/s`, `3317.8027 MiB/s`, p99 `76.42us`;
  JuiceFS was `231549.79 samples/s`, `3617.9655 MiB/s`, p99 `66.90us`
  (`1.09x` JuiceFS/NoKV). Individual NoKV repeats were `189094.52`,
  `228935.00`, `208749.72`, `212339.38`, and `224983.86 samples/s`, so the
  severe read-plan miss outlier did not reproduce in this five-repeat gate.
  Decompose for run-1 showed `read_plan_cache_hits=1536` with no
  read-plan-miss delta; the remaining gap is now the normal p4 hot FUSE/cache
  hit cost, not a metadata-plan miss storm.
- A follow-up experiment changed read-plan cache hits to share
  `Arc<ObjectReadPlan>` internally. It passed targeted cache tests but did not
  improve the mounted p4 client-hot benchmark: artifacts under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T195652Z/` reported NoKV
  `179295.25 samples/s` vs JuiceFS `219590.32 samples/s` (`1.22x`
  JuiceFS/NoKV), and
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T195850Z/` reported NoKV
  `186152.91 samples/s` vs JuiceFS `211374.18 samples/s` (`1.14x`
  JuiceFS/NoKV). The experiment was reverted instead of shipping an
  unsupported optimization.
- The reverted-code confirmation run under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T200225Z/` still showed a
  lower p4 hot median: NoKV `145113.79 samples/s`, `2267.4030 MiB/s`, p99
  `96.10us`; JuiceFS `221760.52 samples/s`, `3465.0082 MiB/s`, p99 `71.85us`
  (`1.53x` JuiceFS/NoKV). Decompose showed read-plan misses stayed absent while
  warm-up/measured object/cache hit counters varied (`read_plan_cache_hits`
  `4608-6144`, matching object/cache activity rather than metadata fallback).
  The next step is a benchmark/runtime stabilization pass for p4 hot cache-hit
  behavior before claiming sustained parity.

### 2026-06-14 P7 Hot Warmup Diagnostics

- Extended `bench/drivers/posix_bench.py` so `ai_shard_range_read` hot rows
  report both measured and warm-up physical read shape in `cost_breakdown`:
  `physical_ranges`, `physical_read_bytes`, `checksum`,
  `warmup_physical_ranges`, `warmup_physical_read_bytes`, and
  `warmup_checksum`.
- Added optional `--stats-url` support to `bench/drivers/posix_bench.py` and
  wired NoKV native mounted runs to pass the mount stats endpoint. When present,
  hot rows append `warmup_stats=[...]` and `measured_stats=[...]` so the row can
  distinguish object GETs during the warm-up pass from block-cache hits during
  the measured pass. JuiceFS rows keep the portable physical counters only.
- This does not change timing or cache hints. The measured hot row is still a
  kernel-cache-bypassed pass after a kernel-cache-bypassed warm-up; the new
  fields make it obvious whether the warm-up and measured pass touched the same
  logical windows.
- Local driver smoke passed:
  `python3 bench/drivers/posix_bench.py --system local --mount /tmp/nokv-posix-smoke --metadata-tier local --object-backend local --profile smoke --concurrency 1 --workloads ai_shard_range_read --dataset-dirs 2 --files-per-dir 8 --sample-bytes 512 --checkpoint-bytes 4096 --checkpoint-steps 1 --range-stride 2 --range-coalesce-gap-bytes 512 --cache-states hot --fsync 0 --emit-header 1`.
  The row included `physical_ranges=2`, `physical_read_bytes=7168`,
  `warmup_physical_ranges=2`, and `warmup_physical_read_bytes=7168`.
- First real mounted p4 hot single-repeat validation passed:
  `NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=1 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifacts are under
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T200802Z/`.
  Both NoKV and JuiceFS rows reported measured and warm-up
  `physical_ranges=4096`, `physical_read_bytes=67108864`, and matching
  checksums. NoKV throughput was `169059.72 samples/s` vs JuiceFS
  `217882.97 samples/s` (`1.29x` JuiceFS/NoKV); this one-repeat row validates
  instrumentation only.
- The same run's NoKV decompose sidecar still showed no read-plan miss delta
  and reported `read_plan_cache_hits=3328`, `object_gets=1664`, and
  `cache_hits=1664`.
- Second real mounted p4 hot single-repeat validation passed with the stats URL
  split:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T201143Z/`. The NoKV row
  reported `warmup_stats=[object_gets=2816;object_get_bytes=46137344;read_plan_cache_hits=2816]`
  and
  `measured_stats=[cache_hits=2816;cache_hit_bytes=46137344;block_cache_hits=2816;block_cache_hit_bytes=46137344;read_plan_cache_hits=2816]`.
  Throughput was NoKV `156312.76 samples/s` vs JuiceFS
  `222616.26 samples/s` (`1.42x` JuiceFS/NoKV). The important diagnostic is
  that warm-up and measured physical rows both read `4096` POSIX windows /
  `67108864` bytes, while NoKV mount stats accounted for `2816` windows in each
  pass. The next gap is explaining which `1280` POSIX windows are bypassing the
  object pipeline counters or are served through a path not represented by the
  current mount stats.
- Added FUSE entry counters to mount stats: `fuse_read_requests` and
  `fuse_read_request_bytes`. These counters are recorded at the FUSE `read()`
  handler before the read-handle/object pipeline paths, so they separate
  benchmark-planned POSIX pread windows from requests that actually reach the
  NoKV mount. Unit coverage: `cargo test -p nokv-fuse object_pipeline_stats -- --nocapture`.
- Third real mounted p4 hot single-repeat validation passed after adding the
  entry counters:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T202118Z/`. The NoKV row
  reported `4096` benchmark pread windows / `67108864` bytes, but
  `warmup_stats=[fuse_read_requests=1024;fuse_read_request_bytes=16777216;object_gets=1024;object_get_bytes=16777216;read_plan_cache_hits=1024]`
  and
  `measured_stats=[fuse_read_requests=1024;fuse_read_request_bytes=16777216;cache_hits=1024;cache_hit_bytes=16777216;block_cache_hits=1024;block_cache_hit_bytes=16777216;read_plan_cache_hits=1024]`.
  Throughput was NoKV `183145.35 samples/s` vs JuiceFS
  `221065.85 samples/s` (`1.21x` JuiceFS/NoKV). The current p4 hot row is
  therefore a mixed kernel/FUSE/object-cache path on macOS, not a pure NoKV
  object hot-path measurement. Use `NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io"`
  for the next NoKV-only diagnostic run when isolating object pipeline overhead.

### 2026-06-14 P7 Direct-IO Hot-Path Isolation and Cache Sharding

- Ran the direct-io p4 hot diagnostic requested by the prior gap:
  `NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io --no-kernel-cache" NOKV_AI_L2_MATRIX_PROFILE=standard NOKV_AI_L2_MATRIX_REPEATS=1 NOKV_AI_L2_MATRIX_CONCURRENCY=4 NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read NOKV_AI_L2_MATRIX_CACHE_STATES=hot NOKV_AI_L2_MATRIX_FSYNC=0 scripts/run-ai-dataplane-l2-matrix.sh`.
  Artifact:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T202532Z/`.
  The NoKV row reported all `4096` benchmark pread windows reaching the FUSE
  handler and object/cache pipeline:
  `warmup_stats=[fuse_read_requests=4096;fuse_read_request_bytes=67108864;object_gets=4096;object_get_bytes=67108864;read_plan_cache_hits=4096]`
  and
  `measured_stats=[fuse_read_requests=4096;fuse_read_request_bytes=67108864;cache_hits=4096;cache_hit_bytes=67108864;block_cache_hits=4096;block_cache_hit_bytes=67108864;read_plan_cache_hits=4096]`.
  Throughput was NoKV `136672.85 samples/s` vs JuiceFS `217032.98 samples/s`;
  this is a NoKV object hot-path diagnostic, not a symmetric mount-cache
  comparison because only NoKV was forced into direct I/O.
- Changed `MemoryBlockCache` from one global mutex to sharded per-object-key
  state for default training-size caches. Each shard owns its LRU order, range
  index, stats, and byte/item budget slice; small caches remain single-shard so
  exact capacity tests keep their semantics. Coverage:
  `cargo test -p nokv-object memory_block_cache -- --nocapture` and
  `cargo test -p nokv-object block_cache -- --nocapture`.
- Post-change direct-io p4 hot single-repeat validation:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T203343Z/`. Throughput was
  effectively unchanged (`136618.91 samples/s`) but p99 improved from
  `112.21us` to `84.80us` in this one-repeat diagnostic. The same stats still
  show `4096` FUSE requests, `4096` block-cache hits, and `4096` read-plan cache
  hits in the measured pass, so throughput is now dominated by fixed per-read
  FUSE/direct-io/copy cost rather than by one obvious shared cache mutex.
- Direct-io with sparse-read prefetch disabled:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T203430Z/`, using
  `NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io --no-kernel-cache --no-prefetch"`.
  NoKV improved slightly to `140268.37 samples/s`, with the same `4096`
  measured block-cache hits. This suggests the read coordinator/prefetch path is
  small overhead for sparse hot reads and not the main bottleneck.
- Default mounted p4 hot single-repeat after the cache sharding change:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T203452Z/`. NoKV reported
  `212282.06 samples/s` vs JuiceFS `224181.41 samples/s` (`1.06x`
  JuiceFS/NoKV), but only `256` FUSE/object-cache reads reached NoKV in warm-up
  and measured stats. This remains the end-to-end macOS mounted hot-cache row,
  not a pure object hot-path row.

### 2026-06-14 P7 Direct-IO FUSE Limit Check

- Added a no-prefetch read-handle fast path: when FUSE prefetch is disabled,
  read-only handles use the already-opened inode attr/generation and call the
  backend read-plan/cache path directly instead of locking and updating
  `FileReadPipeline`. Coverage:
  `cargo test -p nokv-fuse read_handle -- --nocapture`. This removes
  unnecessary state from sparse direct reads, but it should not be counted as a
  throughput win without benchmark evidence.
- On macOS, `--direct-io` now also passes macFUSE `direct_io` and
  `iosize=1048576` mount options. Coverage:
  `cargo test -p nokv-fuse macos_ -- --nocapture`. The local macFUSE binary
  advertises `direct_io` and `iosize=<size>`, but this did not change the
  observed direct-io read split for the packed-shard benchmark.
- Ordinary direct-io/no-prefetch p4 hot validation after the fast path:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T204650Z/`. NoKV reported
  `138859.03 samples/s`, p99 `83.85us`, and the expected `4096`
  `fuse_read_requests`, `4096` block-cache hits, and `4096` read-plan cache
  hits. This is the same performance class as the previous direct-io
  diagnostics.
- Coalesced sparse-window direct-io validation:
  `NOKV_BENCH_RANGE_COALESCE_GAP_BYTES=16384` with
  `NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io --no-kernel-cache --no-prefetch"`.
  Artifact:
  `bench/results/ai-dataplane-l2-matrix-Mac-20260613T204620Z/`. The benchmark
  planned only `32` POSIX pread windows, but NoKV still received `8160`
  `fuse_read_requests` / `133693440` bytes in both warm-up and measured stats.
  Throughput fell to `93820.95 samples/s` because macOS FUSE/direct-io split
  large reads back into 16KiB callbacks. This is a negative result: POSIX
  coalescing through macOS FUSE is not the right path for the AI-native fast
  read plane. The next useful direction is the SDK/fsspec/native batched read
  path, where NoKV controls range planning and object-cache hits without this
	  per-16KiB FUSE callback boundary.

### 2026-06-14 P7 Native Batch Range Read

- Added `PathReadRange`, `PathRangeReadRequest`, and
  `NoKvFsClient::read_path_ranges_batch`. The SDK now accepts a training batch
  of many shard files plus sparse sample ranges, coalesces each file's ranges
  into windows, issues one metadata `OpenPathReadPlanBatch` for all windows, and
  reads distinct shard files in parallel while preserving per-file window order.
- Kept the boundary clean: metadata still returns immutable read plans, object
  storage still owns object/block reads and cache attribution, and the client is
  the only layer that knows this is a dataloader-style batch.
- Updated `ai-shard-range-read` so the timed phase calls the SDK batch range
  primitive in `object_concurrency`-sized batches. The row shape now records
  `range_batch_open=true`; this is the native fast path to compare against the
  mounted FUSE rows.
- Coverage:
  `cargo test -p nokv-client read_path_ranges -- --nocapture` passed, including
  the new single-batch-open range test. `cargo test -p nokv-bench --bin
  nokv-bench -- --nocapture`, `cargo check -p nokv-bench --release`, and
  `cargo fmt --all -- --check` passed.
- RustFS smoke:
  `NOKV_E2E_PROFILE=smoke NOKV_E2E_WORKLOAD=ai-shard-range-read
  NOKV_E2E_OBJECT_CONCURRENCY=4 NOKV_E2E_CARGO_TARGET_DIR=target
  NOKV_E2E_RUSTFS_ADDRESS=127.0.0.1:9050
  NOKV_E2E_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9051 scripts/run-rustfs-e2e.sh
  --sample-bytes 4096 --range-stride 2 --range-coalesce-gap-bytes 512`
  passed. It reported cold `1552.36 samples/s` and warm
  `144530.70 samples/s` with `range_batch_open=true`, `shard_count=8`,
  `batch_size=4`, `selected_samples_per_shard=32`, and `sample_bytes=4096`.
  This is an L1 native RustFS/local-hot smoke, not a JuiceFS comparison row.

### 2026-06-14 P7 Python/fsspec Batch Range Binding

- Added `crates/nokv-python` to the workspace. It owns the Python SDK/fsspec
  training surface and depends upward only on `nokv-client` and `nokv-object`.
- Added the PyO3 `nokv._native.Client` binding. The binding opens a configured
  S3 or tiered-local object store, connects to the metadata service, and exposes
  `read_ranges_batch`, which calls `NoKvFsClient::read_path_ranges_batch`
  directly.
- Added `python/nokv/fsspec.py` with `NoKVFileSystem`. The fsspec surface
  forwards semantic range batches to the native binding and requires explicit
  end offsets for `cat_file`, keeping it on the training-read path rather than
  silently falling back to POSIX.
- Updated the package-boundary contract so `crates/nokv-python` cannot own
  metadata layout, object-provider behavior, FUSE behavior, or duplicate Rust
  SDK range planning.
- Coverage:
  `cargo test -p nokv-python -- --nocapture`, `cargo check -p nokv-python
  --features extension-module`, and `python3 -m py_compile
  crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py` passed. A source-level fsspec smoke
  with an injected fake `nokv._native.Client` also passed and verified
  `read_ranges_batch`, explicit-end `cat_file`, and error behavior for missing
  end offsets.
- Added `scripts/run-python-fsspec-smoke.sh`. It builds `nokv`, creates a
  temporary venv, installs `maturin` and `fsspec`, builds the Python extension
  with `maturin develop --release`, starts RustFS plus `nokv serve`, writes a
  packed shard through the CLI, and reads semantic sample ranges through
  `nokv.fsspec.NoKVFileSystem.read_ranges_batch`.
- Live smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9062
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9063
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7784
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  scripts/run-python-fsspec-smoke.sh` passed. It built the
  `nokv-0.1.0-cp39-abi3-macosx_11_0_arm64.whl` wheel, installed it editable in
  the smoke venv, and printed `NoKV Python fsspec live smoke passed`.

### 2026-06-14 P7 Python Native Benchmark Rows

- Added `bench/drivers/native_fsspec_bench.py`. It drives
  `nokv.fsspec.NoKVFileSystem.read_ranges_batch` directly and emits canonical
  `boundary=L1` rows for the Python native SDK path. The row shape records
  shard count, samples per shard, selected samples, sample bytes, range stride,
  coalescing gap, and `range_batch_size`; the cost breakdown records
  `sdk=python-fsspec`, `range_batch_open=true`, batch calls, semantic ranges,
  semantic bytes, checksum, max gap, block-cache mode, and warm-up counters for
  warm/hot rows.
- Updated `scripts/run-python-fsspec-smoke.sh` to seed a configurable packed
  shard dataset through the deployable CLI, keep the explicit fsspec
  correctness check, then run the native fsspec benchmark driver. The script now
  supports `NOKV_PYTHON_SMOKE_SHARD_COUNT`,
  `NOKV_PYTHON_SMOKE_FILES_PER_DIR`, `NOKV_PYTHON_SMOKE_SAMPLE_BYTES`,
  `NOKV_PYTHON_SMOKE_RANGE_STRIDE`, `NOKV_PYTHON_SMOKE_CONCURRENCY`,
  `NOKV_PYTHON_SMOKE_CACHE_STATES`, and `NOKV_PYTHON_SMOKE_RESULT_CSV`.
- Live smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9066
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9067
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7786
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny smoke emitted
  `boundary=L1`, `tool=native`, `workload=ai_shard_range_read` rows: cold
  `4233.83 samples/s`, `4.1346 MiB/s`, p99 `1876.50us`; warm
  `22630.83 samples/s`, `22.1004 MiB/s`, p99 `345.29us`.
- Caveat: this smoke validates the deployable Python native benchmark surface.
  It is client-cache-state L1 evidence, not an L2 NoKV-vs-JuiceFS mount row and
  not a throughput-limit result.

### 2026-06-14 P7 Layered Data-Plane Matrix

- Added `scripts/merge-layered-benchmark-csv.py`. It merges benchmark CSVs with
  different headers, adds `benchmark_layer`, `source_script`, and `layer_case`,
  and aggregates with those dimensions plus any L2 `matrix_case` / `fsync` /
  `cache_state_scope` fields in the grouping key. This prevents Rust SDK L1,
  Python/fsspec L1, NoKV FUSE L2, and JuiceFS L2 rows from being collapsed into
  one aggregate.
- Added `scripts/run-ai-dataplane-layered-matrix.sh`. The wrapper can run Rust
  SDK L1 through `scripts/run-rustfs-e2e.sh`, Python/fsspec L1 through
  `scripts/run-python-fsspec-smoke.sh`, and mounted L2 through
  `scripts/run-ai-dataplane-l2-matrix.sh`. The default cases are
  `sparse-exact` and `sparse-coalesced`; `large-window` validates 1 MiB native
  semantic windows and skips L2 because the mounted L2 profile owns sample
  size.
- Updated `scripts/run-python-fsspec-smoke.sh` with
  `NOKV_PYTHON_SMOKE_METADATA_TIER`, `NOKV_PYTHON_SMOKE_OBJECT_BACKEND`, and
  `NOKV_PYTHON_SMOKE_HOT_OBJECT_ROOT`, so layered runs can label Python rows as
  `nokv-l1-service` over `rustfs+local-hot+put=cold-then-hot` and use a local
  hot object root.
- L1-only smoke:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_PROFILE=smoke
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-layered-smoke
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. It wrote
  `/tmp/nokv-layered-smoke/layered.raw.csv` and
  `/tmp/nokv-layered-smoke/layered.aggregate.csv`.
- Smoke rows for `sparse-exact`, smoke shape (`8` shards, `64` samples/shard,
  `512` bytes/sample, every second sample, exact windows): Rust SDK L1 cold
  `1721.25 samples/s`, warm `144029.23 samples/s`; Python/fsspec L1 cold
  `10307.20 samples/s`, warm `45999.04 samples/s`. This is a wrapper and
  layering validation run, not a standard-size L2 JuiceFS comparison.
- L2-enabled smoke:
  `NOKV_AI_LAYERED_PROFILE=smoke
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-layered-l2-smoke
  NOKV_AI_LAYERED_L2_CONCURRENCY=1
  NOKV_AI_LAYERED_L2_CACHE_STATES=cold
  NOKV_AI_LAYERED_L2_FSYNC=0
  NOKV_AI_LAYERED_L2_DECOMPOSE=1
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. It wrote
  `/tmp/nokv-layered-l2-smoke/layered.raw.csv`,
  `/tmp/nokv-layered-l2-smoke/layered.aggregate.csv`, the L2 raw/aggregate/env
  CSVs, and the NoKV decompose sidecar under
  `/tmp/nokv-layered-l2-smoke/sparse-exact/l2/`.
- L2 smoke rows for `sparse-exact`, `fsync=0`, `cache_state=cold`, `p=1`:
  NoKV FUSE `47155.98 samples/s`, `23.0254 MiB/s`, p99 `59.73us`; JuiceFS FUSE
  `26611.57 samples/s`, `12.9939 MiB/s`, p99 `222.72us`. This is a tiny local
  mounted smoke with one repeat, not a standard profile result or an official
  MLPerf/DLIO result.
- Standard layered run:
  `NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-layered-standard-sparse-exact
  NOKV_AI_LAYERED_L2_CONCURRENCY="1 4"
  NOKV_AI_LAYERED_L2_CACHE_STATES="cold warm hot"
  NOKV_AI_LAYERED_L2_FSYNC=0
  NOKV_AI_LAYERED_L2_REPEATS=2
  NOKV_AI_LAYERED_L2_DECOMPOSE=1
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. The combined
  artifacts are `/tmp/nokv-layered-standard-sparse-exact/layered.raw.csv` and
  `/tmp/nokv-layered-standard-sparse-exact/layered.aggregate.csv`.
- Standard `sparse-exact` L1 rows (`32` shards, `256` samples/shard,
  `16 KiB` samples, every second sample, exact windows): Rust SDK L1 cold
  `5338.32 samples/s`, warm `138820.01 samples/s`; Python/fsspec L1 cold
  `5710.82 samples/s`, warm `45778.92 samples/s`. This shows Python/fsspec is
  useful as a training integration surface but still has clear binding/batch
  overhead against the Rust SDK warm path.
- Standard `sparse-exact` L2 aggregate, `fsync=0`, `2` repeats: p=1 cold NoKV
  `25215.20 samples/s` vs JuiceFS `18110.03 samples/s` (`1.39x` NoKV);
  p=4 cold NoKV `33775.16` vs JuiceFS `45389.76` (`1.34x` JuiceFS).
  p=1 warm NoKV `277155.43` vs JuiceFS `170027.49` (`1.63x` NoKV);
  p=4 warm NoKV `216503.89` vs JuiceFS `229117.87` (`1.06x` JuiceFS).
  p=1 hot NoKV `193430.30` vs JuiceFS `177698.58` (`1.09x` NoKV);
  p=4 hot NoKV `189550.23` vs JuiceFS `217118.96` (`1.15x` JuiceFS).
  The p=4 rows make the next optimization target concrete: NoKV's mounted
  sparse-reader path still loses to JuiceFS under concurrency even when the p=1
  path is ahead.

### 2026-06-14 P7 FUSE Handle-Local Read-Plan Cache

- Added a per-read-handle `ObjectReadPlanCache` in the FUSE read path. When a
  shard file has a published full-file read plan in the backend cache, the first
  small `pread` window seeds that full plan into the handle-local cache, so the
  remaining sample windows on the same open file handle can slice the plan
  without repeatedly contending on the shared backend read-plan cache.
- Added `client_backend_seeds_handle_read_plan_cache_from_full_file_plan`,
  proving the backend seeds the handle cache from the full-file plan and serves
  subsequent sparse windows without metadata misses.
- Focused same-shape mounted L2 p=4 exact run:
  `NOKV_AI_L2_MATRIX_PROFILE=standard
  NOKV_AI_L2_MATRIX_RESULT_DIR=/tmp/nokv-p4-handle-plan-cache-exact
  NOKV_AI_L2_MATRIX_CONCURRENCY=4
  NOKV_AI_L2_MATRIX_CACHE_STATES="cold warm hot"
  NOKV_AI_L2_MATRIX_FSYNC=0
  NOKV_AI_L2_MATRIX_REPEATS=2
  NOKV_AI_L2_MATRIX_DECOMPOSE=1
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  scripts/run-ai-dataplane-l2-matrix.sh` passed. Artifacts are under
  `/tmp/nokv-p4-handle-plan-cache-exact/`, with combined
  `matrix.raw.csv` and `matrix.aggregate.csv`.
- Against the prior `/tmp/nokv-layered-standard-sparse-exact` p=4 exact rows:
  cold NoKV moved from `33775.16` to `35549.50 samples/s` and p99 from
  `292.72us` to `213.06us`; JuiceFS remains ahead at `44785.01 samples/s`
  (`1.26x`). Warm NoKV moved from `216503.89` to
  `223583.36 samples/s`, effectively matching JuiceFS `222495.44 samples/s`.
  Hot NoKV moved from `189550.23` to `208026.34 samples/s`, narrowing the
  JuiceFS gap to `1.04x`, though NoKV p99 was `86.53us` vs JuiceFS `79.27us`.
- Decompose for the exact cold p=4 rows still shows `4096` FUSE requests,
  `4096` object GETs, `4096` read-plan cache hits, and no read amplification.
  The remaining cold gap is therefore mounted per-request/object-read overhead,
  not extra bytes read. Warm/hot rows are still affected by macOS kernel-cache
  visibility, so treat the hot comparison as a local engineering signal rather
  than a portable production claim.

### 2026-06-14 P7 Sparse-Forward Segment Warmup

- Changed `FileReadPipeline` to recognize a proven sparse-forward stream: after
  multiple small forward reads on the same open file, it can submit a 1MiB
  exact segment warmup for the current object block. A single random read and a
  large forward jump still stay on the exact foreground path.
- Added `file_read_pipeline_warms_cache_for_proven_sparse_forward_stream` and
  `file_read_pipeline_skips_sparse_warmup_after_large_forward_jump` to preserve
  that admission rule. Existing initial-random and continued-small-read tests
  still cover the conservative cases.
- Focused mounted L2 p=4 exact cold gate:
  `NOKV_AI_L2_MATRIX_PROFILE=standard
  NOKV_AI_L2_MATRIX_RESULT_DIR=/tmp/nokv-p4-sparse-warmup-cold-exact-5
  NOKV_AI_L2_MATRIX_CONCURRENCY=4
  NOKV_AI_L2_MATRIX_CACHE_STATES=cold
  NOKV_AI_L2_MATRIX_FSYNC=0
  NOKV_AI_L2_MATRIX_REPEATS=5
  NOKV_AI_L2_MATRIX_DECOMPOSE=1
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  scripts/run-ai-dataplane-l2-matrix.sh` passed. Median NoKV cold p=4 was
  `63892.80 samples/s`, `998.3251 MiB/s`, p99 `265.29us`; JuiceFS was
  `45201.34 samples/s`, `706.2709 MiB/s`, p99 `227.04us`. This is
  `1.41x` NoKV throughput on the same mounted exact sparse shape.
- Decompose for the cold p=4 five-repeat gate shows the intended tradeoff:
  NoKV still sees `4096` FUSE read requests, but object GET count drops from the
  previous `4096` exact foreground GETs to roughly `463-478` total object GETs,
  with `128-131` prefetch segment requests and about `3747-3761` cache hits per
  repeat. The path reads more total object bytes because 1MiB segments are
  warmed, but AI shard throughput improves because request count dominates this
  mounted sparse workload.
- Warm p=4 two-repeat check under
  `/tmp/nokv-p4-sparse-warmup-exact/` stayed at parity: NoKV
  `221222.10 samples/s` vs JuiceFS `221556.98 samples/s`. Hot p=4 five-repeat
  check under `/tmp/nokv-p4-sparse-warmup-hot-exact-5/` was NoKV
  `205955.49 samples/s`, p99 `74.50us`, vs JuiceFS `228505.54 samples/s`,
  p99 `72.08us` (`1.11x` JuiceFS). Hot remains the tail-stability target.

### 2026-06-14 P7 Hot Coverage and Range Alias

- Extended `bench/drivers/posix_bench.py` hot rows with
  `warmup_stats_coverage=[observed_fuse_read_requests=...
  expected_fuse_read_requests=... coverage=...]` and
  `measured_stats_coverage=[...]`. This makes the mounted hot row explicit
  about whether timed reads reached NoKV's FUSE/object path.
- Default mounted p=4 hot coverage gate:
  `NOKV_AI_L2_MATRIX_PROFILE=standard
  NOKV_AI_L2_MATRIX_RESULT_DIR=/tmp/nokv-p4-hot-coverage-gate
  NOKV_AI_L2_MATRIX_CONCURRENCY=4
  NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read
  NOKV_AI_L2_MATRIX_CACHE_STATES=hot
  NOKV_AI_L2_MATRIX_FSYNC=0
  NOKV_AI_L2_MATRIX_REPEATS=1
  NOKV_AI_L2_MATRIX_DECOMPOSE=1
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  scripts/run-ai-dataplane-l2-matrix.sh` passed. The NoKV row reported
  `measured_stats_coverage=[observed_fuse_read_requests=0
  expected_fuse_read_requests=4096 coverage=0.0000]`, so this default macOS hot
  row is an end-to-end mount-cache signal, not a NoKV object hot-path result.
- Added memory-block-cache exact-range aliases: when a small sparse read hits a
  larger cached segment, NoKV records an alias from the exact sample range to
  the segment instead of copying the 16KiB sample into a second cache entry.
  `block_cache_reuses_covering_ranges` now preserves this no-duplicate-put
  behavior; eviction still drops aliases when the target segment is removed.
- Changed the mounted FUSE read-handle read-plan path to slice directly from a
  cached full-file plan. The handle-local cache keeps the full plan instead of
  inserting one sliced read-plan entry for every 16KiB sample window. Added
  `ObjectReadPlanCache::get_exact` and `get_slice_from` so this fast path does
  not rely on an LRU reverse scan.
- NoKV-only direct-io p=4 hot diagnostic:
  `NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io --no-kernel-cache"
  NOKV_AI_L2_MATRIX_PROFILE=standard
  NOKV_AI_L2_MATRIX_RESULT_DIR=/tmp/nokv-p4-hot-range-alias-directio-5
  NOKV_AI_L2_MATRIX_CONCURRENCY=4
  NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read
  NOKV_AI_L2_MATRIX_CACHE_STATES=hot
  NOKV_AI_L2_MATRIX_FSYNC=0
  NOKV_AI_L2_MATRIX_REPEATS=5
  NOKV_AI_L2_MATRIX_DECOMPOSE=1
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  scripts/run-ai-dataplane-l2-matrix.sh` passed. Median NoKV was
  `132884.91 samples/s`, `2076.3267 MiB/s`, p99 `96.78us`, with measured
  coverage `4096/4096` and measured `cache_hits=4096`. JuiceFS in the same
  artifact was `226362.95 samples/s`, p99 `70.07us`; this remains a
  NoKV-only direct-io diagnostic because the direct-io mount option applies to
  NoKV.
- After the read-plan slice fast path, a second NoKV-only direct-io p=4 hot
  five-repeat gate under `/tmp/nokv-p4-hot-plan-slice-directio-5/` passed.
  Median NoKV was `134393.67 samples/s`, `2099.9012 MiB/s`, p99 `103.06us`,
  with measured coverage `4096/4096` and measured `cache_hits=4096`. This is a
  small median gain over the range-alias baseline; the p99 is still noisy, and
  the remaining direct-io hot gap is FUSE/cache-hit per-read overhead rather
  than metadata or object-store work.
- Cold p=4 five-repeat regression gate:
  `NOKV_AI_L2_MATRIX_PROFILE=standard
  NOKV_AI_L2_MATRIX_RESULT_DIR=/tmp/nokv-p4-cold-range-alias-5
  NOKV_AI_L2_MATRIX_CONCURRENCY=4
  NOKV_AI_L2_MATRIX_WORKLOADS=ai_shard_range_read
  NOKV_AI_L2_MATRIX_CACHE_STATES=cold
  NOKV_AI_L2_MATRIX_FSYNC=0
  NOKV_AI_L2_MATRIX_REPEATS=5
  NOKV_AI_L2_MATRIX_DECOMPOSE=1
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  scripts/run-ai-dataplane-l2-matrix.sh` passed. Median NoKV cold p=4 was
  `63872.30 samples/s`, `998.0046 MiB/s`, p99 `243.12us`; JuiceFS was
  `44271.09 samples/s`, `691.7358 MiB/s`, p99 `251.82us`. This is `1.44x`
  NoKV throughput and confirms aliases avoid the cold-path regression seen when
  exact samples were duplicated as cache blocks.
- After the read-plan slice fast path, the cold p=4 five-repeat regression gate
  under `/tmp/nokv-p4-cold-plan-slice-5/` also passed. Median NoKV cold p=4 was
  `64257.53 samples/s`, `1004.0239 MiB/s`, p99 `233.05us`; JuiceFS was
  `45200.88 samples/s`, `706.2637 MiB/s`, p99 `258.95us`, leaving NoKV at
  `1.42x` throughput on the same mounted exact sparse shape.

### 2026-06-14 P7 Typed Range Cache Index

- Reworked the memory block cache's hot range lookup to keep typed exact-range
  and exact-range-alias indexes keyed by `(object_key, offset, len)`. Exact
  16KiB shard-sample hits no longer need to rebuild the
  `object_key:offset:len` string before lookup, and small aliases still point
  at the existing cached segment rather than duplicating bytes.
- Strengthened `memory_block_cache_range_index_drops_evicted_entries` so it
  first creates a covering-range alias, then evicts the target block and checks
  both the exact range and alias range miss afterward. This protects the new
  typed indexes from retaining stale object references.
- Validation passed:
  `cargo fmt --all -- --check`,
  `cargo test -p nokv-object`,
  and `cargo test -p nokv-fuse`.
- NoKV-only direct-io p=4 hot diagnostic:
  `NOKV_AI_L2_PROFILE=standard
  NOKV_AI_L2_REPEATS=5
  NOKV_AI_L2_CONCURRENCY=4
  NOKV_AI_L2_WORKLOADS=ai_shard_range_read
  NOKV_AI_L2_CACHE_STATES=hot
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  NOKV_BENCH_NOKV_MOUNT_OPTIONS="--direct-io --no-kernel-cache"
  NOKV_AI_L2_RESULT_DIR=/tmp/nokv-p4-hot-typed-index-directio-5
  scripts/run-ai-dataplane-l2.sh` passed. Median NoKV was
  `140714.51 samples/s`, `2198.6642 MiB/s`, p99 `100.34us`, with measured
  coverage `4096/4096`, `4096` block-cache hits, and `4096` read-plan hits.
  JuiceFS in the same artifact was `226729.98 samples/s`, p99 `69.29us`; this
  remains a NoKV-only direct-io diagnostic because the direct-io mount option is
  applied only to NoKV.
- Cold p=4 five-repeat regression gate:
  `NOKV_AI_L2_PROFILE=standard
  NOKV_AI_L2_REPEATS=5
  NOKV_AI_L2_CONCURRENCY=4
  NOKV_AI_L2_WORKLOADS=ai_shard_range_read
  NOKV_AI_L2_CACHE_STATES=cold
  NOKV_AI_L2_RANGE_COALESCE_GAP_BYTES=0
  NOKV_AI_L2_RESULT_DIR=/tmp/nokv-p4-cold-typed-index-5
  scripts/run-ai-dataplane-l2.sh` passed. Median NoKV cold p=4 was
  `65127.78 samples/s`, `1017.6215 MiB/s`, p99 `247.69us`; JuiceFS was
  `44669.44 samples/s`, `697.9601 MiB/s`, p99 `205.64us`, leaving NoKV at
  `1.46x` throughput on the same mounted exact sparse shape.

### 2026-06-14 P7 Python Native Stats

- Exposed `stats()` on the PyO3 `nokv._native.Client` and
  `nokv.fsspec.NoKVFileSystem`. The stats surface reports SDK object GET/cache
  counters, prefetch counters, read-plan cache hits/misses, manifest counts,
  and data-fabric read-placement counters from the same `NoKvFsClient` used by
  `read_ranges_batch`.
- Updated `bench/drivers/native_fsspec_bench.py` so each L1 Python/fsspec row
  records `warmup_stats=[...]` and `measured_stats=[...]` deltas when stats are
  available. This makes the native training path explainable without using
  mounted FUSE `/stats` endpoints.
- Strengthened `scripts/run-python-fsspec-smoke.sh` so the live smoke verifies
  that `fs.stats()` includes object, cache, read-plan, and data-fabric fields
  after the correctness read.
- Validation passed:
  `cargo test -p nokv-python -- --nocapture`,
  `cargo check -p nokv-python --features extension-module`, and
  `python3 -m py_compile
  crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`.
- Live RustFS-backed Python/fsspec smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9070
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9071
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7790
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-stats-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The cold row reported
  `measured_stats=[object_gets=5;object_get_bytes=12288;cache_hits=4;...]`;
  the warm row reported measured cache hits only:
  `measured_stats=[cache_hits=8;cache_hit_bytes=8192;...]`. This is tiny L1
  path validation, not a throughput-limit result.

### 2026-06-14 P7 Python Packed Range Return Shape

- Added `NoKvFsClient::read_path_ranges_batch_packed`. It accepts the same
  semantic batch requests as `read_path_ranges_batch`, uses one metadata
  `OpenPathReadPlanBatch` for the coalesced range windows, and fills one
  contiguous output buffer per requested shard path instead of returning one
  `Vec<u8>` per semantic sample.
- Added `nokv._native.Client.read_ranges_batch_packed` and
  `nokv.fsspec.NoKVFileSystem.read_ranges_batch_packed`. The Python wrapper
  keeps range planning in the Rust SDK and exposes the packed shape directly to
  dataloader code.
- Updated `bench/drivers/native_fsspec_bench.py` with
  `--read-shape ranges|packed`. Packed rows compute checksum from windows in the
  contiguous buffer and record `read_shape=packed` in both shape and cost
  breakdown. `scripts/run-python-fsspec-smoke.sh` and
  `scripts/run-ai-dataplane-layered-matrix.sh` pass this through as
  `NOKV_PYTHON_SMOKE_READ_SHAPE` /
  `NOKV_AI_LAYERED_PYTHON_READ_SHAPE`.
- Coverage:
  `cargo test -p nokv-client
  service_file_client_read_path_ranges_batch_packed_uses_single_batch_open
  -- --nocapture`, `cargo test -p nokv-python -- --nocapture`,
  `cargo check -p nokv-python --features extension-module`, and
  `python3 -m py_compile
  crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py` passed.
- Live packed Python smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9090
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9091
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7800
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=packed
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-packed-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed.
- Standard L1-only baseline before packed shape:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-l1-standard-stats-sparse-exact
  NOKV_AI_LAYERED_RUST_OBJECT_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. Rust SDK L1
  was cold `15392.93 samples/s`, warm `136392.02 samples/s`; Python/fsspec
  ranges mode was cold `19560.26 samples/s`, warm `45729.28 samples/s`. The
  warm Python row had `4096` SDK cache hits and no measured object GETs.
- Larger Python batch size did not fix the gap:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-batch32-sparse-exact
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=32
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. Python warm
  was `43937.28 samples/s`, so reducing benchmark-level batch calls from `8`
  to `1` was not the limiter.
- Standard packed Python row:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-sdk-packed-sparse-exact
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=packed
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. Packed mode
  was cold `13696.40 samples/s`, warm `45649.14 samples/s`; warm stats again
  showed `4096` SDK cache hits and no measured object GETs.
- Interpretation: packed output is the right API shape for future contiguous
  dataloader buffers and GPU staging, but it does not by itself close the
  Python warm-path gap. On this standard sparse-exact workload, the remaining
  gap is above object IO: PyO3 `PyBytes` copy/return cost, Python-side checksum
  and driver work, or the Rust SDK buffer materialization still paid before the
  Python object is created. The next native training step should avoid Python
  per-batch byte materialization entirely, for example with a Rust-native
  dataloader path or a borrowed/buffer-protocol staging surface.

### 2026-06-14 P7 Python Caller-Owned Buffer Staging

- Added `NoKvFsClient::read_path_ranges_batch_into`. It accepts semantic
  `PathRangeReadRequest` batches plus caller-provided request offsets and writes
  each request's packed shard bytes into the supplied contiguous output buffer.
  The method still batch-opens all coalesced range windows through one metadata
  `OpenPathReadPlanBatch`, rejects overlapping output regions before contacting
  metadata, and keeps distinct-path object reads parallel.
- Added `nokv._native.Client.read_ranges_batch_into` and
  `nokv.fsspec.NoKVFileSystem.read_ranges_batch_into`. The fsspec wrapper can
  allocate a bytearray or reuse a caller-provided bytearray and returns
  `(offset, len)` slices for each request. The current PyO3 method keeps the GIL
  while mutating the bytearray; this is safe for CPython `bytearray` memory but
  is not the final pinned-buffer or GPU-direct design.
- Extended `bench/drivers/native_fsspec_bench.py` to
  `--read-shape ranges|packed|into`. The `into` mode reuses one bytearray per
  batch and computes checksum from returned `(offset, len)` windows.
  `scripts/run-python-fsspec-smoke.sh` now verifies `read_ranges_batch_into`
  correctness in the live RustFS smoke, and
  `scripts/run-ai-dataplane-layered-matrix.sh` passes
  `NOKV_AI_LAYERED_PYTHON_READ_SHAPE=into`.
- Coverage:
  `cargo test -p nokv-client
  service_file_client_read_path_ranges_batch_into -- --nocapture`,
  `cargo check -p nokv-python --features extension-module`, and
  `cargo test -p nokv-python -- --nocapture` passed.
- Live into-buffer Python smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=into
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-into-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `3394.98 samples/s`, warm `21036.47 samples/s`; this is smoke validation, not
  a throughput-limit result.
- Standard L1-only into row:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-into-sparse-exact
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=into
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. Into-buffer
  mode was cold `14034.93 samples/s`, warm `47419.50 samples/s`; warm stats
  again showed `4096` SDK cache hits and no measured object GETs.
- Interpretation: caller-owned staging is the right API direction and gives a
  small warm-path improvement over `ranges`/`packed`, but it does not close the
  Rust SDK gap (`136392.02 samples/s` warm on the same earlier standard row).
  The remaining copy is now inside the SDK/object executor path:
  `LayoutReadExecutor` still materializes a `Vec<u8>` for each coalesced window
  before the client copies it into the caller buffer. The next real data-plane
  optimization should add a write-into-buffer object read executor and then
  expose the same contract to pinned host memory or HBM-backed buffers.

### 2026-06-14 P7 Object Executor Direct-Write Windows

- Added `read_object_blocks_into_with_cache_options` and
  `BlockReadIntoOutcome` in `nokv-object`. The direct-write path keeps object
  range validation, coalesced GET accounting, cache-hit accounting, cache fill,
  and sparse-hole zero-fill semantics while writing into caller memory instead
  of allocating the final output `Vec<u8>`.
- Added `ChunkStore::read_blocks_into_with_options`,
  `FileReadPipeline::read_blocks_into_with_options`, and
  `LayoutReadExecutor::read_plan_into_with_options`, preserving pipeline
  readahead, cache warmup, placement stats, and data-fabric counters for
  direct-write reads.
- Updated `NoKvFsClient::read_path_ranges_batch_packed` and
  `read_path_ranges_batch_into` so exact single-range windows write directly
  into the packed output buffer. Multi-range coalesced windows still fall back
  to the old window-buffer path because they require a scatter plan from
  window-relative bytes into packed semantic ranges.
- Coverage:
  `cargo test -p nokv-object read_object_blocks_into -- --nocapture`,
  `cargo test -p nokv-client
  service_file_client_read_path_ranges_batch_into -- --nocapture`, and
  `cargo check -p nokv-python --features extension-module` passed.
- Standard L1-only direct-write into row:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-direct-into-sparse-exact
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=into
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-exact` passed. Into-buffer
  direct-write mode was cold `18629.54 samples/s`, warm
  `47763.47 samples/s`; warm stats again showed `4096` SDK cache hits and no
  measured object GETs.
- Interpretation: exact-window direct-write removes one internal output
  allocation/copy and helps cold rows more than warm rows on this local run.
  The warm path remains far below Rust SDK L1 because Python still calls through
  PyO3 per batch, holds a mutable bytearray staging object, computes checksum in
  Python, and the cache hit path still returns owned bytes from the block cache.
  The next useful step is a scatter direct-write plan for coalesced windows plus
  a borrowed or pinned buffer abstraction that can later back host-pinned memory,
  RDMA memory registration, or GPU/HBM staging.

### 2026-06-14 P7 Guarded Scatter Direct-Write

- Added a packed scatter plan for `read_path_ranges_batch_packed` and
  `read_path_ranges_batch_into`. The client maps window-relative object blocks
  into the dense packed output region and can call the object executor's
  direct-write path without first materializing a full coalesced window buffer.
- Kept the optimization guarded: if scatter would create more physical read
  blocks than the metadata/object plan already has, the client falls back to the
  coalesced window read and copies out the requested semantic ranges. This keeps
  cold object-store rows from trading one window read for thousands of tiny
  object GETs.
- Added
  `service_file_client_read_path_ranges_batch_into_keeps_coalesced_gap_window`
  to pin that contract. A coalesced gap request for ranges `(1,2)` and `(5,2)`
  opens one window, returns packed bytes `bcfg`, and records one planned block,
  one object GET, and six object bytes.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `cargo test -p nokv-client
  service_file_client_read_path_ranges_batch_into -- --nocapture`,
  `cargo test -p nokv-object read_object_blocks_into -- --nocapture`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`, and
  `cd docs && npm run build` passed.
- Aggressive unguarded scatter was rejected by evidence. On the standard
  `sparse-coalesced` Python/fsspec L1 row it produced cold
  `6001.78 samples/s`, warm `492865.54 samples/s`, and roughly one object GET
  per semantic sample. That is useful only for already-hot cache hits, not for
  cold object storage.
- Guarded `sparse-coalesced` row:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-guarded-scatter-into-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=into
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced` passed. The row
  was cold `36817.88 samples/s`, warm `403010.77 samples/s`; cold stats kept
  `data_fabric_planned_blocks=32` and `data_fabric_object_gets=32`.
- Guarded `sparse-exact` row with the same standard Python/fsspec L1 shape and
  result directory
  `/tmp/nokv-python-standard-scatter-into-sparse-exact` passed. The row was
  cold `13748.11 samples/s`, warm `47883.38 samples/s`; this is within local
  cold-run noise and does not change the exact-window conclusion.
- Interpretation: the right policy is cold-path coalescing plus direct-write
  only when the physical read plan stays compact. The next real step is a
  cache-aware scatter mode that may split windows after confirming the covering
  ranges are already hot, then a pinned/exported buffer API so Python and future
  RDMA/GPU paths do not keep paying bytearray/GIL staging costs.

### 2026-06-14 P7 Cache-Aware Scatter Direct-Fill

- Extended the packed scatter plan with an `expands_physical_reads` flag. When
  scatter keeps the physical block count compact, it still uses the object
  executor direct-write path. When scatter would split a coalesced gap window,
  the client now tries a cache-only direct fill first.
- The cache-only path calls the existing block-cache range lookup for every
  semantic scatter block and writes directly into the packed caller buffer only
  when all ranges hit. Any miss falls back to the original coalesced window
  read, so cold paths keep the same object GET shape.
- Added
  `service_file_client_read_path_ranges_batch_into_uses_cache_aware_scatter_for_hot_gap_window`.
  The first read populates the cache through one coalesced object GET; the
  second read returns the same packed bytes without another object-store batch
  read and records two semantic cache hits.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`, and
  `cargo test -p nokv-client
  service_file_client_read_path_ranges_batch_into -- --nocapture` passed.
- Standard L1-only cache-aware scatter row:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-cache-aware-scatter-into-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=into
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced` passed. The row
  was cold `60913.03 samples/s`, warm `728744.58 samples/s`.
- The cold measured data-fabric stats stayed compact:
  `data_fabric_planned_blocks=32`,
  `data_fabric_object_gets=32`, and
  `data_fabric_object_get_bytes=133693440`. The warm measured stats were
  `data_fabric_planned_blocks=4096`,
  `data_fabric_cache_hits=4096`, and
  `data_fabric_cache_hit_bytes=67108864`.
- Interpretation: this is the right split for AI shard reads. Cold runs keep
  large coalesced object reads; warm runs scatter from local cache into the
  dataloader buffer and avoid the full-window allocation/copy. The remaining
  high-value gap is the Python staging boundary: a pinned/exported buffer API
  and a Rust-native dataloader path should remove bytearray/GIL overhead before
  RDMA or GPU/HBM direct paths are credible.

### 2026-06-14 P7 Rust-Owned Python Read Buffer

- Added `nokv._native.ReadBuffer` and exported it as `nokv.ReadBuffer`. It owns
  reusable Rust memory behind the Python object and exposes length, capacity,
  reserve, clear, index access, and copy-out helpers.
- Added `Client.read_ranges_batch_buffer` and
  `NoKVFileSystem.read_ranges_batch_buffer`. The method still resolves all
  paths through the Rust SDK batch range primitive, but writes into the
  `ReadBuffer` while the Python GIL is released. This removes the previous
  requirement to hold the GIL while mutating a Python `bytearray`.
- Extended `bench/drivers/native_fsspec_bench.py` to
  `--read-shape ranges|packed|into|buffer`. The buffer shape reuses one
  `ReadBuffer` per pass and checks sample-window boundary bytes through the
  buffer object without materializing per-sample Python `bytes`.
- Extended `scripts/run-python-fsspec-smoke.sh` to verify fresh and reused
  `ReadBuffer` reads against live RustFS plus NoKV metadata service.
- This is pinned-ready, not yet pinned. The buffer is ordinary Rust `Vec<u8>`
  memory, and it does not yet expose a PEP 3118 memoryview. The next step is a
  safe read-only export surface with resize/write exclusion; after that the
  allocator can move to host-pinned or registered memory.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`, and
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings` passed.
- Live buffer smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=buffer
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-buffer-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny smoke row was cold
  `4237.29 samples/s`, warm `22786.57 samples/s`; this is correctness smoke,
  not a throughput limit result.
- Standard L1-only buffer row:
  `NOKV_AI_LAYERED_RUN_L2=0
  NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1
  NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-read-buffer-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=buffer
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced` passed. The row
  was cold `42648.58 samples/s`, warm `756819.18 samples/s`.
- Interpretation: the warm path improved slightly over the bytearray
  cache-aware row (`728744.58 samples/s`) because the SDK read no longer holds
  the GIL or mutates a Python-owned bytearray. The cold result was lower than
  the previous local run (`60913.03 samples/s`) while preserving the same
  compact data-fabric shape (`data_fabric_object_gets=32`), so treat it as
  cold RustFS/local-run noise rather than a data-plane regression.

### 2026-06-14 P7 ReadBuffer Export Guard

- Added `nokv._native.ReadBufferView` and exported it as
  `nokv.ReadBufferView`.
- `ReadBuffer.export(offset=0, length=None)` now returns a read-only view token
  over the Rust-owned staging buffer. The token supports length, byte indexing,
  copy-out, and slice copy-out helpers.
- The buffer tracks active exported views. `reserve`, `clear`, and
  `Client.read_ranges_batch_buffer` reject mutation/refill while any exported
  view is alive, preventing stale Python-side readers from observing a resized
  or overwritten staging buffer.
- A direct PEP 3118 buffer-protocol implementation was attempted first, but the
  current package targets `abi3-py39`; PyO3 does not expose the required
  `Py_buffer`/buffer-protocol hooks under that limited ABI. The shipped surface
  is therefore a NoKV read-only export token. A true `memoryview` should be
  added later behind a Python 3.11+ or non-abi3 feature gate.
- Extended `scripts/run-python-fsspec-smoke.sh` to verify the view contents,
  active export count, and clear/refill rejection while a view is alive.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`, and
  `cd docs && npm run build` passed.
- Live export-guard smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=buffer
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-buffer-export-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny smoke row was cold
  `4348.12 samples/s`, warm `21187.40 samples/s`; this is a live correctness
  smoke for the export guard, not a throughput claim.

### 2026-06-14 P7 Staging Memory Allocator Boundary

- Added `crates/nokv-python/src/staging.rs` as the Python training read staging
  memory boundary. `ReadBuffer` no longer owns a raw `Vec<u8>` directly; it owns
  a `StagingBuffer`.
- The only implemented staging memory kind is `system`. `ReadBuffer.memory_kind()`
  reports that fact to Python callers, and the smoke gate asserts it. No
  CUDA-pinned, RDMA-registered, or HBM-backed memory mode is exposed until a
  real allocator exists.
- The staging boundary preserves the existing active-export guard: clear,
  reserve, and SDK refill still fail while a `ReadBufferView` is alive.
- This is the correct next step before pinned host memory because future
  allocators can be inserted under the same direct-write SDK path without
  leaking CUDA/RDMA details into metadata, object storage, or fsspec range
  planning.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`, and
  `cd docs && npm run build` passed.
- Live staging-buffer smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=buffer
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-staging-buffer-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny smoke row was cold
  `4617.94 samples/s`, warm `21636.24 samples/s`; this proves the staging
  boundary and `memory_kind() == "system"` assertion under live RustFS, not a
  throughput improvement claim.

### 2026-06-14 P7 Page-Locked Host ReadBuffer

- Added Unix `page_locked` staging memory for `ReadBuffer`. It uses `mlock` /
  `munlock` around the Rust-owned staging allocation and preserves the same
  SDK direct-write path as `system` memory.
- `ReadBuffer(capacity=0, memory_kind="system")` now accepts
  `memory_kind="page_locked"`. Invalid memory kinds fail fast with `ValueError`.
  Lock failures return `RuntimeError` and leave the existing buffer state
  unchanged on reserve/refill.
- `NoKVFileSystem.new_read_buffer(..., memory_kind="...")` passes the memory
  kind through to the native binding. `bench/drivers/native_fsspec_bench.py`
  adds `--read-buffer-memory-kind system|page_locked` for `--read-shape buffer`
  and records `read_buffer_memory_kind=...` in shape and cost breakdown.
- This is host page-locking only. It is not CUDA `cudaHostAlloc`, RDMA
  registration, HBM-backed storage, or PEP 3118 memoryview exposure.
- `scripts/run-python-fsspec-smoke.sh` now tries a page-locked `ReadBuffer`
  during the live correctness smoke and rejects unsupported fake kinds such as
  `cuda_pinned`.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`, and
  `cd docs && npm run build` passed.
- Live page-locked smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=buffer
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-page-locked-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed and printed
  `NoKV page_locked ReadBuffer smoke passed`. The tiny benchmark row was cold
  `4331.74 samples/s`, warm `24276.11 samples/s`, with
  `read_buffer_memory_kind=page_locked` in the CSV. This is a correctness and
  labeling smoke, not a page-locked throughput claim.

### 2026-06-14 P7 Prepared Python Range Batch Plan

- Added `nokv._native.RangeBatchPlan` and exported it as `nokv.RangeBatchPlan`.
- `Client.prepare_range_batch` and `NoKVFileSystem.prepare_range_batch` now
  build a native `PreparedPathRangeBatch`. The prepared plan owns the static
  range-batch layout: normalized requests, packed request output offsets,
  request output lengths, coalesced read windows, and ordered metadata
  batch-open requests. Python holds the prepared plan behind `Arc`, so repeated
  planned reads do not clone the full native plan before releasing the GIL.
  `Client.read_range_batch_plan_buffer` and
  `NoKVFileSystem.read_range_batch_plan_buffer` reuse that native plan to read
  into a `ReadBuffer`.
- The plan deliberately does not cache metadata read plans, object layouts, or
  generations. Every read still uses the Rust SDK metadata batch-open path, so
  shard generation fencing and metadata visibility semantics stay unchanged.
- The ordinary `read_ranges_batch_buffer` / `read_path_ranges_batch_into` path
  remains direct and does not build a prepared plan per call. That keeps the
  existing non-prepared hot path from paying for a reusable structure it will
  immediately discard.
- `bench/drivers/native_fsspec_bench.py` adds `--read-shape planned_buffer`.
  The benchmark prepares plans outside the timed read pass and records
  `range_batch_plan=true` plus `read_buffer_memory_kind=...` in CSV shape and
  cost breakdown.
- `scripts/run-python-fsspec-smoke.sh` verifies plan length, range count,
  packed layout, output length, and a live RustFS-backed planned buffer read.
  `scripts/run-ai-dataplane-layered-matrix.sh` now accepts
  `NOKV_AI_LAYERED_PYTHON_READ_SHAPE=planned_buffer` and
  `NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=system|page_locked`.
- Validation:
  `cargo fmt --all -- --check`,
  `git diff --check`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`, and
  `cd docs && npm run build` passed.
- Live planned-buffer smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=planned_buffer
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-planned-buffer-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `3602.12 samples/s`, warm `19553.93 samples/s`, with
  `range_batch_plan=true` and `read_buffer_memory_kind=page_locked` in the CSV.
  This is API correctness and result-labeling evidence, not a throughput claim.

### 2026-06-14 P7 Native Prepared Range-Batch Evidence

- Added `nokv_client::PreparedPathRangeBatch` and exported it from
  `nokv-client`. `NoKvFsClient::prepare_path_ranges_batch` builds the static
  request/window/output plan, and
  `NoKvFsClient::read_prepared_path_ranges_batch_into` executes it against a
  caller buffer while still batch-opening metadata for every read.
- Added regression coverage:
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture` verifies that preparing the plan does not access metadata,
  repeated reads reuse the native layout, and each read still sends the same
  `OpenPathReadPlanBatch` request with expected generations.
- Existing range-batch `into` regressions still pass:
  `cargo test -p nokv-client read_path_ranges_batch -- --nocapture`.
- Python/native checks passed:
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`, and
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`.
- Live RustFS planned-buffer smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=planned_buffer
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-native-plan-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `4484.72 samples/s`, warm `22367.23 samples/s`, with
  `range_batch_plan=true` and `read_buffer_memory_kind=page_locked`.
- Current standard L1 Python/fsspec comparison used the same
  `sparse-coalesced` shape as the previous section: 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  and `cold,warm` client-cache states.
- Final non-prepared command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-buffer-page-locked-native-plan-final-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=buffer
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Final prepared command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-planned-buffer-page-locked-native-plan-arc-rerun-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=planned_buffer
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Cold result: `buffer` read 4096 samples in `0.096631s`,
  `42388.22 samples/s`, `662.3159 MiB/s`, p50 `10500.12 us`, p99
  `17046.83 us`; `planned_buffer` read 4096 samples in `0.089436s`,
  `45798.05 samples/s`, `715.5945 MiB/s`, p50 `8332.52 us`, p99
  `21702.63 us`. That is `+8.04%` throughput and `+20.64%` p50 latency
  reduction, but p99 regressed `-27.31%` in this single cold run.
- Warm result: `buffer` read 4096 samples in `0.007781s`,
  `526410.49 samples/s`, `8225.1639 MiB/s`, p50 `703.17 us`, p99
  `876.15 us`; `planned_buffer` read 4096 samples in `0.006232s`,
  `657252.89 samples/s`, `10269.5765 MiB/s`, p50 `595.46 us`, p99
  `712.12 us`. That is `+24.86%` throughput, `+15.32%` p50 latency
  reduction, and `+18.72%` p99 latency reduction in this single warm run.
- Interpretation: native prepared batch/window layout now matters most on warm
  repeated-read loops where object/cache work is already hot and Python/native
  planning overhead is visible. Cold rows are noisy and still dominated by
  object/backend scheduling and foreground object reads; do not claim a stable
  cold-path win from one run.
- Caveat: this is still L1 native Python/fsspec only. It is not an L2
  mounted-FUSE benchmark, not a NoKV-vs-JuiceFS comparison, and not GPU/RDMA
  direct evidence.

### 2026-06-14 P7 Borrowed Prepared Range-Batch Executor

- Changed `NoKvFsClient::read_prepared_path_ranges_batch_into` to execute
  prepared range batches through borrowed task/window state. The method still
  materializes fresh `PathLayoutOpen` values from metadata for every read, but
  it no longer clones the prepared `path`, request offsets, or coalesced
  `RangeReadWindow` structures into owned `RangeBatchIntoRequestTask` values
  before filling the staging buffer.
- Extracted the common window fill path into a single helper used by both
  ordinary `read_path_ranges_batch_into` and the borrowed prepared executor.
  The helper preserves the existing generation-fenced metadata open,
  read-ahead prefetch, cache-aware scatter fill, guarded direct-write, and
  coalesced fallback behavior.
- Correctness checks passed:
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture`,
  `cargo test -p nokv-client read_path_ranges_batch -- --nocapture`,
  `cargo check -p nokv-python --features extension-module`, and
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`.
- Live RustFS planned-buffer smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=planned_buffer
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-borrowed-plan-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `4098.89 samples/s`, warm `22124.87 samples/s`, with
  `range_batch_plan=true` and `read_buffer_memory_kind=page_locked`.
- Standard L1 command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-planned-buffer-page-locked-borrowed-plan-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=planned_buffer
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape matches the previous standard L1 rows: 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  and `cold,warm` client-cache states.
- Compared with the final non-prepared `buffer` row from
  `/tmp/nokv-python-standard-buffer-page-locked-native-plan-final-sparse-coalesced`:
  cold `planned_buffer` was slower in this run
  (`42388.22 -> 34291.71 samples/s`, `-19.10%` throughput; p50
  `10500.12 -> 15717.85 us`). Warm `planned_buffer` was faster
  (`526410.49 -> 687623.28 samples/s`, `+30.62%` throughput; p50
  `703.17 -> 523.50 us`; p99 `876.15 -> 710.78 us`).
- Compared with the prior `Arc<PreparedPathRangeBatch>` executor from
  `/tmp/nokv-python-standard-planned-buffer-page-locked-native-plan-arc-rerun-sparse-coalesced`:
  the borrowed executor improved the warm row by `+4.62%` throughput and
  reduced warm p50 by `12.08%`. The cold row regressed, while object bytes and
  cache/object stats stayed in the same shape; treat that as local cold-path
  noise unless repeated runs show the same direction.
- Interpretation: borrowed prepared execution is the right hot-loop shape for
  repeated dataloader reads because it removes avoidable per-read cloning of
  static request/window layout. The evidence supports a warm repeated-read
  improvement, not a cold-path or L2/JuiceFS claim.

### 2026-06-14 P7 Python RangeBatchReader

- Added `nokv._native.RangeBatchReader` and exported it as
  `nokv.RangeBatchReader`.
- `Client.prepare_range_batch_reader` and
  `NoKVFileSystem.prepare_range_batch_reader` now build a native prepared
  range-batch plan and a reusable NoKV-owned `ReadBuffer` together. The reader
  exposes `layout()`, `output_len()`, `memory_kind()`, `buffer()`, and `read()`.
  `read()` refills the internal buffer while the Python GIL is released.
- The reader owns only static request/window layout and staging memory. Each
  `read()` still calls the same generation-fenced prepared executor, so metadata
  visibility and object layout freshness stay unchanged.
- `bench/drivers/native_fsspec_bench.py` adds
  `--read-shape batch_reader`. The benchmark creates one reader per path batch
  before warmup/timed reads and records `range_batch_plan=true`,
  `range_batch_reader=true`, and `read_buffer_memory_kind=...`.
- `scripts/run-python-fsspec-smoke.sh` verifies reader length, range count,
  output length, layout, memory kind, buffer reuse, live RustFS read results,
  and active-export refill rejection.
- Validation:
  `cargo fmt --all -- --check`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`, and
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh` passed.
- Live RustFS reader smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9100
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9101
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7810
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=batch_reader
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-batch-reader-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `4158.99 samples/s`, warm `21709.63 samples/s`.
- Standard L1 command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-batch-reader-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=batch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  and `cold,warm` client-cache states.
- Compared with the final non-prepared `buffer` row from
  `/tmp/nokv-python-standard-buffer-page-locked-native-plan-final-sparse-coalesced`:
  cold `batch_reader` was faster
  (`42388.22 -> 53951.59 samples/s`, `+27.28%` throughput; p50
  `10500.12 -> 8984.21 us`; p99 `17046.83 -> 11541.55 us`). Warm
  `batch_reader` was also faster than non-prepared `buffer`
  (`526410.49 -> 566900.80 samples/s`, `+7.69%` throughput; p99
  `876.15 -> 801.28 us`), while p50 was essentially flat
  (`703.17 -> 706.73 us`).
- Compared with the borrowed `planned_buffer` row from
  `/tmp/nokv-python-standard-planned-buffer-page-locked-borrowed-plan-sparse-coalesced`:
  `batch_reader` was faster on this cold run but slower on warm
  (`687623.28 -> 566900.80 samples/s`, `-17.56%` throughput). Treat this as API
  lifecycle evidence, not as proof that `batch_reader` is always faster than
  `planned_buffer`.
- Interpretation: `RangeBatchReader` is the cleaner dataloader-facing object
  because it keeps plan and staging-buffer lifetimes together and removes
  per-call Python buffer passing. The current single-run benchmark supports it
  as a practical API shape, not as a universal throughput win.

### 2026-06-14 P7 Python RangeBatchEpochReader

- Added `nokv._native.RangeBatchEpochReader` and exported it as
  `nokv.RangeBatchEpochReader`.
- `Client.prepare_range_batch_epoch` and
  `NoKVFileSystem.prepare_range_batch_epoch` now build a vector of native
  `RangeBatchReader`s. Each reader keeps its own prepared range-batch plan and
  staging buffer; the epoch object only tracks the next batch index.
- The API exposes `batch_count()`, `reset()`, `read_next()`, `buffer(index)`,
  `layout(index)`, `output_len(index)`, and `memory_kind(index)`. `read_next()`
  refills the selected batch while the Python GIL is released and advances the
  cursor only after a successful read, so active-export failures do not skip a
  batch.
- `bench/drivers/native_fsspec_bench.py` adds `--read-shape epoch_reader`.
  The benchmark prepares all path-batch readers before warmup/timed reads,
  resets the epoch at the start of each pass, calls `epoch.read_next()` for
  each measured batch, and records `range_batch_plan=true`,
  `range_batch_reader=true`, `range_batch_epoch=true`, and
  `read_buffer_memory_kind=...`.
- `scripts/run-python-fsspec-smoke.sh` now validates epoch batch count, output
  length, layout, staging memory kind, reset semantics, round-robin reads,
  live RustFS bytes, and active-export refill rejection.
- Validation:
  `cargo fmt --all -- --check`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`, and
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh` passed.
- Live RustFS epoch smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9110
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9111
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7811
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=epoch_reader
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-epoch-reader-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `2675.06 samples/s`, warm `18940.52 samples/s`.
- Standard L1 epoch command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-epoch-reader-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=epoch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  and `cold,warm` client-cache states.
- Standard L1 epoch rows were cold `27441.57 samples/s`, warm
  `607137.18 samples/s`. A second epoch run in
  `/tmp/nokv-python-standard-epoch-reader-page-locked-sparse-coalesced-rerun`
  produced cold `28011.57 samples/s`, warm `594826.46 samples/s`.
- Current-code `batch_reader` comparison in
  `/tmp/nokv-python-standard-batch-reader-page-locked-sparse-coalesced-current`
  produced cold `50238.74 samples/s`, warm `624814.94 samples/s`. On this
  workload, `epoch_reader` is therefore not a throughput win: warm is close
  (`-2.83%` to `-4.80%` versus current `batch_reader`), while cold is much
  slower (`-45.38%` and `-44.24%`). The object/cache counters report the same
  logical bytes and nearly the same object GET bytes as `batch_reader`, so this
  should be investigated as epoch-call overhead or cold scheduling variance
  before claiming a cold-path improvement.
- Interpretation: this is the dataloader-worker lifecycle layer above
  `RangeBatchReader`. It does not change metadata freshness or object layout
  safety; it removes repeated Python-side request/reader orchestration from a
  training epoch hot loop. Keep it as the cleaner API surface for long-lived
  workers, not as the current fastest benchmark shape.

### 2026-06-14 P7 Python Epoch `read_all`

- Added `RangeBatchEpochReader.read_all()`. It builds the current epoch order
  from the cursor, preflights every selected reader for active exports, then
  fills every prepared batch buffer inside one GIL-released native call.
- `read_next()` remains available for stepwise consumers. A full `read_all()`
  cycle advances by exactly `batch_count()` positions, so the cursor returns to
  the same index. The benchmark resets before each pass and expects order
  `[0, 1, ...]`.
- `bench/drivers/native_fsspec_bench.py` now uses `read_all()` for
  `--read-shape epoch_reader`, records one timed epoch call per pass, validates
  every prepared buffer after the call, and emits
  `range_batch_epoch_read_all=true` plus `range_batch_epoch_batches=...`.
- `scripts/run-python-fsspec-smoke.sh` now validates both `read_next()` and
  `read_all()` results, including active-export rejection for the full-epoch
  path.
- Validation:
  `cargo fmt --all -- --check`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `git diff --check`, and `cd docs && npm run build` passed. The docs build
  reported only the existing VitePress chunk-size warning.
- Live RustFS `read_all()` smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9120
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9121
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7812
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=epoch_reader
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-epoch-reader-read-all-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `3279.64 samples/s`, warm `20694.13 samples/s`, with
  `range_batch_epoch_read_all=true`.
- Standard L1 `read_all()` command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-epoch-reader-read-all-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=epoch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  and `cold,warm` client-cache states.
- Standard L1 `read_all()` rows were cold `40592.47 samples/s`, warm
  `569215.00 samples/s`. A second run in
  `/tmp/nokv-python-standard-epoch-reader-read-all-page-locked-sparse-coalesced-rerun`
  produced cold `29852.16 samples/s`, warm `564900.61 samples/s`.
- Compared with the earlier stepwise `epoch_reader` rows
  (`27441.57/28011.57` cold and `607137.18/594826.46` warm), `read_all()`
  improves cold throughput on these two runs but regresses warm throughput. The
  current-code `batch_reader` row
  `/tmp/nokv-python-standard-batch-reader-page-locked-sparse-coalesced-current`
  remains faster at cold `50238.74 samples/s`, warm `624814.94 samples/s`.
  Treat `read_all()` as a lifecycle and call-count improvement, not yet as the
  fastest path.
- Interpretation: this is the first step toward a Rust-native DataLoader epoch
  loop. It reduces Python/PyO3 call count for prepared batches, but p50/p99 in
  `epoch_reader` rows now describe one full epoch read call rather than one
  path-batch read call. The next performance step should remove the remaining
  per-batch metadata/object execution serialism inside `read_all()` or add a
  Rust-native parallel epoch executor before expecting it to beat
  `batch_reader`.

### 2026-06-14 P7 Bounded Parallel Python Epoch `read_all`

- Changed `RangeBatchEpochReader.read_all()` from serial native execution to
  bounded native parallel execution. The internal cap is two epoch readers at a
  time; this keeps metadata/object reads overlapped without opening all epoch
  batch object responses at once.
- The full-parallel attempt, which spawned every prepared batch reader at once,
  failed the standard RustFS run with `No buffer space available` and an S3 body
  decode failure. That failure is useful evidence: the epoch executor needs a
  bounded object-store pressure limit, not unbounded thread fanout.
- Error reporting remains deterministic: every worker in the active bounded
  group is joined, and Python receives the first failing epoch-reader index in
  epoch order. Active-export checks still happen before the native call and
  again inside each reader before mutating its staging buffer.
- `bench/drivers/native_fsspec_bench.py` now emits
  `range_batch_epoch_parallel=true` and
  `range_batch_epoch_parallelism=...` in both shape and cost breakdown.
- Validation:
  `cargo fmt --all -- --check`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `git diff --check`, and `cd docs && npm run build` passed. The docs build
  reported only the existing VitePress chunk-size warning.
- Live RustFS bounded-parallel smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9140
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9141
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7814
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=epoch_reader
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-epoch-reader-bounded-parallel-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `3998.75 samples/s`, warm `20828.83 samples/s`, with
  `range_batch_epoch_parallelism=1` because there was only one path batch.
- Standard L1 bounded-parallel epoch command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-epoch-reader-bounded-parallel-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=epoch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  `range_batch_epoch_parallelism=2`, and `cold,warm` client-cache states.
- Standard bounded-parallel epoch rows were cold `59021.41 samples/s`, warm
  `614649.64 samples/s`. A second run in
  `/tmp/nokv-python-standard-epoch-reader-bounded-parallel-page-locked-sparse-coalesced-rerun`
  produced cold `46691.92 samples/s`, warm `614227.31 samples/s`.
- Current-code `batch_reader` baseline in
  `/tmp/nokv-python-standard-batch-reader-page-locked-sparse-coalesced-bounded-epoch-baseline`
  produced cold `28477.76 samples/s`, warm `674724.59 samples/s`.
- Interpretation: bounded epoch parallelism is now a real cold-path win on this
  standard L1 sparse-coalesced run, but it is still slower than `batch_reader`
  on the hot cache path. This makes sense: cold reads benefit from overlapped
  object work, while warm reads are dominated by cache copies, thread spawn, and
  full-epoch aggregation overhead. The next step should avoid per-call thread
  spawn, for example by moving epoch workers into a persistent native iterator
  or by adding a Rust-native dataloader executor that owns worker lifetime.

### 2026-06-14 P7 Persistent Python Epoch Workers

- Changed `RangeBatchEpochReader` to own persistent native epoch workers. The
  worker pool is created with the epoch reader, reused by every `read_all()`,
  and shut down on drop. `worker_count()` exposes the actual worker count for
  smoke and benchmark verification.
- `read_all()` now preflights active exports, submits prepared-batch read jobs
  to the persistent workers, waits for all results, and reports the first
  failure in epoch order. Worker panics are caught and returned as read errors
  instead of hanging the Python caller.
- `bench/drivers/native_fsspec_bench.py` now records
  `range_batch_epoch_persistent_workers=true` and uses `epoch.worker_count()`
  for the measured row's `range_batch_epoch_parallelism=...`.
- `scripts/run-python-fsspec-smoke.sh` asserts that a two-batch epoch has two
  workers and still validates `read_next()`, `read_all()`, reset behavior,
  page-locked staging, and active-export rejection.
- Validation:
  `cargo fmt --all -- --check`,
  `cargo check -p nokv-python --features extension-module`,
  `cargo test -p nokv-python -- --nocapture`,
  `cargo test -p nokv-client
  service_file_client_prepared_path_ranges_batch_reuses_native_layout
  -- --nocapture`,
  `cargo clippy -p nokv-client -p nokv-object -p nokv-python
  --all-targets -- -D warnings`,
  `python3 -m py_compile crates/nokv-python/python/nokv/__init__.py
  crates/nokv-python/python/nokv/fsspec.py
  bench/drivers/native_fsspec_bench.py`,
  `bash -n scripts/run-python-fsspec-smoke.sh
  scripts/run-ai-dataplane-layered-matrix.sh`,
  `git diff --check`, and `cd docs && npm run build` passed. The docs build
  reported only the existing VitePress chunk-size warning.
- Live RustFS persistent-worker smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9150
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9151
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7815
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=epoch_reader
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-epoch-reader-persistent-worker-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny row was cold
  `2976.42 samples/s`, warm `20412.54 samples/s`, with
  `range_batch_epoch_persistent_workers=true`.
- Standard L1 persistent-worker epoch command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-epoch-reader-persistent-worker-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=epoch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  `range_batch_epoch_parallelism=2`, and `cold,warm` client-cache states.
- Standard persistent-worker epoch rows were cold `54340.21 samples/s`, warm
  `715635.52 samples/s`. A second run in
  `/tmp/nokv-python-standard-epoch-reader-persistent-worker-page-locked-sparse-coalesced-rerun`
  produced cold `44835.26 samples/s`, warm `674497.83 samples/s`.
- Compared with bounded per-call thread spawn rows
  (`59021.41/46691.92` cold and `614649.64/614227.31` warm), persistent
  workers mainly improve the hot path. Compared with the current-code
  `batch_reader` baseline
  `/tmp/nokv-python-standard-batch-reader-page-locked-sparse-coalesced-bounded-epoch-baseline`
  at cold `28477.76 samples/s`, warm `674724.59 samples/s`, persistent
  `epoch_reader` is now competitive on warm and faster on the stronger cold
  run, but cold variance is still high.
- Interpretation: owning worker lifetime in the epoch object is the right
  direction for an AI dataloader path. The next performance step should move
  more of the batch iteration and checksum/export boundary out of Python, or
  add a Rust-native dataloader iterator that can prefetch the next epoch batch
  while Python consumes the current staging buffer.

### 2026-06-14 P7 Python Benchmark Timing Split

- Updated `bench/drivers/native_fsspec_bench.py` to split timed Python/fsspec
  rows into `native_read_us=...` and `python_consume_us=...` in
  `cost_breakdown`.
- `native_read_us` is the timed SDK/fsspec read-call surface: for
  `epoch_reader`, that is one `RangeBatchEpochReader.read_all()` call; for
  `batch_reader`, it is the sum of per-batch `reader.read()` calls.
- `python_consume_us` is the benchmark's post-read layout validation and
  checksum loop over returned or staged bytes. The canonical `seconds`,
  throughput, and caveat remain unchanged and still include both native read
  time and Python consume time.
- Live RustFS timing-split smoke:
  `NOKV_PYTHON_SMOKE_RUSTFS_ADDRESS=127.0.0.1:9160
  NOKV_PYTHON_SMOKE_RUSTFS_CONSOLE_ADDRESS=127.0.0.1:9161
  NOKV_PYTHON_SMOKE_SERVER_ADDRESS=127.0.0.1:7816
  NOKV_PYTHON_SMOKE_CARGO_TARGET_DIR=target
  NOKV_PYTHON_SMOKE_SHARD_COUNT=2
  NOKV_PYTHON_SMOKE_FILES_PER_DIR=8
  NOKV_PYTHON_SMOKE_SAMPLE_BYTES=1024
  NOKV_PYTHON_SMOKE_CONCURRENCY=2
  NOKV_PYTHON_SMOKE_READ_SHAPE=epoch_reader
  NOKV_PYTHON_SMOKE_READ_BUFFER_MEMORY_KIND=page_locked
  NOKV_PYTHON_SMOKE_CACHE_STATES=cold,warm
  NOKV_PYTHON_SMOKE_RESULT_CSV=/tmp/nokv-python-fsspec-epoch-reader-timing-split-smoke.csv
  scripts/run-python-fsspec-smoke.sh` passed. The tiny warm row reported
  `native_read_us=342.38` and `python_consume_us=5.71`.
- Standard L1 persistent-worker epoch command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-epoch-reader-persistent-worker-timing-split-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=epoch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Standard L1 `batch_reader` comparison command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-batch-reader-timing-split-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=batch_reader
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples/shard, 128 selected samples/shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`, Python
  concurrency 4, RustFS plus local-hot object root, page-locked staging memory,
  and `cold,warm` client-cache states.
- Persistent-worker `epoch_reader` timing-split rows:
  cold `54983.92 samples/s`, `native_read_us=72320.33`,
  `python_consume_us=1585.87`; warm `600296.75 samples/s`,
  `native_read_us=5166.54`, `python_consume_us=1535.88`.
- `batch_reader` timing-split rows:
  cold `53794.84 samples/s`, `native_read_us=73697.96`,
  `python_consume_us=1847.63`; warm `598750.13 samples/s`,
  `native_read_us=5292.04`, `python_consume_us=1468.33`.
- Interpretation: with persistent workers, `epoch_reader` and `batch_reader`
  now have similar native read time on this warm workload. Around 1.5 ms of the
  measured standard pass is still Python-side buffer traversal and checksum.
  The next optimization should avoid Python per-range iteration in the
  dataloader hot path: either expose a true buffer protocol / tensor handoff or
  move the training iterator's sample-window consumption into Rust.

### 2026-06-14 P7 Pre-Native Prepared Buffer Standard L1 Evidence

- Before `PreparedPathRangeBatch` moved the static window layout into
  `nokv-client`, ran the standard L1 Python/fsspec sparse-coalesced workload
  with page-locked staging memory for both `buffer` and `planned_buffer`.
- Baseline command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-buffer-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=buffer
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Planned command:
  `NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_RUN_RUST_L1=0
  NOKV_AI_LAYERED_RUN_PYTHON_L1=1 NOKV_AI_LAYERED_PROFILE=standard
  NOKV_AI_LAYERED_CARGO_TARGET_DIR=target
  NOKV_AI_LAYERED_RESULT_DIR=/tmp/nokv-python-standard-planned-buffer-page-locked-sparse-coalesced
  NOKV_AI_LAYERED_PYTHON_CONCURRENCY=4
  NOKV_AI_LAYERED_PYTHON_CACHE_STATES=cold,warm
  NOKV_AI_LAYERED_PYTHON_READ_SHAPE=planned_buffer
  NOKV_AI_LAYERED_PYTHON_READ_BUFFER_MEMORY_KIND=page_locked
  scripts/run-ai-dataplane-layered-matrix.sh sparse-coalesced`.
- Workload shape: standard profile, `sparse-coalesced`, 32 shards, 256
  samples per shard, 128 selected samples per shard, 16 KiB sample size,
  `range_stride=2`, `max_gap_bytes=16384`, `range_batch_size=4`,
  Python concurrency 4, RustFS object backend, local hot object root, client
  cache cold and warm states.
- Cold results:
  `buffer` read 4096 samples in `0.068515s`, `59782.28 samples/s`,
  `934.0981 MiB/s`, p50 `8125.85 us`, p99 `10796.97 us`.
  `planned_buffer` read 4096 samples in `0.068247s`,
  `60017.58 samples/s`, `937.7747 MiB/s`, p50 `7817.00 us`, p99
  `10416.75 us`. That is `+0.39%` throughput, `+3.80%` p50 latency
  reduction, and `+3.52%` p99 latency reduction for this single cold run.
- Warm results:
  `buffer` read 4096 samples in `0.006240s`, `656405.83 samples/s`,
  `10256.3412 MiB/s`, p50 `585.46 us`, p99 `745.44 us`.
  `planned_buffer` read 4096 samples in `0.006166s`,
  `664288.03 samples/s`, `10379.5004 MiB/s`, p50 `528.42 us`, p99
  `774.25 us`. That is `+1.20%` throughput, `+9.74%` p50 latency
  reduction, and `-3.86%` p99 latency regression for this single warm run.
- Interpretation: prepared range-batch plans remove repeated Python request
  normalization and packed-output layout construction from timed repeated reads.
  The measured gain is small on this standard L1 workload, which is expected
  because the dominant work is still Rust SDK batch open, object/cache reads,
  and copy into the staging buffer. Keep it as a cleaner API and as a necessary
  boundary for later DataLoader plan reuse, but do not claim a fundamental
  throughput improvement from this single run.
- Caveat: this is an L1 native Python/fsspec benchmark only. It is not an L2
  mounted-FUSE benchmark, not a NoKV-vs-JuiceFS comparison, and not RDMA/GPU
  direct evidence.

### 2026-06-14 Multi-Shard M0: P0/P1 HA Correctness Hardening

First phase of the multi-shard, multi-node metadata effort. These fixes make a
single shard's failover/handoff lossless and self-fencing, a prerequisite for
fleet rebalance (ownership handoff reuses the failover restore path). All are
clean breaking changes (no compat shims).

- **Multi-segment shared-log chain (P0).** `nokv-control::LogRef` is now an
  ordered `Vec<LogSegmentRef>` (was a single `manifest_key`). The sync log
  accumulates a `MetadataLogSegmentPointer` chain exposed via
  `MetadataLogSyncSnapshot.segments`; the server publishes the full chain and
  replays every segment whose tail is above the checkpoint LSN. Previously a
  failover after more than one post-checkpoint commit replayed only the newest
  segment and silently lost acknowledged metadata.
- **Time-based lease self-fence (P0).** `NoKvFs` gained `lease_deadline_ms` plus
  an injectable `now_ms()`/`clock_override_ms`. The commit fence
  (`check_owner_lease`) rejects with `MetadError::LeaseExpired` once the deadline
  passes. The owner arms `deadline = renew_start + lease_ttl` on acquire/renew/
  mark-serving (basis captured before the control round-trip, so it never
  outlives the control plane's expiry). A partitioned owner whose `get_shard`
  also fails now self-fences instead of committing forever. Armed only when
  auto-renewal is enabled.
- **Atomic checkpoint (image, lsn, digest) capture (P0).** `backup_metadata`
  captures the sync-log `(lsn, digest)` before the image export and returns it in
  `MetadataBackupOutcome`; the server publishes that instead of a later snapshot
  read (which could be ahead of the image and drop a write on restore). The LSN
  is a safe lower bound (image ⊇ log_lsn); redundant replay is idempotent via
  command dedupe. Segments at/below the checkpoint are pruned after publish.
- **Epoch check→apply TOCTOU (P1).** New `epoch_fence: RwLock<()>`: commits hold
  a read guard across fence-check + Holt apply; `install`/`observe` owner-epoch
  take the write guard, so a failover bump cannot land between them.
- **Graceful lease release (P1).** `Server` releases the owner lease on `Drop`
  (after stopping renewal) so a standby acquires without waiting the TTL.
- **Archive-error fidelity (P1).** A sync-log archive failure after a durable
  apply returns `MetadError::SyncLogArchiveFailed { committed: true }` (single and
  batch paths) instead of a generic `Codec`, so callers reconcile instead of
  blind-retrying data that already landed.
- New wire variants `LeaseExpired` / `SyncLogArchiveFailed` plumbed through
  `WireMetadataError`, the client decode, and FUSE errno (LeaseExpired -> ESTALE).
- **Deferred to M2:** the etcd stable single-`sessions/{shard}` key +
  `Compare::lease` rework (etcd-side acquire/failover TOCTOU), done when etcd.rs
  is restructured for fleet placement and gated by the multi-node smoke.

Validation: `cargo test -p nokv-control -p nokv-meta -p nokv-server -p nokv-protocol`
passed; `cargo build -p nokv -p nokv-bench` clean. New tests cover the lease
self-fence (single + batch + disabled), deadline arming on/off, graceful release,
and multi-segment log restore. Three `nokv-client` background-prefetch tests fail
in the working tree but pass at the committed baseline and only break under the
uncommitted data-plane WIP (`file_client.rs`/`pipeline`); they are unrelated to
this phase, which does not touch that path.

### 2026-06-14 Multi-Shard M1: Shard Model & Global Inode Types

Pure foundation types in `nokv-types` (the shared leaf both client and server
depend on), so the partitioning function is bit-identical everywhere. No
behavior change — every shard still defaults to index 0.

- **Global inode uniqueness via high-bit shard tag.** `InodeId` gained
  `SHARD_BITS=16` / `LOCAL_BITS=48`, `compose(shard_index, local)`,
  `shard_index()`, `local()`. Shard 0 is the identity on `local`, so existing
  single-shard ids and the root (global id 1) are unchanged; non-zero shards tag
  the high bits, making ids globally unique *and* self-routing (the owning shard
  is recoverable from the id, which is what lets bare-inode RPCs route with no
  lookup). New `ModelError::InodeLocalOutOfRange`.
- **`ShardMap` longest-prefix router** (`nokv-types/src/shard.rs`): `ShardPrefix`
  (`mount:path`, parses/round-trips the control-plane `ShardId` shape),
  `ShardRoute`, and `ShardMap::route(mount, path)` / `route_inode(inode)` with a
  `DEFAULT_SHARD_INDEX=0` fallback. Prefix matching is component-boundary safe
  (`/data` never captures `/dataset`) and mount-isolated.
- **Deferred to M3:** allocator seeding by shard index (`compose(index, 2)`,
  range-clamped) lands when `NoKvFs` gains its `shard_index` during server
  mapification — adding the field now would be dead code (all shards are 0).

Validation: `cargo test -p nokv-types` (15 passed, incl. 8 new shard tests:
inode compose/decompose incl. root, distinct-shard non-collision, prefix
parse/round-trip + rejects, longest-prefix + component boundary + mount
isolation, insert-replace, route_inode). `cargo build -p nokv-meta -p nokv-fuse
-p nokv-client` clean (the new `ModelError` variant breaks no downstream match).

### 2026-06-15 Multi-Shard M2: Control Plane for the Fleet

`nokv-control` learns shard identity, enumeration, and placement — while still
storing zero namespace state (no inode/dentry/chunk). The control crate was
already shard-keyed; this adds what the client needs to route and what placement
needs to move shards.

- **`ShardRecord` identity fields.** Added `endpoint: Option<String>` (the
  owner's reachable host:port — set on acquire from the `NodeId`, cleared on
  release), `prefix: String` (the subtree this shard owns; derived from the
  `mount-<n>:<path>` shard id, default `/`), and `shard_index: u16` (the stable
  index encoded in the high bits of every inode the shard mints). All
  `#[serde(default)]`, so old records decode.
- **`register_shard` + `list_shards`.** New `ControlStore` methods (both the
  in-memory and etcd backends): `register_shard(shard_id, prefix, shard_index)`
  sets a shard's identity while it is unowned (idempotent; a live owner keeps its
  routing); `list_shards()` enumerates every record (in-memory clone; etcd
  prefix range-get over `{prefix}/shards/`) so a client can build the
  longest-prefix routing map and placement can find unowned/owned shards.
- **`placement` module.** `register_shard` / `assign` (fresh acquire) / `handoff`
  (epoch bump) / `unowned_shards` / `shards_owned_by`. Documents the load-bearing
  insight: **handoff is the same mechanism as failover** — the epoch bump fences
  the old owner and the target restores from the shard's checkpoint + log refs in
  object storage; nothing moves node-to-node.
- **Deferred:** the etcd stable single-`sessions/{shard}` key + `Compare::lease`
  rework (from M0/0.5) and a control-plane watch/revision for reactive client
  refresh both land with the M5 client router (the client can poll `list_shards`
  until then); per-shard archive prefixes are wired in M3 when the server owns
  the per-shard `MetadataArchiveConfig`.

Validation: `cargo test -p nokv-control` (12 passed, incl. new
`register_assign_handoff_flow` and `list_shards_enumerates_all`; codec round-trip
updated for the new fields). No `ShardRecord` is constructed outside the control
crate, so server/binary/bench are unaffected by the added fields.

### 2026-06-15 Multi-Shard M3: Server Mapification + RPC Routing

The metadata server is now a multi-shard host. This is the structural core that
makes the server side of the fleet real.

- **`NoKvFs.shard_index` + allocator seeding** (the M1-deferred piece). New
  `with_shard_index(self, k)` builder seeds the inode allocator into the shard's
  high-bit subspace (`fetch_max(compose(k, 2))` — identity for shard 0, never
  regresses a recovered high-water) and `shard_index()` accessor. Done as a
  builder so the ~59 existing `NoKvFs::new`/`open_existing` call sites are
  untouched (default shard 0).
- **`Server` holds `BTreeMap<ShardId, ShardSlot>` + `ShardMap`** instead of seven
  singular fields. Each `ShardSlot` owns its own Holt engine (isolated at
  `{meta-path}/{sanitized-shard-id}/metadata-state.holt`), control-plane owner
  lease, sync log, and background workers (renewal/object-gc/history-gc/backup),
  with per-shard checkpoint and shared-log archive prefixes. `open_shard_slot`
  reads each shard's `(index, prefix)` from the control record, applies
  `with_shard_index`, and preserves the failover restore + multi-segment log
  inheritance from M0. `OpenedControlStore`/`open_with_control` now take a
  `Vec<ServerShardOwnerOptions>`; the no-control path builds one default shard
  (index 0, no owner) so single-node dev is unchanged.
- **`Server::route(&request) -> &ShardSlot`** dispatches by an exhaustive
  `request_routing_key` (no `_` arm — a new RPC variant forces a routing
  decision at compile time): path ops → `ShardMap` longest-prefix; inode/parent
  ops → `InodeId::shard_index()` (self-routing); unaddressable ops → default
  shard. `execute()` resolves the slot once and every arm uses `slot.service()`;
  `execute_batch` resolves a single slot and rejects cross-shard batches;
  `handle_binary_rpc` publishes the handling shard's `LogRef` after a committing
  op. A shard not hosted here returns the new typed `NotOwner { shard_id,
  endpoint }` (wire → client → FUSE `ESTALE`) as a re-resolve hint.
- CLI/bench updated to the `Vec` owner API; `Server::service()` is now test-only
  (production routes through slots).
- **Follow-ups noted:** `OpenPathReadPlanBatch` currently routes to the default
  shard — M6 (client per-shard fan-out) must route it by its entries' shard;
  cross-path ops (rename/clone/diff) route on the source and rely on the
  single-shard service rejecting a cross-shard peer (M7 makes that an explicit
  `EXDEV`).

Validation: `cargo test -p nokv-server` 48/48; `cargo build --workspace
--exclude nokv-python` clean; `cargo fmt --check` clean. The per-shard
open/restore/sync-log path is exercised by the existing controlled-failover,
control-open, lease, and log-ref tests.

### 2026-06-15 Multi-Shard M4: Per-Shard MVCC / Snapshot Audit

Per-shard MVCC independence falls out of M3 (each `ShardSlot` is its own
`NoKvFs` with its own clock/allocator/snapshot state), so this phase is the
proof: the first tests exercising one `Server` hosting *multiple* shards (all
prior server tests used a single default shard). Five integration tests in
`rpc/tests.rs::multi_shard` against a 2-shard server (default `mount-1:/` index 0
+ `mount-1:/dataset` index 1):

- create under `/dataset/...` is served by shard 1 and its inode carries
  `shard_index()==1`; under `/other/...` shard 0 / index 0; `Server::route`
  returns the right slot for path and bare-inode requests;
- independent commit clocks (a burst on shard 1 leaves shard 0's `commit_total`
  unchanged) and disjoint high-bit inode subspaces (no cross-shard id
  collisions);
- an unhosted shard index returns `ServerError::NotOwner` (not silently served);
- subtree reads are scoped to the owning shard (no cross-boundary bleed);
- a cross-`/dataset`-boundary clone routes on its source and fails `NotFound`
  (no silent corruption) — explicit `EXDEV` remains M7.

No production/routing change was needed (the invariants already hold). Cross-
shard query scope above a graft point sees only the routed shard (documented v1
limitation); snapshot-id shard context surfacing moves to the M5 client API.

Validation: `cargo test -p nokv-server` 53/53; `cargo clippy -p nokv-server
--all-targets -- -D warnings` clean.

### 2026-06-15 Multi-Shard M5: Client Shard-Map Router

The training client now routes each metadata RPC to the owning shard's endpoint
instead of one hard-coded address — the end-to-end multi-node enabler.

- **Shared routing-key extractor.** `RoutingKey` + `request_routing_key` moved
  from `nokv-server` to `nokv-protocol` (both `pub`); `Server::route` now calls
  the protocol function, so client and server route through the *same*
  partitioning logic by construction. (No new protocol deps — it inspects only
  `&str`/`u64`.)
- **`MetadataClient` routing modes.** A `RoutingMode` enum: `SingleShard {
  address }` (today's behavior — what `NoKvFsClient::connect` and all single-
  endpoint tests use) and `Fleet(FleetRouter)`. `FleetRouter { control:
  Arc<dyn ControlStore>, mount, shard_map: RwLock<ShardMap>, endpoints:
  RwLock<HashMap<u16, SocketAddr>> }` is built from `control.list_shards()`
  (records → `ShardRoute`s + `shard_index → endpoint`). Constructors
  `single_shard(addr)` / `fleet(control, mount)`; `NoKvFsClient::connect_fleet`.
  `nokv-client` gains a `nokv-control` dep.
- **Routing lives only in `call()`** (the one chokepoint every RPC flows
  through): it inspects the request via `request_routing_key`, maps to a shard
  index (path longest-prefix / inode high-bits / default), and dials that
  shard's endpoint. The ~60 typed methods are untouched.
- **Re-resolve + retry.** `call_fleet` is a bounded loop (`FLEET_MAX_ATTEMPTS=3`):
  on `NotOwner`/`StaleOwnerEpoch`/`LeaseExpired` (or an index with no cached
  endpoint) it refreshes both caches from `control.list_shards()`, evicts stale
  pooled connections, and re-resolves against the new owner — riding out a
  handoff. The real error is propagated on the final attempt (never masked);
  non-handoff errors return immediately; single-shard mode does not retry.

Validation: `cargo test -p nokv-client` 83 passed (+ only the 3 known
pre-existing background-prefetch failures, unrelated to this work); new tests
`fleet_resolves_each_request_to_its_shard_owner_endpoint` and
`fleet_refreshes_and_retries_against_new_owner_on_not_owner` (two one-shot
servers: stale owner replies `NotOwner`, client refreshes + retries the new
owner). `cargo test -p nokv-server` 53/53; `cargo clippy` clean; `nokv` + FUSE
build.

### 2026-06-15 Multi-Shard M6: Batch Fan-Out + Data-Plane P1 Fixes

Closes the multi-shard data plane and fixes two correctness bugs from the
original safety review.

- **Shard-aware batch routing.** `request_routing_key` routes
  `OpenPathReadPlanBatch` by its first entry's path (empty → default); the client
  guarantees each batch it sends is single-shard, so first-entry routing reaches
  the right owner (previously the whole batch routed to shard 0).
- **Client per-shard fan-out (order-preserving).** Fleet-mode
  `open_path_read_plan_batch` groups requests by owning shard, sends one chunked
  single-shard batch per shard (inheriting `call_fleet` refresh+retry), and
  re-scatters plans into the original input order so `plans[i] ↔ requests[i]`
  holds — file_client range-batch callers untouched. Single-shard mode unchanged.
- **P1 — sparse-hole zero-fill.** `try_fill_scatter_from_block_cache` now
  `output.fill(0)` before copying cached blocks; a range spanning a sparse-file
  hole previously returned stale bytes from the reused staging buffer. Test reads
  a hole-spanning window into a `0xFF`-dirty buffer and asserts zeros (fails
  without the fix).
- **P1 — generation pinning across batch chunks.** When one file's windows span
  >1 metadata-open RPC chunk, the first chunk's generation is threaded as
  `expected_generation` into later chunks, so a concurrent rewrite yields a clean
  `StaleBodyGeneration` rather than bytes spliced across generations. Single
  `open_path_read_plan_batch` chokepoint, so all range-batch variants inherit it.

Validation: `cargo test -p nokv-client` 87 passed (+ only the 3 known pre-existing
prefetch failures); `cargo test -p nokv-server` 54/54; `cargo build -p nokv
-p nokv-bench` clean; clippy clean.

### 2026-06-15 Multi-Shard M7: Cross-Shard EXDEV Fencing

Cross-shard rename/hardlink/clone now return a typed `CrossShard` error that maps
to `EXDEV` end-to-end (meta → protocol → server → client → FUSE), replacing the
misleading `NotFound` that a cross-shard destination produced when resolved
inside the source shard's namespace. Two-layer defense:

- **Client primary (Fleet mode).** `rename`/`rename_replace`/`clone_subtree_path`/
  `diff_subtrees` compare `route_path(src)` vs `route_path(dst)` and return
  `CrossShard` *before* issuing the RPC — the POSIX path-based case the server
  can't see (it routes on the source path, so the destination resolves in the
  source shard). SingleShard mode skips the check (one shard).
- **Server backstop.** `ensure_same_shard` at the top of `rename_inner` (rename +
  rename_replace) and `link` rejects any inode-addressed op whose directory
  inodes' `shard_index()` differ from each other or this service's index, before
  any lookup/mutation — converting a misrouted dual-inode op into a clean EXDEV
  no-op. `clone_subtree` mints its dst locally so there is no inode-addressed
  cross-shard clone to fence.

Both checks are no-ops for legitimate same-shard work (every inode carries its
shard in its high bits). New: `MetadError::CrossShard` / `WireMetadataError::
CrossShard` + the `EXDEV` FUSE arm. Validation: nokv-meta 179, nokv-server 54,
nokv-client 89 (+ the 3 known pre-existing prefetch failures); 6 new tests
(same-shard still works; foreign-shard inode rename/link rejected with no
mutation; client path-pair cross-shard → CrossShard with no RPC; FUSE → EXDEV);
`nokv` + FUSE build + clippy clean.

### 2026-06-15 Multi-Shard M8: Fleet Tests + Benchmark (capstone)

Proves the assembled fleet end-to-end rather than per-layer, and closes the last
CLI gap.

- **End-to-end fleet integration test** (`rpc/tests.rs::fleet_e2e`, real `Server`s
  on localhost `TcpListener`s sharing one `Arc<InMemoryControlStore>`, driven by
  the real `NoKvFsClient::connect_fleet`): (1) creates/reads route across two
  servers by prefix with correct per-shard inode tagging and listing isolation;
  (2) after an owner handoff (B' failover-acquires shard 1 on a new port, epoch
  bump fences B) the client transparently `StaleOwnerEpoch`→refresh→re-resolves
  and keeps serving; (3) the strongest: B' restores shard 1 from its checkpoint
  image **and replays the shared log** on an empty meta dir, and the client keeps
  serving pre- and post-checkpoint files plus new writes; (4) a route to an
  unowned shard fails fast. Deterministic (`with_renewal(None)`, explicit epoch
  transitions).
- **Distributed-metadata benchmark** (`nokv-bench --workload
  metadata-shard-routing`): builds an in-process N-shard fleet, drives the same
  path set single-shard vs fleet, emits two comparable rows with real server-side
  per-shard commit counts and a routing-overhead %, with an explicit CSV caveat
  ("IN-PROCESS routing only, not a multi-machine cluster").
- **Multi-node HA fleet smoke** (`scripts/run-multishard-fleet-smoke.sh`):
  env-gated deploy gate — etcd + RustFS, two server processes each owning a
  shard, fleet client, kill-owner → failover → client re-resolves. Needs
  etcd+RustFS so NOT run here (syntax-checked only), like the existing HA smoke.
- **CLI enablement** (the gap the capstone surfaced): `nokv serve --shard-index N`
  (server registers its (prefix-from-id, index) identity on open) and a
  fleet-routing client mode when the etcd control backend is configured
  (`NoKvFsClient::connect_fleet`, feature-gated). Binary-level exposure of
  already-built library mechanics.

Validation: `cargo test -p nokv-server` 59/0 (incl. 4 fleet_e2e + shard-index
registration), `nokv` 24, `nokv-bench` tests pass; `cargo build`/`clippy` clean;
the 3 known `nokv-client` prefetch failures remain untouched.

**Residual risk:** the in-process tests/bench exercise routing + control-plane
logic but not real cross-machine latency or etcd quorum — that is exactly what
the (un-run-here) `run-multishard-fleet-smoke.sh` gate covers. The 3 pre-existing
`nokv-client` background-prefetch test failures predate this work (they pass at
the committed baseline; broken only by the uncommitted data-plane WIP) and are
out of scope for the multi-shard arc.

### 2026-06-15 etcd Session-Key Hardening + First Real-etcd Validation

Closes the last correctness item from the original safety review (the etcd
`acquire_after_failure` session-absence TOCTOU + keepalive resurrection), and
validates the control plane against a **real etcd** for the first time (in-docker),
upgrading it from "compiles" to "verified on real etcd."

- **Stable per-shard session key.** `shard_session_key` is now
  `{prefix}/sessions/{hex(shard_id)}` — no epoch/lease_id in the path, so there is
  exactly ONE session key per shard across owner generations. Its value carries
  the lease and it is attached to the owner's etcd lease, so it exists iff some
  owner's lease is live. Removed `shard_session_key_for`,
  `session_key_exists` (the out-of-txn GET), and `active_record_session` (the
  stale-record key reconstruction).
- **Atomic guards (no more TOCTOU).** `acquire_unassigned`/`acquire_after_failure`
  guard `create_revision(session_key)==0` (genuinely unowned — the previous
  owner's lease expired and its key auto-deleted) together with the record
  `mod_revision`, folding the previous-session-absent check INTO the transaction.
  A live old owner (keepalive holding the lease) keeps the key present →
  `create_revision != 0` → failover is refused, so two owners can no longer both
  win. `mark_serving`/`release` guard `lease(session_key)==my_lease`; `renew`
  validates the stable key still carries my lease.
- `InMemoryControlStore` deliberately unchanged (its permissive in-process
  failover backs the server test suite, which has no real lease TTL).
- **Real-etcd validation (docker, single-node etcd 3.5):** new env-gated
  `nokv-control` test `stable_session_key_fences_failover_until_lease_expires`
  PASSED on live etcd — proves a second acquire is rejected, failover is fenced
  while the old lease is alive, and failover succeeds at epoch+1 after the lease
  is revoked. The server-level `configured_etcd_control_store_expires_session_and_allows_failover`
  smoke was also run against the same live etcd.

Validation: `cargo test -p nokv-control` (default) 12/0; `cargo build/clippy
-p nokv-control --features etcd` clean; `cargo build -p nokv-server -p nokv
--features etcd` clean; env-gated etcd tests green against docker etcd. (The full
`run-multishard-fleet-smoke.sh` deploy gate still also wants RustFS + multi-process
orchestration; etcd is no longer the missing piece.)

### 2026-06-15 Real-Cluster Fleet Smoke PASSED (in-process → real cluster)

`scripts/run-multishard-fleet-smoke.sh` was run for real (brew etcd 3.6 +
homebrew RustFS + two separate `nokv serve` processes), and PASSED end to end —
the first validation of the whole data+metadata fleet on real binaries, not just
in-process.

- **What it proved:** server A owns `mount-1:/` (idx 0), server B owns
  `mount-1:/dataset` (idx 1), both via `nokv serve --control-backend etcd`; a
  fleet client routes each path to the owning *process* via etcd. Cross-shard
  writes/reads are correct and the inodes are shard-tagged into disjoint
  high-bit subspaces (`dataset_pre_inode = (1<<48)|3`, `other_inode = 3`), so two
  real processes genuinely served the two paths. After a checkpoint + a
  post-checkpoint write, B is killed; B' (new process, new port, EMPTY meta dir)
  failover-acquires at epoch 2, restores shard 1 from its checkpoint image +
  replays the shared log, and the fleet client transparently re-resolves and
  reads BOTH pre- and post-checkpoint files, then accepts a new write; fsck on
  both shards reports `dangling_count:0`. Metrics: `failover_observed_ms≈5345`
  (3s lease TTL), `owner_b2_startup_ms≈306` (restore + replay).
- **Bug the real cluster caught (in-process tests could not):** with a 3s etcd
  lease TTL, the server's hardcoded 5s renewal interval lapsed the lease before
  renewing — the exact TTL/renewal mismatch the original review predicted. Fixed
  at the root in the CLI (`nokv serve`, etcd backend): the renewal interval and
  the wall-clock self-fence deadline now derive from the configured etcd lease
  TTL (`interval = min(user, max(1s, ttl/3))`, `lease_ttl = ttl`), so a short TTL
  renews fast enough to stay alive and the self-fence trips no later than etcd's
  own expiry. A user-set faster interval is still respected.

This upgrades the control plane AND the full multi-process data/metadata path from
"in-process validated" to "validated on a real single-box cluster." Remaining for
"production-proven": real multi-machine network/partition + etcd quorum, and scale
benchmark numbers.

### 2026-06-15 Shard-Routing Scaling Sweep + Multi-Machine Runbook

- **Scaling sweep.** `metadata-shard-routing` gained `NOKV_BENCH_SHARD_ROUTING_{SHARDS,CONCURRENCY}`
  env knobs (defaults preserve the old shards=4/sequential row) and a concurrent
  fleet driver that hands each worker its own fleet `MetadataClient` (a shared
  client serializes on the per-connection `Mutex<TcpStream>`). Routed metadata
  throughput scales with shard count on a single multi-core box: e.g. one
  observed run 1→2→4→8 shards = 7.6k→15.3k→26.7k→30.4k ops/s (≈4× at 8, fan-out
  even, skew 0), plateauing near the box's thread/RPC limit. Absolute numbers are
  machine-load-dependent; the row/CSV `caveat` states this is a single-box,
  in-process-server, shared-in-memory-control microbench — it shows on-box shard
  parallelism, NOT cross-machine scaling.
- **Multi-machine runbook.** `docs/multishard-fleet-runbook.md`: how to deploy the
  fleet across machines (shared etcd quorum + shared S3, one `nokv serve` per
  shard with `--shard-id`/`--shard-index`/`--node-id`, fleet clients pointed at
  the control plane, failover via `--failover-from-epoch`), the lease-TTL/renewal
  coupling guidance, how to point the local smoke at external etcd+S3, and the v1
  limits. The cross-machine smoke + real scale curve remain the last production
  gate (need ≥2 machines + multi-node etcd).

### 2026-06-15 Graft hardening — lifecycle/GC, cross-shard atomicity, recovery fix

Made the cross-shard graft production-grade (three follow-ups from the graft
landing). All verified end-to-end on a 2-shard fleet + FUSE mount.

- **Recovery fallback crash (fixed).** `recover_allocator_state`'s fallback scan
  (used when the durable `allocator_key` is absent) crashed on a populated real
  Holt store: `RecordFamily::CommandDedupe` stores a header-less 24-byte
  dedupe-result (no version/kind byte), which the scan's `decode_current_value`
  reads as "unknown kind 0". Root fix: `CommandDedupe` removed from
  `ALLOCATOR_RECOVERY_FAMILIES` (now 12) — it carries no inode and its commit
  version is redundant with the Inode/Dentry/Gc/Watch records written in the same
  commit, so removal can't lower the recovered high-water; it is the ONLY
  non-standard-encoded family (verified). Real-store test forces the fallback
  (deletes `allocator_key`) and recovers correctly without panic.
- **Guard remove/rename of a graft point (prevents silent corruption).**
  `prepare_remove_empty_dir` checked `PrefixEmpty` on the child's dentries *in the
  parent shard* — for a graft the contents live in the child shard, so the parent
  saw "empty" and `rmdir /dataset` SILENTLY orphaned the whole child subtree. Fix:
  `is_graft_child(entry) = entry.attr.inode.shard_index() != self.shard_index()`
  guards `remove_empty_dir`/`remove_file`/`rename` (src+dst, after the self-rename
  no-op) → new `MetadError::GraftPoint` → `WireMetadataError::GraftPoint` → FUSE
  `EBUSY`. Also added a typed `WireMetadataError::DirectoryNotEmpty` (it was
  collapsing to a generic Backend string over the wire). No-op for shard 0.
- **GC pin (verified, no pin needed).** NoKV-FS has no logical orphan-inode GC;
  reclamation is object-block (explicit Gc queue), snapshot-pin expiry, history
  prune, and Holt physical-frame gc — none reclaim a live current-tree record. The
  grafted subtree root has a live local dentry + Inode record on the child shard,
  so it is referenced and unreclaimable. Test runs the full child-shard GC surface
  and asserts the subtree root + nested dir + file body all survive.
- **Cross-shard atomicity + lifecycle.** `ShardRecord` gained
  `subtree_root_inode: Option<u64>` + `ControlStore::set_subtree_root_inode`
  (in-mem + etcd CAS). `register_graft` records it durably FIRST (the atomic
  registration point), then writes the reconcilable parent graft dentry.
  `reconcile_grafts()` (client) + a `Server::reconcile_local_grafts()` startup hook
  re-create a graft dentry lost after the control record landed (self-healing);
  CLI `reconcile-grafts`. `unregister_graft(prefix)` + `remove_graft` meta op +
  `RemoveGraft` RPC + CLI `unregister-graft`: clears the control record first (so
  reconcile can't resurrect), then rmdir's the child subtree on the CHILD shard
  (emptiness checked where contents live — non-empty → `DirectoryNotEmpty`, graft
  left intact), then removes the parent graft dentry. Idempotent.
- **End-to-end proof** (etcd + RustFS + 2 shards + FUSE): control plane records
  `subtree_root_inode=(1<<48)|2` for `/dataset`; `reconcile-grafts` idempotent;
  multi-shard FUSE traversal still works; **`rmdir /dataset` via FUSE now returns
  EBUSY** (was a silent subtree leak) and the subtree stays intact;
  `unregister-graft /dataset` removes the graft + empty child subtree and `ls /`
  no longer shows it. Full workspace tests green; fmt/clippy clean.
- **Residual (flagged)**: server-startup reconcile heals only locally-owned /
  root-level parents — deeply-nested cross-shard graft parents rely on the
  `reconcile-grafts` CLI (fleet-routed). Recursive graft removal is out of scope
  (non-empty child → DirectoryNotEmpty). `unregister_graft` is self-healing on
  retry, not single-shot atomic across shards.

### 2026-06-15 Cross-shard graft point — multi-shard FUSE traversal works

Implemented the create-time graft so a parent shard's namespace can traverse into
a subtree shard — the gap that blocked unified multi-shard FUSE mounts. Official
fio/mdtest now run across both shards through one mount.

- **`create_graft` meta op** (`nokv-meta/service/namespace.rs`): writes a dentry
  projection in the parent shard whose `child` is the child shard's subtree-dir
  inode, with an embedded **stub dir attr** and **no `inode_key` Inode record**.
  Reads need no change (read_dir_plus/lookup read the embedded attr); FUSE
  `getattr/readdir` on that inode route by high bits to the child shard for the
  real attr + contents. `create_graft_command` emits exactly one mutation (the
  Dentry projection) under predicates parent-Exists + dentry-NotExists.
- **Shard-aware allocator recovery (P0 safety)**: `recover_allocator_state` now
  takes `shard_index` and only folds an inode into the allocator high-water when
  `inode.shard_index() == shard_index`. Without this, the fallback scan would fold
  a graft dentry's *foreign* child inode and push the parent shard's allocator into
  another shard's id subspace → cross-shard id collision. `open_existing` threads
  the shard index (callers updated; clean break, no shim). Two unit tests:
  graft-traversable-without-inode-record, and recovery-does-not-poison-parent
  (shard-scoped, not a blanket skip). Shard 0 is a no-op (`compose(0,x)==x`).
- **RPC/CLI**: `MetadataRpcRequest::CreateGraft`, routed by the `parent` inode to
  the parent shard; `MetadataClient::create_graft` + `register_graft(prefix)`
  (mkdir on child shard → graft dentry on parent shard, idempotent); CLI
  `register-graft PREFIX`. Also fixed `Command::Mount`/`MountSnapshot` to build a
  fleet `MetadataClient::fleet` under `--control-backend etcd` (previously
  single-shard only → refused under a fleet).
- **End-to-end proof** (etcd + RustFS + 2 shard servers + fleet FUSE mount):
  `register-graft /dataset` → `ls /` now shows the `dataset` graft (inode
  `(1<<48)|2`, shard 1); FUSE `stat /dataset` (which ENOENT'd before) succeeds;
  mkdir/write/read into `/dataset` (shard 1) and `/root_dir` (shard 0) all work
  through one mount. **Official benchmarks on the unified 2-shard namespace**:
  mdtest `/` (shard 0) dir-stat 18.4K/s, file-create 660/s, file-read 4.5K/s;
  mdtest `/dataset` (shard 1, via graft) dir-stat 18.4K/s, file-stat 3.8K/s,
  file-read 3.9K/s; fio on `/dataset` write 662 MB/s (646 IOPS) / read 303 MB/s
  (296 IOPS). Full workspace tests green; fmt/clippy clean.
- **Open items (flagged, not yet done)**: (1) cross-shard lifecycle of the graft
  target — removing the parent graft dentry doesn't reap the child subtree, and
  the child shard's GC has no signal that an external graft pins its subtree root
  (needs control-plane refcounting). (2) `register_graft` is idempotent but not
  atomic across the two shards (mkdir-then-graft; re-run heals). (3) Pre-existing
  latent bug surfaced: `recover_allocator_state`'s fallback scan chokes on
  `RecordFamily::CommandDedupe` rows (no value-header) — masked in prod by the
  durable allocator record; spawned as a separate background task.

### 2026-06-15 Full-architecture validation + official fio/mdtest + FUSE fleet fix

Exercised the **complete** distributed path (client → etcd control plane → owning
shard's server → its Holt engine) instead of the single-server shortcut, and ran
the industry-standard benchmarks.

- **Control-plane routing confirmed** end to end. Brought up etcd + RustFS + two
  shard servers (default `/` index 0, `/dataset` index 1) registered in etcd; the
  fleet client resolved each path to its shard via etcd and the inodes are
  high-bit shard-tagged: `/root_file.bin` → inode 1026 (shard 0),
  `/dataset/ds_file.bin` → inode `(1<<48)|3` (shard 1). So path → etcd shard map
  → shard server → separate Holt engine is real; the data plane still goes client
  → RustFS directly.
- **Fixed a real gap: the FUSE `mount` command ignored the control plane.**
  `Command::Mount`/`MountSnapshot` built a single-shard `MetadataClient` pinned to
  `--server-bind` (default :7777 → connection refused under a fleet). Added
  `open_mount_metadata` (mirrors `open_client`): with `--control-backend etcd` the
  mount now builds a fleet `MetadataClient::fleet` and routes via the control
  plane. nokv fmt + clippy (`--features etcd`) clean.
- **Found a deeper gap (not yet fixed): the cross-shard graft point isn't
  created**, so multi-shard FUSE *traversal* can't cross into a subtree shard.
  `mkdir /dataset` routes by longest-prefix to shard 1 and creates `/dataset` as
  shard 1's namespace root, but no `/dataset` dentry is planted in shard 0's root
  pointing at it. Path-prefix access (CLI, Python SDK, fleet client) works because
  it routes by the whole path string; FUSE `lookup(parent_ino, name)` goes shard 0
  → no dentry → ENOENT. Registering a subtree shard must also write the parent-
  shard graft dentry → child-shard subtree-root inode (the create-time graft the
  endgame design calls for). Until then, multi-shard FUSE mounts can't traverse
  graft points; single-shard FUSE and all path-based access are fine.
- **Official benchmarks through the real POSIX path** (FUSE → framed RPC → Holt →
  RustFS), single-shard mount, uid-matched: **mdtest** dir-create 1,303/s, dir-stat
  14,717/s, file-create 500/s, file-stat 2,644/s, file-read 4,014/s, tree-create
  1,130/s, removals 1.6–2.8K/s; **fio** seqwrite ~489 MB/s (477 IOPS @1 MiB),
  seqread ~249 MB/s (243 IOPS). Single box, localhost RustFS — FUSE+RPC per-op
  latency dominates, not a saturation test.

### 2026-06-15 Python SDK: write + namespace + checkpoint + torch

Closed the read-only gap in the Python SDK. The Rust client already implemented
the full write/namespace/snapshot/publish surface; this work *binds* it (mostly
PyO3 + Python, little new core logic).

- **PyO3 bindings (`crates/nokv-python/src/lib.rs`)** on `Client`, every network
  call releasing the GIL via `py.detach` (same pattern as the read methods):
  write — `put_artifact(..., replace=)` (atomic immutable-generation publish with
  staged-object rollback on metadata failure), `create_file`, `mkdir`; namespace
  — `lookup`, `stat`, `exists`, `list_dir` (internally paged), `remove_file`,
  `rmdir`, `rename(..., replace=)`; snapshots — `snapshot`, `snapshot_pin`,
  `retire_snapshot`, `renew_snapshot`, `cat(path, snapshot_id=)`. Added
  `nokv-meta`/`nokv-types` as direct deps for the return-type conversions
  (`dentry_to_py`/`path_metadata_to_py`/`attr_to_py`/`snapshot_pin_to_py`).
- **fsspec completion (`python/nokv/fsspec.py`)**: `NoKVFileSystem` is now a full
  read+write filesystem — `info`/`ls`/`exists`/`mkdir`/`makedirs`/`rm_file`/
  `rmdir`/`mv`/`pipe_file`/`cat_file` + a writable `NoKVBufferedFile` (`_open`
  read via batch range path, write accumulates and publishes one immutable
  artifact on close). Unlocks the fsspec ecosystem (HF datasets, pyarrow, pandas,
  `torch.save`→fsspec file). The latency-sensitive batch range-read fast path is
  retained as NoKV-specific methods.
- **Checkpoint module (`python/nokv/checkpoint.py`)**: `publish_checkpoint` /
  `publish_shard`+`commit_checkpoint` (the distributed barrier path) / `latest_step`
  / `resolve_checkpoint` / `load_shard` / `load_checkpoint`. The manifest is the
  atomic commit point (written last; a partial checkpoint is invisible to
  resolution). Unit-tested with an in-memory fake client — 5/5 pass incl.
  partial-checkpoint-invisibility and the per-rank publish+commit flow
  (`crates/nokv-python/tests/test_checkpoint.py`, pytest-optional).
- **PyTorch integration (`python/nokv/torch.py`, optional import)**:
  `NoKVRangeDataset`/`NoKVIterableDataset` over the batch range path (fork-safe via
  a per-worker lazy client factory); a `torch.distributed.checkpoint` backend
  (`NoKVStorageWriter`/`NoKVStorageReader` + `save_checkpoint`/`load_checkpoint`)
  whose **read path coalesces every DCP `ReadItem` per shard into one
  `read_ranges_batch`** — so a resharding load becomes coalesced prepared range
  reads, not N point GETs. DCP type imports are resolved defensively (the ABC has
  shifted across torch minors); **needs a validation pass against a specific torch
  version** since torch is not available in this build env.

Gates: `cargo build -p nokv-python` clean (no warnings); all Python modules
`py_compile` clean; checkpoint unit tests 5/5. A torch-env DCP round-trip is the
one remaining unvalidated piece (no torch in this env).

**Live end-to-end validation (maturin develop + real server + RustFS).** Built
the wheel with `maturin develop` into a venv and ran `tests/test_live_sdk.py`
against a single `nokv serve` + RustFS — **5/5 pass**: write/read/namespace
round-trip, replace-publishes-new-generation, subtree-snapshot reproducible read,
the full fsspec read+write surface, and checkpoint publish/resolve. The live run
caught **three real binding bugs**, now fixed: (1) `put_artifact` defaulted
`manifest_id=""` → `InvalidChunkLayout`; now derives `artifacts/<path>` like the
CLI; (2) `replace=True` on a missing path returned not-found; now create-or-replace
(the natural overwrite for fsspec `wb` + idempotent checkpoint re-publish);
(3) `cat(snapshot_id=)` used `read_artifact_at_snapshot` (artifact-repo
resolution, which the CLI's `cat-snapshot` also gets wrong for subtree snapshots)
→ switched to the path-based `read_file_path_at_snapshot`, so the SDK correctly
reads subtree snapshots (which re-root the snapshotted dir as `/`).

**Benchmarks (single box, localhost RustFS).** Engine, in-process via
`nokv-bench`: mdtest-easy 172K ops/s, mdtest-hard 215K ops/s,
metadata-concurrent-read 26K ops/s; checkpoint-publish 2.2K ops/s; read path
cold→warm — ai-dataset-batch-read 1.2K→23.6K ops/s, ai-shard-range-read
66K→696K ops/s (340 MiB/s warm). SDK path (Python→RPC→engine→RustFS, 64 KiB
objects): put_artifact 489 ops/s (31 MiB/s), cat 1.25K ops/s (78 MiB/s),
`read_ranges_batch` per-call 8.2K ops/s (510 MiB/s), one coalesced batch over 300
files **16.9K ranges/s / 1.06 GiB/s**; checkpoint publish 62 MiB/s, resolve+load
212 MiB/s. Caveat: small op counts on one box over localhost RustFS — these are
latency-bound per-op figures and the coalesced-batch bandwidth, not a saturated
multi-node throughput test.

### 2026-06-15 Data-plane prefetch tests realigned to the readahead policy

The 3 long-standing `nokv-client` prefetch failures were **stale tests**, not a
data-plane bug. The WIP readahead policy was deliberately changed to suppress
prefetch on a stream's *first* (possibly random) read — it now arms only on the
second sequential read (`initial_read_is_large_enough_for_readahead` +
`advance_readahead_window`), and the object-layer's own test was updated to match
(`first.readahead == None`). The three `service_tests.rs` tests still encoded the
old "small first read forward-prefetches, next read reuses" contract, so their
canned positional response sequences shifted by one → `unexpected metadata rpc
result BodyReadPlan`. Proven by HEAD-passes / working-tree-fails in a throwaway
worktree. Rewrote all three to the new policy (a 3-read sequential stream; the
prefetch arms on read 2 and read 3 reuses the warmed plan). #1/#2 assert
read-plan-cache miss/hit; #3 (object-data dedup) now asserts the stable dedup
invariants (prefetch arms + fully completes, foreground reuses cached bytes)
rather than exact GET counts. Whole workspace green (nokv-client 92/0, object
106/0, meta 179/0, server 59/0). Open observation: under the new policy three
tiny sequential reads of an 18-byte file fetch ~2× the file (forward-fill window
overlaps the readahead prefetch) — likely a tiny-object test artifact, flagged
for a look at whether forward-fill and readahead should coordinate.

### 2026-06-15 Holt Engine Upgrade 0.5.4 → 0.7.1

Bumped the `holt` metadata-engine dependency (workspace Cargo.toml) from 0.5.4 to
0.7.1 (latest on crates.io), kept `default-features = false`. Despite crossing two
0.x minors, the upgrade is fully compatible with NoKV's usage: no API breakage
(the surface we touch — `DB`/`Tree`/`TreeConfig`, `CheckpointImage`,
`DBAtomicBatch`, `RecordVersion`, `KeyScanOutcome`/`RangeEntry`/`KeyRangeEntryRef`,
and `open`/`atomic`/`snapshot`/`view`/`range`/`checkpoint`/`export`+`install_checkpoint_image`/`gc`
— compiled clean) and no behavioral regression (nokv-meta 179/0, nokv-server 59/0,
incl. all checkpoint/snapshot/range/gc + failover-restore paths). `cargo build
--workspace` clean; `cargo clippy -p nokv-meta` clean. Only the 3 pre-existing
`nokv-client` data-plane prefetch failures remain (unrelated). Cargo.lock updated
to holt 0.7.1.

### 2026-06-17 Async publish journal recovery hardening

Closed a recovery hole in the FUSE async-publish journal. The journal frame digest
now covers the frame kind, payload length, and payload, so a corrupted frame kind
cannot reinterpret a valid publish body as another record type. Tombstones are
accepted only when the payload is exactly `(inode, generation)`; trailing bytes now
make the frame a corrupt tail and keep earlier publish records live instead of
retiring them.

This protects the write-back ACK boundary: once `flush`/`release` has acknowledged
that cache blocks and the pending publish record are durable, replay cannot lose
that record because of a malformed tombstone tail.

Validation:

- `cargo fmt --all -- --check`
- `cargo test -p nokv-fuse publish_journal -- --nocapture`
- `cargo check -p nokv-fuse`

Next gap: decide the production-vs-benchmark default for tiered object writes.
`HotThenBackgroundCold` is good for local-NVMe latency, but the production
metadata-visible invariant should either wait for cold/object-store durability or
document an explicit RPO window for local-hot acknowledged data.

### 2026-06-17 Tiered object write default aligned with DFS durability

Changed `TieredObjectStoreOptions::default()` from `HotThenBackgroundCold` to
`ColdThenHot`. A default tiered backend now writes the cold/object-store tier
before returning from `put`, then fills the local hot tier as cache. This makes
the default object-store ACK boundary match the production DFS invariant that
metadata should not make bytes visible before the durable object tier has them.

The AI benchmark path still opts into `HotThenBackgroundCold` explicitly where
it is measuring local-NVMe hot-write latency, so benchmark rows stay honest about
the durability mode they exercise instead of inheriting a cache-first default.

Validation:

- `cargo test -p nokv-object tiered_object_store -- --nocapture`
- `cargo test -p nokv-bench hot_root -- --nocapture`
- `cargo check -p nokv-object`
- `cargo fmt --all -- --check`

Next gap: rerun the layered data-plane benchmark with the durability mode called
out in the row labels, so `ColdThenHot` production writes and
`HotThenBackgroundCold` cache-first writes are not compared as the same system.

### 2026-06-17 Layered data-plane durability labels

Benchmark rows now include the tiered write policy in the `object_backend` label
whenever a local hot tier is active. Examples:

- `rustfs+local-hot+put=cold-only` for read workloads that seed durable cold
  objects before measuring hot reads.
- `rustfs+local-hot+put=cold-then-hot` for production-style tiered writes that
  acknowledge after the cold object store before filling local hot.
- `rustfs+local-hot+put=hot-background` for cache-first write workloads that
  acknowledge after local-hot staging and enqueue the cold put in the background.

This keeps the existing layered merge/baseline scripts working because the
durability mode stays inside an existing grouping key instead of adding a new
column that older consumers might ignore.

Updated all layered entrypoints that own this label: Rust L1 `nokv-bench`,
Python/fsspec L1 smoke, and mounted L2 NoKV-vs-JuiceFS scripts.

Validation:

- `cargo test -p nokv-bench hot_root -- --nocapture`
- `cargo test -p nokv-bench csv_row_reports_hot_path_attribution -- --nocapture`
- `cargo check -p nokv-bench`
- `cargo fmt --all -- --check`
- `bash -n scripts/run-ai-dataplane-layered-matrix.sh scripts/run-python-fsspec-smoke.sh scripts/lib/fs-bench-common.sh`
- `NOKV_AI_LAYERED_RUN_PYTHON_L1=0 NOKV_AI_LAYERED_RUN_L2=0 NOKV_AI_LAYERED_PROFILE=smoke NOKV_AI_LAYERED_CASES=sparse-exact scripts/run-ai-dataplane-layered-matrix.sh`

Smoke result path:
`bench/results/ai-dataplane-layered-Mac-20260617T030110Z/layered.aggregate.csv`.
The Rust SDK L1 smoke emitted `rustfs+local-hot+put=cold-only` rows for
`ai-shard-range-read`: cold 1,312 samples/s and warm 134,690 samples/s on this
single local RustFS run. This validates the row labelling and hot-read path; it
is not a saturated multi-node performance claim.

Next gap: run the full layered matrix with Python L1 and mounted L2/JuiceFS
enabled, then compare only rows with matching boundary, cache state, and
durability label.

### 2026-06-17 Full-layer AI data-plane smoke

Ran the layered smoke with Rust SDK L1, Python/fsspec L1, and mounted
NoKV-vs-JuiceFS L2 enabled for both default sparse-read shapes. The L2 runs used
`fsync=0`, `concurrency=1`, `profile=smoke`, RustFS on localhost, Redis-backed
JuiceFS metadata, and NoKV's local Holt metadata server. The rows carry the new
durability labels:

- Rust SDK L1 read-seed rows: `rustfs+local-hot+put=cold-only`
- Python/fsspec L1 and mounted NoKV L2 rows:
  `rustfs+local-hot+put=cold-then-hot`
- JuiceFS L2 rows: `rustfs`

The Python/fsspec smoke initially failed because the benchmark workdir lives
under a path with spaces, and RustFS parsed that path as multiple endpoints.
`scripts/run-python-fsspec-smoke.sh` now uses a no-space temp RustFS data
directory when the workdir contains whitespace, while keeping logs and CSVs in
the requested result directory.

`sparse-exact` result path:
`bench/results/ai-dataplane-layered-Mac-20260617T032714Z/layered.aggregate.csv`.

- Rust SDK L1: cold 1,706 samples/s, warm 148,945 samples/s.
- Python/fsspec L1: cold 12,961 samples/s, warm 15,979 samples/s.
- Mounted L2 cold: NoKV 46,230 ops/s vs JuiceFS 21,838 ops/s, NoKV 2.12x.
- Mounted L2 warm: NoKV 179,576 ops/s vs JuiceFS 64,272 ops/s, NoKV 2.79x.

`sparse-coalesced` result path:
`bench/results/ai-dataplane-layered-Mac-20260617T032825Z/layered.aggregate.csv`.

- Rust SDK L1: cold 1,961 samples/s, warm 443,610 samples/s.
- Python/fsspec L1: cold 71,055 samples/s, warm 506,387 samples/s.
- Mounted L2 cold: NoKV 106,079 ops/s vs JuiceFS 19,425 ops/s, NoKV 5.46x.
- Mounted L2 warm: NoKV 186,244 ops/s vs JuiceFS 48,598 ops/s, NoKV 3.83x.

Interpretation limits: these are single-repeat smoke rows on one local machine,
not saturated multi-node throughput numbers. The comparison is valid only inside
matching L2 rows with the same cache state, same generated shape, same object
endpoint, and explicit durability label.

Validation:

- `bash -n scripts/run-python-fsspec-smoke.sh scripts/run-ai-dataplane-layered-matrix.sh scripts/lib/fs-bench-common.sh`
- `NOKV_AI_LAYERED_PROFILE=smoke NOKV_AI_LAYERED_CASES=sparse-exact NOKV_AI_LAYERED_L2_CONCURRENCY=1 NOKV_AI_LAYERED_L2_CACHE_STATES='cold warm' NOKV_AI_LAYERED_L2_FSYNC=0 NOKV_AI_LAYERED_PYTHON_CACHE_STATES='cold,warm' scripts/run-ai-dataplane-layered-matrix.sh`
- `NOKV_AI_LAYERED_PROFILE=smoke NOKV_AI_LAYERED_CASES=sparse-coalesced NOKV_AI_LAYERED_L2_CONCURRENCY=1 NOKV_AI_LAYERED_L2_CACHE_STATES='cold warm' NOKV_AI_LAYERED_L2_FSYNC=0 NOKV_AI_LAYERED_PYTHON_CACHE_STATES='cold,warm' scripts/run-ai-dataplane-layered-matrix.sh`

Next gap: run the same two cases with more repeats and L2 concurrency `1 4`,
then inspect p95/variance and decompose rows before making a stronger
NoKV-vs-JuiceFS claim.

### 2026-06-17 Repeat-3 layered L2 diagnosis and FUSE concurrent-read cleanup

Repeated the layered smoke for both sparse-read shapes with `L2_REPEATS=3`,
`L2_CONCURRENCY="1 4"`, `L2_CACHE_STATES="cold warm"`, and `fsync=0`.
Artifacts:
`bench/results/ai-dataplane-layered-Mac-20260617T034848Z/layered.aggregate.csv`.

Mounted L2 median results:

- `sparse-exact`, p=1 cold: NoKV 42,554 ops/s vs JuiceFS 25,164 ops/s
  (`1.69x` NoKV).
- `sparse-exact`, p=1 warm: NoKV 137,474 ops/s vs JuiceFS 65,795 ops/s
  (`2.09x` NoKV).
- `sparse-exact`, p=4 cold: NoKV 28,012 ops/s vs JuiceFS 29,042 ops/s
  (rough parity, JuiceFS `1.04x`).
- `sparse-exact`, p=4 warm: NoKV 78,199 ops/s vs JuiceFS 193,560 ops/s
  (`2.48x` JuiceFS).
- `sparse-coalesced`, p=1 cold: NoKV 96,789 ops/s vs JuiceFS 40,865 ops/s
  (`2.37x` NoKV).
- `sparse-coalesced`, p=1 warm: NoKV 155,418 ops/s vs JuiceFS 63,606 ops/s
  (`2.44x` NoKV).
- `sparse-coalesced`, p=4 cold: NoKV 36,020 ops/s vs JuiceFS 36,112 ops/s
  (tie).
- `sparse-coalesced`, p=4 warm: NoKV 187,982 ops/s vs JuiceFS 164,467 ops/s
  (`1.14x` NoKV throughput, but NoKV p99 is worse).

L1 rows in the same artifact remain separate from the mounted L2 comparison.
Python/fsspec p=4 warm reached 46,500 ops/s for `sparse-exact` and 551,328
ops/s for `sparse-coalesced`; Rust SDK p=4 warm reached 141,463 ops/s and
671,181 ops/s respectively.

Decompose showed the exact sparse path is not primarily object-GET bound:
`sparse-exact` cold p=1 served 128 KiB of semantic data through 256 FUSE
callbacks and 4 MiB of FUSE read request bytes. Object reads were only 24 GETs
and 524 KiB because cache/prefetch handled most bytes. The remaining p=4 gap is
therefore FUSE request scheduling, read-handle locking, cache-hit copy cost, and
macOS FUSE small-request behavior, not more metadata or object-cache design.

Implemented the cleanup step that is valid on all platforms:

- FUSE read handles now bypass the per-handle `FileReadPipeline` mutex for
  reads that do not need stateful sequential/sparse-forward pipeline behavior.
  Those reads use the existing backend direct read path, shared read-plan cache,
  and object/block cache, while the pipeline still observes the read boundary so
  a later contiguous read can re-enter stateful prefetch correctly.

Tried to make `4` FUSE workers the cross-platform default, but `fuser` rejects
`n_threads != 1` outside Linux. The default therefore remains Linux `4` and
non-Linux/macOS `1`; macOS mounted p=4 rows are still constrained by the FUSE
worker model and should not be treated as proof of Linux multi-worker behavior.

Validation so far:

- `cargo test -p nokv-object file_read_pipeline -- --nocapture`
- `cargo test -p nokv-fuse read_handle -- --nocapture`
- `cargo test -p nokv-fuse default_fuse_threads_match_parallel_read_baseline -- --nocapture`
- p=4 mounted rerun attempt:
  `NOKV_AI_LAYERED_RUN_RUST_L1=0 NOKV_AI_LAYERED_RUN_PYTHON_L1=0 NOKV_AI_LAYERED_RUN_L2=1 ... scripts/run-ai-dataplane-layered-matrix.sh`
  exposed the non-Linux `fuser` worker limit and was stopped before producing
  comparable rows.
- After restoring the non-Linux default to `1`, a minimal mounted L2 p=4 cold
  smoke passed:
  `NOKV_BENCH_PROFILE=smoke NOKV_BENCH_CACHE_STATES=cold scripts/run-fs-benchmark.sh --quick --repeats 1 --concurrency 4 --product-workloads ai_shard_range_read --primitive-workloads none --skip-real-tools ...`.
  It emitted NoKV `33,893 ops/s`, p99 `352.72us`, and JuiceFS `33,986 ops/s`,
  p99 `420.09us`; this is a mount-health smoke and not a stable performance
  claim.

Next gap: run a focused p=4 mounted L2 rerun after the FUSE concurrent-read
cleanup on Linux or force macOS `--fuse-threads 1` for apples-to-apples local
diagnostics, then compare only the same `sparse-exact`/`sparse-coalesced` rows
and check whether FUSE read request counts stay constant while throughput
improves.

### 2026-06-13 Combined Gate

- `cargo fmt --all -- --check`: passed.
- `git diff --check`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-control-target cargo test -p nokv-control`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-meta log::tests`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-protocol -p nokv-meta -p nokv-server -p nokv-client open_path_read_plan_batch`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-server rpc_open_read_plan_batch_returns_one_result_per_request`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-client metadata_client_open_read_plan_batch_returns_plans`: passed.
- `CARGO_TARGET_DIR=/tmp/nokv-p1-target cargo test -p nokv-client service_file_client_read_paths_uses_single_batch_open`: passed.
