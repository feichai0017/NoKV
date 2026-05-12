# DeltaChannel optimization handoff

This note records the current LangGraph DeltaChannel optimization state for the
NoKV checkpoint integration. It is intended as a handoff document for the next
engineering session, not as end-user documentation.

## Constraints

- Treat NoKV as a KV store with native fsmeta namespace semantics, not as a SQL
  store. Use directories, dentries, inode `opaque_attrs`, `LookupPlus`, and
  `ReadDirPlus` as first-class primitives.
- Use LangGraph and Postgres saver behavior as the contract reference only.
  Avoid copying SQL or JSONB-shaped implementation details into NoKV.
- Keep `writes/<checkpoint_id>/...` as the canonical pending-write source.
  `delta_channels/<channel>/...` is a derived acceleration index.
- Do not cache mutable `heads/latest` or `thread-tombstone` unless the
  cross-writer/delete semantics are explicitly designed.

## LangGraph contract context

LangGraph 1.2.0 introduced DeltaChannel as a beta feature. The relevant saver
contract is `get_delta_channel_history(config, channels)`, which returns, per
channel:

- `writes`: pending writes in oldest-to-newest replay order;
- `seed`: the nearest ancestor checkpoint value for that channel when available.

Correctness depends on walking the target checkpoint's parent chain, so forks
must be filtered by parent pointers. The default LangGraph saver implementation
walks the chain and scans each ancestor checkpoint's writes. The Postgres saver
has a two-stage fast path, but its SQL shape is only useful as behavioral
guidance.

## Current layout

The current fsmeta layout in `src/langgraph/checkpoint/nokv/layout.py` is a good
fit for NoKV-native optimization:

- `langgraph/threads/<thread>/namespaces/<ns>/checkpoints/c~<checkpoint_id>`
  stores `CheckpointEntryAttrs` in inode `opaque_attrs`. These attrs include
  `checkpoint_id`, `parent_checkpoint_id`, the checkpoint body ref, and
  `seed_body_refs_by_channel`.
- `.../blobs/<channel>/v~<version>` stores immutable `ChannelBlobEntryAttrs`.
- `.../writes/c~<checkpoint_id>/w~<task_id>~<idx>` stores canonical
  `WriteEntryAttrs`.
- `.../delta_channels/<channel>/dw~<checkpoint_id>~<task_id>~<idx>` stores
  derived `DeltaWriteEntryAttrs` for channel-first delta reads.
- `.../heads/latest` stores mutable head metadata.
- `.../thread-tombstone` stores logical delete metadata.

The layout intentionally keeps immutable entries addressable by stable names and
keeps mutable state in separate entries. This is what lets the saver use
directory metadata pages instead of remote point reads for many read paths.

## Current implementation

Main files:

- `src/langgraph/checkpoint/nokv/saver.py`
  - implements `NoKVCheckpointSaver`;
  - supports `put`, `put_writes`, `get_tuple`, `list`, `delete_thread`, and
    `get_delta_channel_history`;
  - has a local directory inode cache for path resolution;
  - uses `_list_dir()` with paged `ReadDirPlus` (`_DIR_PAGE_LIMIT = 1024`).
- `src/langgraph/checkpoint/nokv/layout.py`
  - defines namespace paths and typed `opaque_attrs` payloads.
- `src/langgraph/checkpoint/nokv/body_store.py`
  - keeps large checkpoint/channel/write bodies outside fsmeta.
- `src/langgraph/checkpoint/nokv/_bench_metrics.py`
  - wraps fsmeta calls for benchmark-only phase and path-category metrics.
- `bench/delta_channel_benchmark.py`
  - runs LangGraph official DeltaChannel-shaped workloads and records
    write/read/delta/storage metrics.

## Implemented fast paths

### Stage 1: checkpoint seed refs in checkpoint attrs

Each checkpoint attrs payload now stores `seed_body_refs_by_channel`. Delta
history no longer needs to hydrate checkpoint bodies or inspect channel blob
entries just to find each channel's nearest seed.

Legacy behavior:

- If `seed_body_refs_by_channel` is missing on the target or an ancestor, the
  saver falls back to the canonical parent-chain reader.

### Stage 1.5: checkpoint directory page walk

`_delta_replay_window()` now calls `_load_checkpoint_attrs_map()`, which reads
the `checkpoints/` directory through `ReadDirPlus`, parses
`CheckpointEntryAttrs` from inode `opaque_attrs`, builds:

- `checkpoint_id -> CheckpointEntryAttrs`;
- on-path `eligible_by_ch`;
- `seed_ref_by_ch`;
- `ancestor_rank`.

Then it walks parent pointers locally. Fork correctness still comes from
`parent_checkpoint_id`, not from sorting checkpoint ids.

Important edge cases:

- target checkpoint missing: return an empty delta result;
- malformed checkpoint attrs: return `None` and fall back;
- legacy missing `seed_body_refs_by_channel`: return `None` and fall back.

This changed Stage 1 from O(parent-chain length) remote `lookup_plus` calls to a
small number of `ReadDirPlus` pages.

### Stage 2: materialized delta channel index

`put_writes()` still writes canonical `writes/<checkpoint_id>/...` entries. When
`enable_delta_index` is true, it also writes per-channel index entries under
`delta_channels/<channel>/...`.

`_load_delta_index_writes()` scans one directory per requested channel and
filters by the eligible checkpoint set computed in Stage 1. It sorts by ancestor
rank, task id, and write idx so the result is oldest-to-newest for replay.

Crash atomicity note:

- The canonical write and derived delta-index write are not currently
  cross-file atomic. If the index is absent or untrusted, the saver falls back
  to the canonical parent-chain path.
- A future generic fsmeta batch-mutate API could close this window without
  changing the public saver contract.

### Write path: create-first for append-only entries

The saver now uses fsmeta `Create` directly for entries whose names are stable
and whose content is immutable or idempotent:

- checkpoint entries use immutable create with conflict-time attr compare;
- channel blob entries use immutable create with conflict-time attr compare;
- ordinary pending writes use create-or-ignore on `ALREADY_EXISTS`;
- ordinary delta-index entries use create-or-ignore on `ALREADY_EXISTS`;
- special `WRITES_IDX_MAP` channels still use upsert because LangGraph expects
  deterministic negative-index writes to be replaceable;
- `heads/latest` and `thread-tombstone` still use mutable upsert.

This is a saver-layer change that better expresses existing LangGraph semantics
through fsmeta's native create-if-absent operation. It does not require a new
metadata-layer API.

During the first LangGraph100 run after this change, a real concurrent
directory-create race surfaced: `Create` could return `ALREADY_EXISTS` while a
follow-up `Lookup` still saw a transient miss. The saver conflict path now uses
`ReadDirPlus` as a directory-page recovery path after create conflicts. This
matches the metadata architecture: `ReadDirPlus` bypasses single-entry negative
lookup state and reads the namespace page directly.

### Bounded immutable metadata cache

The saver now keeps one bounded in-process LRU cache for immutable metadata:

- checkpoint attrs keyed by `(thread_id, checkpoint_ns, checkpoint_id)`;
- channel blob attrs keyed by `(thread_id, checkpoint_ns, channel, version)`.

The default bound is 4096 entries and `metadata_cache_max_entries=0` disables
the cache. It is a positive cache only: mutable `heads/latest`, logical
`thread-tombstone`, and pending-write entries are not cached.

Warm-up points:

- successful `put()` inserts the checkpoint attrs and channel blob attrs it
  just wrote;
- `ReadDirPlus(checkpoints/)` parses checkpoint inode attrs and inserts them;
- cache misses still fall back to fsmeta and populate after successful parse.

This is still a saver-layer optimization. It avoids repeated gRPC
`LookupPlus` calls for immutable attrs in one process, while fsmeta remains the
source of truth.

### fsmeta BatchLookupPlus semantic support and saver wiring

The fsmeta metadata layer now exposes a generic `BatchLookupPlus` RPC. The
LangGraph saver uses it for the remaining mutable live-target resolution when a
config does not provide an explicit `checkpoint_id`.

Semantics:

- request shape: one mount plus many `(parent, name)` lookup keys;
- response shape: one result per input key, preserving input order;
- missing dentry: per-item `found=false`;
- dentry exists but inode missing or inconsistent: whole-RPC error, matching
  metadata-corruption behavior rather than a normal application miss;
- `snapshot_version=0`: reads latest and merges the Peras visible overlay;
- `snapshot_version!=0`: reads the exact MVCC version and bypasses latest-only
  overlay/cache behavior;
- one call uses one fsmeta read version and internally performs one batched
  dentry read plus one batched inode read.

Implementation files:

- `pb/fsmeta/fsmeta.proto` and generated Go/Python stubs;
- `fsmeta/plan.go`, `fsmeta/types.go`;
- `fsmeta/exec/runner.go`;
- `fsmeta/server/*`;
- `fsmeta/client/*`;
- `src/langgraph/checkpoint/nokv/fsmeta_client.py` exposes
  `batch_lookup_plus()` as a thin wrapper.

Saver integration:

- `NoKVCheckpointSaver(enable_batch_lookup_plus=True)` is the default;
- latest-target reads batch `thread-tombstone` and `heads/latest` into one RPC;
- explicit `checkpoint_id` configs still only check the tombstone path, because
  the caller has already selected a target checkpoint;
- `UNIMPLEMENTED` or missing-client-method failures disable the feature in the
  current saver instance and fall back to individual `LookupPlus` calls;
- mutable head/tombstone entries are still not cached.

Expected benchmark impact is modest but directionally correct. After the
immutable metadata cache, remaining `lookup_plus` calls are mostly mutable
`heads/latest` and logical tombstone checks. `BatchLookupPlus` groups those
checks into a single metadata-layer read version, but it is not a substitute for
future versioned/CAS head or tombstone protocol work.

## Benchmark instrumentation

`InstrumentedFsMetaClient` now records:

- operation totals, such as `lookup_plus`, `read_dir_plus`, `create`,
  `update_inode`;
- phase splits: `write_phase`, `get_state_phase`, `delta_history_phase`,
  `storage_count_phase`;
- path categories: `checkpoint_attrs`, `channel_blob`, `head`, `tombstone`,
  `write_entry`, `delta_index`, `directory`, `mixed_lookup`, `other`;
- combined `by_phase_category` data.

This is benchmark-only instrumentation and should not be used in production
saver code.

## Latest benchmark runs

### LangGraph official workload after immutable cache

Cluster state for this run:

- Docker Compose NoKV cluster already running.
- `fsmeta-bench` mount registered.
- fsmeta running with Peras visible commit enabled.

Command:

```bash
uv run --extra bench python bench/delta_channel_benchmark.py \
  --savers nokv \
  --scenarios k3_freq_mixed \
  --turn-counts 100 \
  --read-repeats 3 \
  --run-id lg100-immutable-cache-20260512-183013
```

Result artifact:

`artifacts/langgraph-nokv-bench/delta-channel-lg100-immutable-cache-20260512-183013.json`

Summary:

| saver | scenario | turns | write_ms | read_ms | delta_ms | lookup_plus | read_dir_plus | fsmeta_entries | storage_bytes |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| nokv-delta-index | k3_freq_mixed | 100 | 79.44 | 72.22 | 74.68 | 612 | 633 | 2,017 | 290,831 |

Phase split:

| phase | lookup_plus | read_dir_plus | create | update_inode |
| --- | ---: | ---: | ---: | ---: |
| write_phase | 597 | 395 | 2,024 | 299 |
| get_state_phase | 9 | 9 | 0 | 0 |
| delta_history_phase | 6 | 12 | 0 | 0 |
| storage_count_phase | 0 | 217 | 0 | 0 |

Path category split:

| category | lookup_plus | read_dir_plus | create | update_inode |
| --- | ---: | ---: | ---: | ---: |
| channel_blob | 0 | 5 | 700 | 0 |
| checkpoint_attrs | 0 | 106 | 300 | 0 |
| delta_index | 0 | 315 | 400 | 0 |
| directory | 0 | 6 | 223 | 0 |
| head | 405 | 1 | 1 | 299 |
| tombstone | 207 | 0 | 0 | 0 |
| write_entry | 0 | 200 | 400 | 0 |

Historical reference from earlier runs:

| run | write_ms | read_ms | delta_ms | lookup_plus | read_dir_plus |
| --- | ---: | ---: | ---: | ---: | ---: |
| NoKV baseline | 732.14 | 594.11 | 605.44 | 20,074 | 311 |
| NoKV + Peras before directory Stage 1 | 349.96 | 558.86 | 551.55 | 20,074 | 311 |
| NoKV + Peras after directory Stage 1 | 102.47 | 72.49 | 61.77 | 3,424 | 633 |
| NoKV + Peras + create-first | 81.79 | 78.88 | 66.50 | 1,224 | 633 |
| NoKV + Peras + create-first + immutable cache | 79.44 | 72.22 | 74.68 | 612 | 633 |

Interpretation:

- Peras removed a large part of create/update commit cost.
- Directory Stage 1 removed the delta-history parent-chain point-read
  amplification.
- Create-first removed ordinary write and delta-index pre-read amplification.
- Immutable metadata cache removed checkpoint attrs and channel blob attrs
  point reads from the benchmark.
- The remaining `lookup_plus` calls are almost entirely mutable `head` and
  logical tombstone checks. A single-run `delta_ms` increase should be treated
  as latency noise unless repeated runs confirm it; the fsmeta operation shape
  moved in the intended direction.

### LangGraph official workload after BatchLookupPlus saver wiring

Artifact:

`artifacts/langgraph-nokv-bench/delta-channel-lg100-500-batchlookup-20260512-195832.json`

Important caveat: this run was useful for correctness and call-shape validation,
but the Compose container had `--peras-visible-commit=false`, so do not compare
its write latency against the Peras-enabled rows above.

Summary:

| scenario | turns | write_ms | read_ms | delta_ms | lookup_plus | batch_lookup_plus | read_dir_plus | fsmeta_entries | storage_bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| k1_freq50 | 100 | 377.35 | 10.25 | 35.28 | 398 | 109 | 419 | 1,413 | 202,365 |
| k1_freq50 | 500 | 433.01 | 32.00 | 142.45 | 1,990 | 509 | 2,164 | 7,013 | 1,817,428 |
| k3_freq50_uniform | 100 | 546.47 | 16.98 | 59.10 | 398 | 109 | 629 | 2,017 | 409,445 |
| k3_freq50_uniform | 500 | 598.95 | 68.04 | 245.89 | 1,990 | 509 | 3,158 | 10,017 | 4,447,930 |
| k3_freq_mixed | 100 | 563.95 | 65.17 | 67.88 | 404 | 109 | 647 | 2,017 | 290,831 |
| k3_freq_mixed | 500 | 617.53 | 265.45 | 270.12 | 2,004 | 509 | 3,206 | 10,017 | 2,506,850 |
| k8_freq50_uniform | 100 | 1069.90 | 39.44 | 139.94 | 398 | 109 | 1,154 | 3,527 | 927,145 |
| k8_freq50_uniform | 500 | 1146.62 | 161.93 | 480.65 | 1,990 | 509 | 5,643 | 17,527 | 11,024,185 |
| k8_freq_mixed | 100 | 1076.54 | 138.34 | 164.24 | 404 | 109 | 1,189 | 3,527 | 650,481 |
| k8_freq_mixed | 500 | 1221.47 | 514.09 | 622.80 | 2,004 | 509 | 5,723 | 17,527 | 6,512,849 |

Call-shape interpretation:

- `batch_lookup_plus` appears as `mixed_lookup` because it batches
  `thread-tombstone` and `heads/latest`;
- in turn100 rows, 109 batch calls replaced the remaining latest-target grouped
  reads; in turn500 rows, the count is 509;
- delta history phase has `lookup_plus=0` and only five latest-target batch
  calls per row, with the rest of the read work now dominated by `ReadDirPlus`
  over checkpoint and delta-index directories;
- write phase still has `head` `lookup_plus` from mutable head upsert/recovery,
  which is outside this BatchLookupPlus cut.

### fsmeta mixed API benchmark with Peras visible commit

Use the following exact command when running the lower-level fsmeta benchmark:

```bash
NOKV_FSMETA_PROFILE=median \
NOKV_FSMETA_PERAS_VISIBLE_COMMIT=true \
NOKV_FSMETA_PERAS_HOLDER_ID=fsmeta-holder-1 \
NOKV_FSMETA_PERAS_WITNESS_STORES=1,2,3 \
NOKV_FSMETA_PERAS_WITNESS_QUORUM=2 \
NOKV_PERAS_WITNESS=true \
NOKV_FSMETA_COMPOSE_DOWN=1 \
NOKV_FSMETA_COMPOSE_BUILD=0 \
make fsmeta-bench
```

Operational note: a cold start can fail the first workload with transient
`NotFound` after creates if Raft leaders, coordinator grants, mount watches, or
Peras authority/witness state have not settled. For reliable runs, start the
cluster with the same Peras env, make sure `fsmeta-bench` is registered, wait
for the gateway to observe it, and then run the benchmark command above.

Verified run:

- pre-warmed the Compose cluster for about 90 seconds;
- confirmed fsmeta args:
  `--peras-holder-id=fsmeta-holder-1`,
  `--peras-visible-commit=true`,
  `--peras-witness-stores=1,2,3`,
  `--peras-witness-quorum=2`;
- confirmed `BatchLookupPlus(fsmeta-bench, missing-name)` returned a normal
  per-item miss;
- ran `make fsmeta-bench`, which passed and wrote:
  `benchmark/data/fsmeta/results/fsmeta_compose_median_20260512T105337Z.csv`.

## Tests added for the optimization

`tests/test_saver_conformance.py` covers:

- directory-page Stage 1 does not call `_load_checkpoint_attrs()` per ancestor;
- legacy checkpoints missing `seed_body_refs_by_channel` fall back correctly;
- missing target checkpoint returns an empty delta result;
- checkpoint/channel immutable entries use create-first;
- ordinary writes and delta-index entries use create-or-ignore;
- special `WRITES_IDX_MAP` writes keep upsert behavior;
- directory create conflicts can recover through `ReadDirPlus`.
- immutable metadata cache hits after `put()`;
- `ReadDirPlus(checkpoints/)` warms checkpoint attrs cache;
- metadata cache respects its entry bound with LRU eviction.
- latest checkpoint reads batch tombstone and head via `BatchLookupPlus`;
- unavailable `BatchLookupPlus` falls back to single-entry `LookupPlus`;
- delta-history latest-target resolution uses the same batch path.

`tests/test_bench_metrics.py` covers:

- phase/category metrics for checkpoint attrs;
- `read_dir_plus` category tagging for delta index directories.
- `batch_lookup_plus` metrics and mixed path-category tagging.

Go tests cover fsmeta `BatchLookupPlus`:

- ordered per-item results with duplicate inputs and a missing dentry;
- one read version for dentry and inode batched reads;
- snapshot-version retry behavior;
- missing inode returns a whole-operation `ErrNotFound`;
- gRPC service and typed client request/response mapping.

Recent verification performed before this note:

```bash
make proto-check
go test ./fsmeta/... ./pb/fsmeta
go test ./...
uv run --extra dev pytest -q
uv run python -m py_compile \
  src/langgraph/checkpoint/nokv/fsmeta_client.py \
  src/langgraph/checkpoint/nokv/_proto/fsmeta_pb2.py \
  src/langgraph/checkpoint/nokv/_proto/fsmeta_pb2_grpc.py \
  src/langgraph/checkpoint/nokv/saver.py \
  src/langgraph/checkpoint/nokv/_bench_metrics.py \
  bench/delta_channel_benchmark.py
git diff --check
git diff --cached --check
```

## Recommended next steps

1. Re-run LangGraph100 after each step.
   - Track `write_ms`, `read_ms`, `delta_ms`, `lookup_plus_count`,
     `read_dir_plus_count`, `fsmeta_entries`, and `storage_bytes`.
   - Use phase/category splits to verify the intended call sites moved.

2. Decide whether mutable head/tombstone reads are acceptable.
   - Do not cache them blindly; they carry cross-writer/delete semantics.
   - If they become the next bottleneck, consider a versioned/CAS head protocol
     or a carefully bounded negative tombstone strategy.

3. Re-run LangGraph official turn100/turn500 with the Peras benchmark env.
   - The existing BatchLookupPlus 100/500 run had Peras visible commit disabled,
     so it validates shape, not final latency.
   - Use the fsmeta-bench env block above when bringing up the cluster, and
     confirm `--peras-visible-commit=true` plus witness store/quorum args before
     running LangGraph.
