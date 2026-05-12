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
- `.../delta_channels/<channel>/dw~2~<checkpoint_id_hex>~<task_id>~<idx>`
  stores derived `DeltaWriteEntryAttrs` for ordered, channel-first delta reads.
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

### Stage 2: range-aware materialized delta channel index

`put_writes()` still writes canonical `writes/<checkpoint_id>/...` entries. When
`enable_delta_index` is true, it also writes per-channel index entries under
`delta_channels/<channel>/...`.

The delta index name is ordered by checkpoint id:

`dw~2~<checkpoint_id_utf8_hex>~<encoded_task_id>~<idx>`

The hex checkpoint-id component preserves bytewise checkpoint-id ordering under
normal fsmeta directory sorting. `_load_delta_index_writes()` now computes the
eligible checkpoint window from Stage 1 and uses `ReadDirPlus(start_after,
limit)` with a lower/upper name bound so mixed-frequency workloads do not need
to list the entire `delta_channels/<channel>/` directory and then filter it in
Python.

Safety behavior:

- the range path is used only when the current saver process knows the ordered
  index covers every eligible checkpoint/channel pair;
- old `dw~...` entries and mixed upgraded data fall back to the legacy full
  directory scan;
- canonical `writes/<checkpoint_id>/...` remains the source of truth when the
  derived index is absent or untrusted.

The saver still sorts materialized writes by ancestor rank, task id, and write
idx so the result is oldest-to-newest for replay.

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

The benchmark also wraps payload handling:

- NoKV records external body-store `put_typed` and `get_typed` time and bytes.
  For NoKV, payload hydrate is the combination of body-store `get_typed` and
  serde `loads_typed`.
- Both NoKV and PostgreSQL record serde `dumps_typed` and `loads_typed`. For
  PostgreSQL there is no separate body-store metric because the payload bytes
  come from SQL rows, but `loads_typed` is still real payload hydrate and must
  be counted in comparisons.
- Saver-level counters record pending write counts for delta history, including
  seed counts and replay write counts.

## Latest benchmark runs

### Range-aware delta index AB runs with Peras visible commit

Cluster state for this run:

- Docker Compose NoKV cluster already running.
- `fsmeta-bench` mount registered.
- fsmeta running with Peras visible commit enabled and witness quorum enabled.
- local PostgreSQL used the URI
  `postgres://localhost:5432/postgres?sslmode=disable`.

Command:

```bash
uv run --extra bench python bench/delta_channel_benchmark.py \
  --savers nokv,postgres \
  --scenarios <scenario> \
  --turn-counts 500 \
  --read-repeats 5 \
  --run-id <run-id> \
  --postgres-uri 'postgres://localhost:5432/postgres?sslmode=disable'
```

Artifacts:

- `artifacts/langgraph-nokv-bench/delta-channel-ab-k3mixed500-range-20260512-120035.json`
- `artifacts/langgraph-nokv-bench/delta-channel-ab-k8mixed500-range-20260512-120808.json`

Summary:

| scenario | saver | write_ms | read_ms | delta_ms | storage | peak_mem |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| k3_freq_mixed | nokv-delta-index | 152.24 | 185.38 | 196.67 | 2.5 MB | 6.6 MB |
| k3_freq_mixed | postgres | 25.25 | 130.09 | 167.61 | 3.0 MB | 10.4 MB |
| k8_freq_mixed | nokv-delta-index | 374.53 | 469.00 | 544.71 | 6.5 MB | 17.4 MB |
| k8_freq_mixed | postgres | 63.43 | 303.75 | 437.63 | 5.4 MB | 25.0 MB |

Interpretation:

- The range-aware index improved the mixed read shape versus the old full
  channel-directory scan, but NoKV is still slower than PostgreSQL on these
  local AB runs: k3 read is 1.43x slower, k3 delta history is 1.17x slower; k8
  read is 1.54x slower, k8 delta history is 1.24x slower.
- The largest remaining NoKV write cost is metadata mutation and read-replay
  during writes, not payload serialization alone.
- PostgreSQL still pays payload hydrate through `serde.loads_typed`; it is not
  free just because there is no external body store.

Payload hydrate split:

| scenario | saver | phase | body get count | body get total_ms | serde load count | serde load total_ms |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| k3_freq_mixed | NoKV | get_state | 3,015 | 201.46 | 3,015 | 306.42 |
| k3_freq_mixed | NoKV | delta_history | 3,260 | 199.77 | 3,260 | 276.45 |
| k3_freq_mixed | PostgreSQL | get_state | n/a | n/a | 3,010 | 284.76 |
| k3_freq_mixed | PostgreSQL | delta_history | n/a | n/a | 3,265 | 365.32 |
| k8_freq_mixed | NoKV | get_state | 8,030 | 503.19 | 8,030 | 773.15 |
| k8_freq_mixed | NoKV | delta_history | 11,395 | 697.96 | 11,395 | 740.06 |
| k8_freq_mixed | PostgreSQL | get_state | n/a | n/a | 8,025 | 775.00 |
| k8_freq_mixed | PostgreSQL | delta_history | n/a | n/a | 11,415 | 1,119.47 |

NoKV fsmeta range-read split:

| scenario | phase | delta_index ReadDirPlus count | total_ms | p95_ms |
| --- | --- | ---: | ---: | ---: |
| k3_freq_mixed | get_state | 10 | 197.63 | 83.79 |
| k3_freq_mixed | delta_history | 15 | 257.52 | 18.86 |
| k8_freq_mixed | get_state | 20 | 462.08 | 87.01 |
| k8_freq_mixed | delta_history | 40 | 602.67 | 28.63 |

Pending replay counts:

| scenario | delta_history repeats | avg seeds | avg pending writes |
| --- | ---: | ---: | ---: |
| k3_freq_mixed | 5 | 2 | 650 |
| k8_freq_mixed | 5 | 4 | 2,275 |

PostgreSQL payload hydrate note:

The PostgreSQL saver stores serialized payloads in table rows, so there is no
separate body-store `get_typed` step in the benchmark. However, every returned
checkpoint/write/channel payload is still deserialized through
`serde.loads_typed`. In the k8 mixed 500 run, PostgreSQL spent 775.00 ms in
`get_state` serde loads and 1,119.47 ms in `delta_history` serde loads across
the five read repeats. Those numbers are the PostgreSQL payload hydrate cost
and should be included in future comparisons.

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
- ordered delta-index names preserve checkpoint ordering for range scans;
- delta history uses the ordered delta index to scan only the replay window
  when the saver knows the index coverage is complete.

`tests/test_bench_metrics.py` covers:

- phase/category metrics for checkpoint attrs;
- `read_dir_plus` category tagging for delta index directories.
- `batch_lookup_plus` metrics and mixed path-category tagging.
- body-store and serde payload instrumentation.

Go tests cover fsmeta `BatchLookupPlus`:

- ordered per-item results with duplicate inputs and a missing dentry;
- one read version for dentry and inode batched reads;
- snapshot-version retry behavior;
- missing inode returns a whole-operation `ErrNotFound`;
- gRPC service and typed client request/response mapping.

Recent verification for the current Python integration cut:

```bash
uv run pytest -q tests/test_layout.py tests/test_saver_conformance.py
uv run --extra dev pytest -q
uv run python -m py_compile \
  src/langgraph/checkpoint/nokv/layout.py \
  src/langgraph/checkpoint/nokv/saver.py \
  src/langgraph/checkpoint/nokv/_bench_metrics.py \
  bench/delta_channel_benchmark.py
git diff --check
```

## Recommended next steps

1. Treat PostgreSQL `serde.loads_typed` as payload hydrate in every AB read.
   It is not an external body-store fetch, but it is still deserializing the
   serialized checkpoint/write/channel payload returned by PostgreSQL.

2. Decide whether mutable head/tombstone reads are acceptable.
   - Do not cache them blindly; they carry cross-writer/delete semantics.
   - If they become the next bottleneck, consider a versioned/CAS head protocol
     or a carefully bounded negative tombstone strategy.

3. Investigate NoKV write amplification under mixed high-K.
   - The k8 mixed run still spends substantial time in write-phase body reads,
     delta-index range reads, metadata creates, and head updates.
   - Keep the next cut small and measure it against k3/k8 mixed turn500 AB
     before broadening the scenario set.
