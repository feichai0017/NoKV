# Cache Optimization Plan (NoKV single-node engine)

## Goals
- Reduce double caching and unnecessary copies on SST reads.
- Make long scans/compaction bypass block cache by default.
- Trim OS page cache after flush/vlog writes to avoid RSS bloat.
- Add observability for bypass/zero-copy/madvise to drive adaptive policies.
- Optionally support a VMcache-style “index-only” block cache (store offsets, not data).

## Current state (baseline)
- Iterators support `BypassBlockCache`, `ZeroCopy`, `AccessPattern`; compaction iterators use sequential access and bypass cache is available but not auto-enabled for user scans.
- Block cache only covers L0/L1; deeper levels read via mmap.
- mmap advise is hooked (Sequential/Random/WillNeed/DontNeed). Value log uses `Advise(DONTNEED)` after writes/rotate.
- Zero-copy is supported with generation checks; default point lookups copy.
- No SST flush DONTNEED; no metrics for bypass/zero-copy/madvise.

## Step-by-step plan

### Step 1: Flush DONTNEED for SST
- After `tableBuilder.flush` writes and fsyncs the SST, call `Advise(DONTNEED)` on the mmap region to drop hot pages from OS cache (data now in block cache if needed).
- Keep optional/guarded to avoid hurting workloads that immediately reread flushed data.

### Step 2: Auto-bypass for long scans/compaction
- Default `BypassBlockCache=true` for:
  - Compaction iterators (already sequential).
  - User iterators with large `PrefetchBlocks` or explicit “scan” mode.
- Keep point lookups/random iterators using cache by default; honor explicit user overrides.

### Step 3: Metrics & observability
- Add expvar counters:
  - BypassBlockCache hits/uses.
  - ZeroCopy uses/fallbacks.
  - Madvise calls per pattern and failures.
- Optionally log page-fault/readahead stats where available.

### Step 4: Adaptive policy
- Use metrics to auto-switch:
  - Large/long-running iterator → bypass + sequential advise.
  - Random/point → keep cache + random advise.
  - If cache hit rate drops or page-faults spike, widen prefetch or fallback to cache.
- Keep manual overrides; adaptive as default heuristic.

### Step 5 (optional): VMcache-style index-only block cache
- Add a mode where block cache stores `{fid, offset, len, generation}` instead of data.
- On hit, read from mmap (zero-copy) after generation check; fallback to copy on mismatch.
- Benefits: avoid user-space data duplication, rely on OS page cache; keep current data-cache mode as default.

## Validation
- `go test ./...` for correctness.
- YCSB (sync=true/false, various conc/workloads) compare ops/s and p99 vs baseline.
- Collect pprof and new counters to confirm reduced double caching/page-fault patterns.
