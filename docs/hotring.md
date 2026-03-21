# HotRing

`hotring` is NoKV's optional internal hotspot detector. The package lives in [`hotring/`](../hotring) and is no longer part of the default read path, LSM compaction planning, or value-log bucket routing.

## Current Role

The production role is deliberately narrow:

- detect write-hot keys when `Options.HotRingEnabled` is enabled
- enforce `Options.WriteHotKeyLimit` via `HotRing.TouchAndClamp`
- publish write-hot snapshots through `StatsSnapshot.Hot.WriteKeys` and `StatsSnapshot.Hot.WriteRing`

If `HotRingEnabled=false`, the DB runs without HotRing tracking on the hot path.

## What HotRing Does Not Control Anymore

These integrations were intentionally removed from the default engine path:

- read-path hot tracking and asynchronous read prefetch
- LSM compaction scoring based on hot-key overlap
- value-log hot/cold bucket routing
- hot-write batch enlargement heuristics

The default value-log configuration keeps ordinary hash bucketization enabled through `ValueLogBucketCount`, but bucket selection no longer depends on HotRing.

## Data Structure

HotRing remains a concurrent in-memory frequency tracker with:

- sharded hash buckets
- lock-free bucket lists for lookup/insert
- atomic counters per node
- optional sliding-window and rotation support

These capabilities are still useful for optional write throttling and operational diagnostics, even though they are no longer wired into unrelated subsystems.

## Relevant Options

| Option | Meaning |
| --- | --- |
| `HotRingEnabled` | Master switch for write-hot tracking. |
| `WriteHotKeyLimit` | Reject writes once a single key exceeds the configured threshold. |
| `HotRingBits` | Bucket count (`2^bits`) for the tracker. |
| `HotRingTopK` | Number of hot keys exported in stats. |
| `HotRingRotationInterval` | Optional dual-ring rotation. |
| `HotRingWindowSlots` / `HotRingWindowSlotDuration` | Optional sliding-window tracking. |
| `HotRingNodeCap` / `HotRingNodeSampleBits` | Bound in-memory growth. |

## Write Throttling

When both `HotRingEnabled` and `WriteHotKeyLimit > 0` are set, NoKV records write frequency by `CF + UserKey` and returns `utils.ErrHotKeyWriteThrottle` once the limit is reached.

This path exists to protect the engine from pathological skew. It is intentionally independent from cache warming, compaction, and value-log routing.

## Stats

HotRing contributes only write-side observability:

- `StatsSnapshot.Hot.WriteKeys`
- `StatsSnapshot.Hot.WriteRing`
- `StatsSnapshot.Write.HotKeyLimited`

The CLI surfaces the same data under `nokv stats`.

## Design Position

HotRing should be understood as:

- an optional internal detector
- an optional write throttling tool
- not a required performance feature
- not a default read-path or value-log optimization

That narrower scope keeps the core engine path simpler and makes HotRing easier to reason about.
