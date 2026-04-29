# 2026-04-27 Parallel L0 Compaction: making `NumCompactors=4` actually parallel

> Status: shipped. This is the production form of plan B. The bench log on large datasets kept emitting `worker=0 level=0` in a loop (every other worker idle). Code-tracing showed the root cause was **not** subcompactions (those were already implemented) — it was **two hardcoded concurrency-suppression points on the L0→L0 fallback path**. This PR removes both of them.

---

## 1. Symptom

Running 30M / 50M `make bench` against the NoKV load phase, the log showed:

```
INFO compaction complete worker=0 level=0
INFO write slowdown enabled due to compaction backlog
WARN write stop enabled due to compaction backlog
INFO compaction complete worker=0 level=0
INFO write slowdown enabled due to compaction backlog
WARN write stop enabled due to compaction backlog
...(infinite loop)
```

With `NumCompactors=4`, only worker 0 was working; workers 1/2/3 returned `false` for the entire cycle and waited for the next ticker (5s). L0 backlog accumulation rate > one worker's drain rate → write stop.

## 2. The two suppression points exposed by code tracing

### Trap #1: hardcoded "compactor 0 only"

```go
// engine/lsm/planner.go (old)
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
    if cd.compactorId != 0 {
        // Only allow compactor 0 to avoid L0->L0 contention.
        return false  // ← workers 1/2/3 are rejected outright on the fallback path
    }
    ...
}
```

### Trap #2: L0→L0 writes an InfRange state entry

```go
// PlanForL0ToL0 (old)
return Plan{
    ThisLevel: level,
    NextLevel: level,
    TopIDs:    tableIDsFromMeta(out),
    ThisRange: InfRange,  // ← grabs the entire L0 keyspace
    NextRange: InfRange,
}, true

// fillTablesL0ToL0
lm.compactState.AddRangeWithTables(0, InfRange, cd.plan.TopIDs)
//                                    ^^^^^^^^
// any subsequent PlanForL0ToLbase sees state.Overlaps(0, anything)==true
// and bails immediately
```

**The deadly cycle from these two combined**:

1. Big dataset → L1/L2 are occupied → `PlanForL0ToLbase` fails on `state.Overlaps`.
2. Falls back to L0→L0: trap #1 lets only worker 0 in.
3. Worker 0 runs L0→L0 and registers InfRange.
4. While worker 0 is running, workers 1/2/3 try L0→Lbase → trap #2 makes the InfRange block them.
5. Workers 1/2/3 try L0→L0 fallback → trap #1 rejects them.
6. Workers 1/2/3 return false the entire cycle → ticker waits 5s.
7. Worker 0 finishes and releases, loop back to step 1.

## 3. Fix: add an `IntraLevel` flag to State

Core idea: **within-level compaction (L0→L0) only reserves resources by table id, not by key range**. Peer workers' L0→L0 see the held tables via `state.HasTable` and skip them automatically; peer workers' L0→Lbase isn't blocked by a fake InfRange range.

### 3.1 `StateEntry.IntraLevel`

```go
type StateEntry struct {
    ...
    TableIDs   []uint64
    IntraLevel bool  // ← new
}
```

### 3.2 `CompareAndAdd` skips the range check

```go
func (cs *State) CompareAndAdd(_ LevelsLocked, entry StateEntry) bool {
    ...
    if !entry.IntraLevel {
        if thisLevel.overlapsWith(entry.ThisRange) { return false }
        if nextLevel.overlapsWith(entry.NextRange) { return false }
        thisLevel.ranges = append(thisLevel.ranges, entry.ThisRange)
        nextLevel.ranges = append(nextLevel.ranges, entry.NextRange)
    }
    // Intra-level: only reserve tables, not ranges.
    thisLevel.delSize += entry.ThisSize
    for _, fid := range entry.TableIDs {
        cs.tables[fid] = struct{}{}
    }
    return true
}
```

### 3.3 `Delete` correspondingly skips range cleanup

```go
if !entry.IntraLevel {
    // original range-cleanup logic
}
// Both branches still clean up table ids.
for _, fid := range entry.TableIDs {
    delete(cs.tables, fid)
}
```

### 3.4 `PlanForL0ToL0` switches to IntraLevel + cap

```go
const l0ToL0MaxTablesPerWorker = 8  // prevent one worker from eating all L0 tables

func PlanForL0ToL0(...) (Plan, bool) {
    var out []TableMeta
    for _, t := range tables {
        if state != nil && state.HasTable(t.ID) { continue }
        out = append(out, t)
        if len(out) >= l0ToL0MaxTablesPerWorker { break }
    }
    if len(out) < 4 { return Plan{}, false }
    return Plan{
        ThisLevel:  level,
        NextLevel:  level,
        TopIDs:     tableIDsFromMeta(out),
        ThisRange:  KeyRange{},  // no longer InfRange
        NextRange:  KeyRange{},
        IntraLevel: true,
    }, true
}
```

cap=8 matches RocksDB's `max_subcompactions`. 4 tables is the lower bound where L0→L0 merge is worth it (fewer is not worth merging); above 8, one worker's single merge runs long and starves peers.

### 3.5 `fillTablesL0ToL0` removes the worker-0 restriction

```go
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
    // No longer: if cd.compactorId != 0 { return false }
    cd.nextLevel = lm.levels[0]
    ...
    plan, ok := PlanForL0ToL0(...)
    if !ok { return false }
    cd.applyPlan(plan)
    if !lm.resolvePlanLocked(cd) { return false }
    cd.plan.ThisFileSize = math.MaxUint32
    cd.plan.NextFileSize = cd.plan.ThisFileSize
    return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
    // No longer: AddRangeWithTables(0, InfRange, ...)
}
```

### 3.6 `PlanForL0ToLbase` skips IntraLevel-occupied tables

L0→L0 now holds resources by table, not by range, so when L0→Lbase picks a contiguous overlap group it must use `state.HasTable` to skip held tables itself:

```go
for _, t := range l0 {
    if state != nil && state.HasTable(t.ID) {
        if len(out) > 0 {
            // gap appeared mid-accumulation; commit what we have
            break
        }
        continue  // not yet accumulating, skip this table and find the next
    }
    dkr := RangeForTables([]TableMeta{t})
    if len(out) == 0 || kr.OverlapsWith(dkr) {
        out = append(out, t)
        kr.Extend(dkr)
    } else {
        break
    }
}
```

## 4. addSplits cap from 5 → `max(NumCompactors, 5)`

The old cap=5 was hardcoded into the width calculation. Bumping to `max(NumCompactors, 5)` lets each compaction's internal builder fan-out match `NumCompactors` — at `NumCompactors=8`, one compaction can run 8 builder goroutines instead of 5.

## 5. Tests

`engine/lsm/parallel_l0_test.go`:

- `TestPlanForL0ToL0AllowsConcurrentWorkers` — two consecutive PlanForL0ToL0 calls both succeed with disjoint table sets.
- `TestPlanForL0ToL0DoesNotBlockL0ToLbase` — after L0→L0 reserves the first 8 tables, L0→Lbase still gets a plan from the rest.
- `TestStateIntraLevelEntryDeletesCleanly` — round-trip CompareAndAdd → Delete on an IntraLevel entry, without breaking table-id accounting.

`engine/lsm/parallel_l0_bench_test.go`:

- `BenchmarkPlanForL0ToL0Concurrent` — concurrent plan picking at 1/2/4/8 workers.
- `BenchmarkPlanForL0ToLbaseUnderL0ToL0Pressure` — L0→Lbase cost of finding a non-conflicting group while L0→L0 is occupying tables.

## 6. Decision log

- **Don't touch the picker priority algorithm**: workers still pick levels by the same priority list — they're just no longer worker-id-restricted on the L0→L0 fallback. Minimal intrusion.
- **No new locks**: State still has a single `sync.RWMutex`; IntraLevel is only an internal branch in `CompareAndAdd`. Lock granularity unchanged.
- **cap=8**: matches RocksDB `max_subcompactions`, the sweet spot between L0→L0 merge benefit and worker fairness.
- **Don't actually have `PlanForL0ToLbase` find multiple groups**: L0→Lbase already skips held tables to find the next contiguous overlap group. Multiple parallel L0→Lbase necessarily collide on Lbase (L1) ranges — at most one L0→Lbase truly runs at a time. That's correct.

## 7. Known trade-offs

- L0→L0 cap=8 means at most 8 SSTs are merged in one L0→L0. If L0 SST count is far above 4×NumCompactors (extreme backlog), idle workers can still appear — but at that point L0 has already stop-written; the bottleneck has shifted to compaction speed, and adding workers won't help (each worker still has to do one 8-table merge of work).
- IntraLevel entries don't write a range, meaning the metric `State.HasRanges()` won't reflect L0→L0 occupancy. If we later want to differentiate "active L0→L0 sub-task count" in stats, we'll need a separate metric.
- When `PlanForL0ToLbase` sees a table reserved by IntraLevel, it breaks accumulation. So the L0→Lbase group is always **before** or **after** the held tables, never spanning across them. This is fine for normal workloads; in the extreme "L0→L0 happens to hold the middle tables of L0", L0→Lbase may plan a smaller group than ideal.

## 8. Validation

`go test ./engine/lsm/ -count=1` all green, the 3 new tests pass.
`go test -race ./engine/lsm/ -count=1` race-clean.
The 2 new benches run.

Next step: observe the worker_id distribution in compaction logs on 30M `make bench`. Expectation is the 100% worker=0 distribution flattens to worker={0,1,2,3} evenly.
