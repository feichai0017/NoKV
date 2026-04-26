# 2026-04-27 Parallel L0 Compaction：让 NumCompactors=4 真的并行

> 状态：已落地。本文是 plan B 的实际形态——bench log 在大数据集下持续
> 输出 `worker=0 level=0` 循环（其余 worker 全部 idle），代码 trace 之后
> 发现 root cause 不是 subcompactions（那个早就实现了），而是 **L0→L0
> fallback 路径上有两个硬编码的并发抑制点**，本 PR 把这两点消除。

---

## 1. 现象

跑 30M / 50M `make bench` 的 NoKV load 阶段，日志里出现：

```
INFO compaction complete worker=0 level=0
INFO write slowdown enabled due to compaction backlog
WARN write stop enabled due to compaction backlog
INFO compaction complete worker=0 level=0
INFO write slowdown enabled due to compaction backlog
WARN write stop enabled due to compaction backlog
...（无限循环）
```

`NumCompactors=4` 配置下只有 worker 0 在干活，worker 1/2/3 整个 cycle
都返回 `false` 然后等下一个 ticker（5s）。L0 backlog 堆积速度 >
单 worker 的消化速度 → write stop。

## 2. 代码 trace 出来的两个抑制点

### Trap #1：硬编码 "compactor 0 only"

```go
// engine/lsm/planner.go (旧版)
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
    if cd.compactorId != 0 {
        // Only allow compactor 0 to avoid L0->L0 contention.
        return false  // ← worker 1/2/3 在 fallback 路径直接被 reject
    }
    ...
}
```

### Trap #2：L0→L0 写 InfRange 状态条目

```go
// PlanForL0ToL0 (旧版)
return Plan{
    ThisLevel: level,
    NextLevel: level,
    TopIDs:    tableIDsFromMeta(out),
    ThisRange: InfRange,  // ← 整个 L0 keyspace 全占
    NextRange: InfRange,
}, true

// fillTablesL0ToL0
lm.compactState.AddRangeWithTables(0, InfRange, cd.plan.TopIDs)
//                                    ^^^^^^^^
// 任何后续 PlanForL0ToLbase 看到 state.Overlaps(0, anything)==true 直接 bail
```

**两个 trap 叠加的死循环**：

1. dataset 大 → L1/L2 都被占 → `PlanForL0ToLbase` 因 state.Overlaps 失败
2. fallback 到 L0→L0：trap #1 让只有 worker 0 能进
3. worker 0 跑 L0→L0，注册 InfRange
4. 在 worker 0 跑期间，worker 1/2/3 试 L0→Lbase → trap #2 让 InfRange 阻塞
5. worker 1/2/3 试 L0→L0 fallback → trap #1 reject
6. worker 1/2/3 整个 cycle 返回 false → ticker 等 5s
7. worker 0 跑完释放，循环回 1

## 3. 修法：State 加 IntraLevel 标志

核心思想：**within-level compaction（L0→L0）只按 table id 占资源，不
注册 key range**。peer worker 的 L0→L0 通过 `state.HasTable` 看到被占
的 table 自动跳过；peer worker 的 L0→Lbase 不会被一个虚假的 InfRange
range 卡住。

### 3.1 `StateEntry.IntraLevel`

```go
type StateEntry struct {
    ...
    TableIDs   []uint64
    IntraLevel bool  // ← new
}
```

### 3.2 `CompareAndAdd` 跳过 range 检查

```go
func (cs *State) CompareAndAdd(_ LevelsLocked, entry StateEntry) bool {
    ...
    if !entry.IntraLevel {
        if thisLevel.overlapsWith(entry.ThisRange) { return false }
        if nextLevel.overlapsWith(entry.NextRange) { return false }
        thisLevel.ranges = append(thisLevel.ranges, entry.ThisRange)
        nextLevel.ranges = append(nextLevel.ranges, entry.NextRange)
    }
    // Intra-level: 只占 tables，不占 range
    thisLevel.delSize += entry.ThisSize
    for _, fid := range entry.TableIDs {
        cs.tables[fid] = struct{}{}
    }
    return true
}
```

### 3.3 `Delete` 对应跳过 range 清理

```go
if !entry.IntraLevel {
    // 原 range 清理逻辑
}
// 都跑 table id 清理
for _, fid := range entry.TableIDs {
    delete(cs.tables, fid)
}
```

### 3.4 `PlanForL0ToL0` 改用 IntraLevel + cap

```go
const l0ToL0MaxTablesPerWorker = 8  // 防止一个 worker 吃光所有 L0

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
        ThisRange:  KeyRange{},  // 不再 InfRange
        NextRange:  KeyRange{},
        IntraLevel: true,
    }, true
}
```

cap=8 是 RocksDB 的 `max_subcompactions` 同款值——4 张是 L0→L0 合并
收益的下限（少了不值得 merge），8 张以上一个 worker 单次合并耗时太长
反而饿死 peer。

### 3.5 `fillTablesL0ToL0` 删除 worker 0 限制

```go
func (lm *levelManager) fillTablesL0ToL0(cd *compactDef) bool {
    // 不再 if cd.compactorId != 0 { return false }
    cd.nextLevel = lm.levels[0]
    ...
    plan, ok := PlanForL0ToL0(...)
    if !ok { return false }
    cd.applyPlan(plan)
    if !lm.resolvePlanLocked(cd) { return false }
    cd.plan.ThisFileSize = math.MaxUint32
    cd.plan.NextFileSize = cd.plan.ThisFileSize
    return lm.compactState.CompareAndAdd(LevelsLocked{}, cd.stateEntry())
    // 不再 AddRangeWithTables(0, InfRange, ...)
}
```

### 3.6 `PlanForL0ToLbase` 跳过被 IntraLevel 占用的 table

L0→L0 现在按 table 占资源不占 range，所以 L0→Lbase 在选 contiguous
overlap group 时必须自己用 `state.HasTable` 把已占 table 跳过：

```go
for _, t := range l0 {
    if state != nil && state.HasTable(t.ID) {
        if len(out) > 0 {
            // 累积中间出现 gap，承诺已经累计的部分
            break
        }
        continue  // 还没开始累积，跳过这个 table 找下一个
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

## 4. addSplits cap 从 5 → max(NumCompactors, 5)

旧 cap=5 hard-coded 在 width 计算里。bump 成 `max(NumCompactors, 5)`
让每次 compaction 内部的 builder fan-out 跟 NumCompactors 同步——
NumCompactors=8 时一次 compaction 也能并行 8 个 builder goroutine
而不是 5。

## 5. 测试

`engine/lsm/parallel_l0_test.go`：

- `TestPlanForL0ToL0AllowsConcurrentWorkers` — 两次连续 PlanForL0ToL0
  调用都成功并各自得到 disjoint table 集
- `TestPlanForL0ToL0DoesNotBlockL0ToLbase` — L0→L0 占了前 8 个 table
  之后，L0→Lbase 仍能拿到剩余 table 的 plan
- `TestStateIntraLevelEntryDeletesCleanly` — IntraLevel 条目的
  CompareAndAdd → Delete round-trip，不破坏 table id 计数

`engine/lsm/parallel_l0_bench_test.go`：

- `BenchmarkPlanForL0ToL0Concurrent` — 1/2/4/8 worker 并发挑 plan
- `BenchmarkPlanForL0ToLbaseUnderL0ToL0Pressure` — L0→L0 已占用时
  L0→Lbase 找 non-conflicting group 的 cost

## 6. 决策日志

- **不动 picker 优先级算法**：worker 仍然按相同的 priority 列表挑
  level，只是 fallback 到 L0→L0 时不再被 worker_id 限制。最小侵入。
- **不引入新 lock**：State 仍然单一 `sync.RWMutex`，IntraLevel 只是改
  CompareAndAdd 内部分支。锁粒度不变。
- **cap 选 8**：和 RocksDB max_subcompactions 一致，这是 L0→L0 合并
  收益和 worker 公平性的甜蜜点。
- **不真改 PlanForL0ToLbase 寻找多 group**：L0→Lbase 自身已经会跳过
  被占 table 找下一个 contiguous overlap group。多 worker 并行 L0→Lbase
  在 Lbase（L1）的 range 必然冲突 → 一个时刻最多只有一个 L0→Lbase
  真正在跑。这是正确的。

## 7. 已知 trade-off

- L0→L0 cap=8 表示一次 L0→L0 最多合并 8 张 SST。如果 L0 SST 数远超
  4×NumCompactors（极端 backlog 时），仍然有 idle worker——但此时 L0
  已经 stop write 了，瓶颈已经转到 compaction speed 本身，加 worker
  数不会缓解（每个 worker 还是要做一次 8-table merge 的活）。
- IntraLevel entry 不写 range，意味着 metric `State.HasRanges()` 不
  会反映 L0→L0 占用。如果将来要在 stats 中区分 "active L0→L0 sub-task
  count"，需要单独 metric。
- PlanForL0ToLbase 看到 IntraLevel 占的 table 时是 break 累积——
  这意味着 L0→Lbase 选的 group 永远在被 L0→L0 占的 table 之**前**或
  之**后**，不能跨越占用区间。常规 workload 没问题；极端的"L0→L0
  正好占在 L0 中间几个 table" 场景下 L0→Lbase 可能 plan 偏小。

## 8. 验证

`go test ./engine/lsm/ -count=1` 全绿，新 3 个 test 通过。
`go test -race ./engine/lsm/ -count=1` race-clean。
新 2 个 bench 跑通。

下一步：在 30M `make bench` 上观测 worker_id 在 compaction 日志里的
分布——预期会从 100% worker=0 变成 worker={0,1,2,3} 均分。
