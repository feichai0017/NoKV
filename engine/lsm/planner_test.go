package lsm

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

func ikey(s string, ts uint64) []byte {
	return kv.InternalKey(kv.CFDefault, []byte(s), ts)
}

func splitUserKey(t *testing.T, internal []byte) []byte {
	t.Helper()
	_, userKey, _, ok := kv.SplitInternalKey(internal)
	require.True(t, ok)
	return userKey
}

func TestKeyRangeOperations(t *testing.T) {
	empty := KeyRange{}
	require.True(t, empty.IsEmpty())

	left := ikey("a", 10)
	right := ikey("c", 5)
	r := KeyRange{Left: left, Right: right}
	require.False(t, r.IsEmpty())
	require.True(t, empty.OverlapsWith(r))
	require.False(t, r.OverlapsWith(KeyRange{}))

	r2 := KeyRange{Left: ikey("b", 9), Right: ikey("d", 1)}
	require.True(t, r.OverlapsWith(r2))

	r3 := KeyRange{Left: ikey("d", 9), Right: ikey("e", 1)}
	require.False(t, r.OverlapsWith(r3))

	copyRange := KeyRange{Left: append([]byte(nil), left...), Right: append([]byte(nil), right...)}
	require.True(t, r.Equals(copyRange))

	var ext KeyRange
	ext.Extend(r)
	require.True(t, ext.Equals(r))
	ext.Extend(r3)
	require.True(t, ext.OverlapsWith(r3))
}

func TestPlanStateEntry(t *testing.T) {
	plan := Plan{
		ThisLevel: 0,
		NextLevel: 1,
		TopIDs:    []uint64{1},
		BotIDs:    []uint64{2},
		ThisRange: KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
	}
	entry := plan.StateEntry(64)
	require.ElementsMatch(t, []uint64{1, 2}, entry.TableIDs)

	empty := Plan{ThisLevel: 1, NextLevel: 1}
	entry = empty.StateEntry(0)
	require.Empty(t, entry.TableIDs)
}

func TestPlanBuilderSelections(t *testing.T) {
	t1 := TableMeta{ID: 1, MinKey: ikey("a", 10), MaxKey: ikey("b", 1), MaxVersion: 5, Size: 8 << 20}
	t2 := TableMeta{ID: 2, MinKey: ikey("b", 10), MaxKey: ikey("c", 1), MaxVersion: 1, Size: 6 << 20}
	t3 := TableMeta{ID: 3, MinKey: ikey("d", 10), MaxKey: ikey("e", 1), MaxVersion: 2, Size: 4 << 20}
	tables := []TableMeta{t1, t2, t3}

	kr := RangeForTables([]TableMeta{t1, t2})
	require.True(t, bytes.HasPrefix(splitUserKey(t, kr.Left), []byte("a")))
	require.True(t, bytes.HasPrefix(splitUserKey(t, kr.Right), []byte("c")))

	left, right := OverlappingTables([]TableMeta{t1, t2, t3}, RangeForTables([]TableMeta{t2}))
	require.Equal(t, 0, left)
	require.Equal(t, 2, right)

	plan, ok := PlanForStagingFallback(2, []TableMeta{t1})
	require.True(t, ok)
	require.Equal(t, 2, plan.ThisLevel)

	plan, ok = PlanForRegular(1, tables, 2, []TableMeta{t3}, nil)
	require.True(t, ok)
	require.Equal(t, uint64(2), plan.TopIDs[0])

	old := time.Now().Add(-2 * time.Hour)
	recent := time.Now().Add(-5 * time.Second)
	t4 := TableMeta{ID: 4, MinKey: ikey("f", 10), MaxKey: ikey("g", 1), StaleSize: 12 << 20, CreatedAt: old, Size: 8 << 20}
	t5 := TableMeta{ID: 5, MinKey: ikey("h", 10), MaxKey: ikey("i", 1), StaleSize: 1, CreatedAt: old, Size: 8 << 20}
	plan, ok = PlanForMaxLevel(6, []TableMeta{t4, t5}, 20<<20, nil, time.Now(), 0)
	require.True(t, ok)
	require.Equal(t, uint64(4), plan.TopIDs[0])

	ttlCandidate := TableMeta{ID: 6, MinKey: ikey("j", 10), MaxKey: ikey("k", 1), StaleSize: 1, CreatedAt: old, Size: 8 << 20}
	plan, ok = PlanForMaxLevel(6, []TableMeta{ttlCandidate}, 20<<20, nil, time.Now(), time.Hour)
	require.True(t, ok)
	require.Equal(t, uint64(6), plan.TopIDs[0])
	require.Equal(t, "ttl", plan.StatsTag)

	recentTTL := TableMeta{ID: 13, MinKey: ikey("l", 10), MaxKey: ikey("m", 1), StaleSize: 1, CreatedAt: recent, Size: 8 << 20}
	_, ok = PlanForMaxLevel(6, []TableMeta{recentTTL}, 20<<20, nil, time.Now(), time.Hour)
	require.False(t, ok)

	shard := []TableMeta{
		{ID: 7, MinKey: ikey("a", 9), MaxKey: ikey("b", 1), Size: 4 << 20},
		{ID: 8, MinKey: ikey("b", 9), MaxKey: ikey("c", 1), Size: 4 << 20},
		{ID: 9, MinKey: ikey("c", 9), MaxKey: ikey("d", 1), Size: 4 << 20},
	}
	plan, ok = PlanForStagingShard(0, shard, 1, []TableMeta{}, 4<<20, 1, nil)
	require.True(t, ok)
	require.Len(t, plan.TopIDs, 3)

	l0 := []TableMeta{
		{ID: 10, MinKey: ikey("a", 9), MaxKey: ikey("b", 1)},
		{ID: 11, MinKey: ikey("b", 9), MaxKey: ikey("c", 1)},
		{ID: 12, MinKey: ikey("d", 9), MaxKey: ikey("e", 1)},
	}
	plan, ok = PlanForL0ToLbase(l0, 1, []TableMeta{t3}, nil)
	require.True(t, ok)
	require.Equal(t, 2, len(plan.TopIDs))

	l0 = []TableMeta{
		{ID: 20, MinKey: ikey("a", 9), MaxKey: ikey("b", 1), Size: 5 << 20, CreatedAt: old},
		{ID: 21, MinKey: ikey("b", 9), MaxKey: ikey("c", 1), Size: 5 << 20, CreatedAt: old},
		{ID: 22, MinKey: ikey("c", 9), MaxKey: ikey("d", 1), Size: 5 << 20, CreatedAt: old},
		{ID: 23, MinKey: ikey("d", 9), MaxKey: ikey("e", 1), Size: 5 << 20, CreatedAt: old},
		{ID: 24, MinKey: ikey("e", 9), MaxKey: ikey("f", 1), Size: 200 << 20, CreatedAt: old},
		{ID: 25, MinKey: ikey("f", 9), MaxKey: ikey("g", 1), Size: 5 << 20, CreatedAt: recent},
	}
	plan, ok = PlanForL0ToL0(0, l0, 90<<20, NewState(1), time.Now())
	require.True(t, ok)
	require.Equal(t, 4, len(plan.TopIDs))
}

func TestTableHelpers(t *testing.T) {
	require.Nil(t, tableIDsFromMeta(nil))

	t1 := TableMeta{ID: 1, MinKey: ikey("a", 10), MaxKey: ikey("b", 1), Size: 4}
	t2 := TableMeta{ID: 2, MinKey: ikey("b", 10), MaxKey: ikey("c", 1), Size: 4}
	t3 := TableMeta{ID: 3, MinKey: ikey("c", 10), MaxKey: ikey("d", 1), Size: 4}
	tables := []TableMeta{t1, t2, t3}

	bot := collectBotTables(t1, tables, 10)
	require.Len(t, bot, 1)

	bot = collectBotTables(TableMeta{ID: 99, MinKey: ikey("z", 1)}, tables, 10)
	require.Nil(t, bot)
}

// TestPlanForL0ToL0AllowsConcurrentWorkers verifies that two consecutive
// PlanForL0ToL0 calls (simulating two compactor workers) each receive a
// disjoint slice of L0 tables and that the second call is NOT blocked by
// the first claiming an InfRange entry on level 0. With the IntraLevel
// flag the state machine claims by table ID only, so peer workers see
// the right tables filtered out via state.HasTable but no range overlap
// blocks them.
func TestPlanForL0ToL0AllowsConcurrentWorkers(t *testing.T) {
	state := NewState(8)
	tables := makeL0PlannerTestTables(t, 16)

	// Worker 0 picks first.
	plan0, ok0 := PlanForL0ToL0(0, tables, 0, state, time.Time{})
	require.True(t, ok0)
	require.True(t, plan0.IntraLevel)
	require.Len(t, plan0.TopIDs, l0ToL0MaxTablesPerWorker)
	require.True(t, state.CompareAndAdd(LevelsLocked{}, plan0.StateEntry(0)))

	// Worker 1 picks second; should get the remaining tables.
	plan1, ok1 := PlanForL0ToL0(0, tables, 0, state, time.Time{})
	require.True(t, ok1, "second worker must find a non-conflicting plan")
	require.True(t, plan1.IntraLevel)
	require.Len(t, plan1.TopIDs, l0ToL0MaxTablesPerWorker)
	for _, fid := range plan0.TopIDs {
		for _, fid2 := range plan1.TopIDs {
			require.NotEqual(t, fid, fid2, "plans must hold disjoint table sets")
		}
	}
	require.True(t, state.CompareAndAdd(LevelsLocked{}, plan1.StateEntry(0)))

	// Worker 2 finds nothing usable (only 16 tables / cap=8 each = 2 plans max).
	_, ok2 := PlanForL0ToL0(0, tables, 0, state, time.Time{})
	require.False(t, ok2)
}

// TestPlanForL0ToL0DoesNotBlockL0ToLbase verifies the core PR motivation:
// an in-flight L0→L0 compaction must NOT prevent a peer worker from
// running L0→Lbase on a different (non-overlapping) range. Pre-fix,
// L0→L0 used InfRange and any subsequent L0→Lbase hit
// `state.Overlaps(0, anything)`. With IntraLevel the L0 entry only
// claims tables, so L0→Lbase can claim a fresh range slice.
func TestPlanForL0ToL0DoesNotBlockL0ToLbase(t *testing.T) {
	state := NewState(8)
	l0 := makeL0PlannerTestTables(t, 12)
	lbase := []TableMeta{
		{ID: 100, MinKey: ikey("a", 10), MaxKey: ikey("z", 1), Size: 32 << 20},
	}

	planA, okA := PlanForL0ToL0(0, l0, 0, state, time.Time{})
	require.True(t, okA)
	require.True(t, planA.IntraLevel)
	require.True(t, state.CompareAndAdd(LevelsLocked{}, planA.StateEntry(0)))

	planB, okB := PlanForL0ToLbase(l0, 1, lbase, state)
	require.True(t, okB, "L0→Lbase must not be blocked by an in-flight L0→L0")
	require.False(t, planB.IntraLevel)
	planAIDs := make(map[uint64]struct{}, len(planA.TopIDs))
	for _, fid := range planA.TopIDs {
		planAIDs[fid] = struct{}{}
	}
	for _, fid := range planB.TopIDs {
		_, claimed := planAIDs[fid]
		require.False(t, claimed, "L0→Lbase must not pick tables already claimed by L0→L0")
	}
}

// makeL0PlannerTestTables fabricates N L0 TableMeta entries with overlapping
// keyranges (the realistic L0 case for random-write workloads).
func makeL0PlannerTestTables(t *testing.T, n int) []TableMeta {
	t.Helper()
	out := make([]TableMeta, n)
	for i := range out {
		out[i] = TableMeta{
			ID:        uint64(i + 1),
			MinKey:    ikey(formatL0Key(i*2), 10),
			MaxKey:    ikey(formatL0Key(i*2+10), 1),
			Size:      8 << 20,
			CreatedAt: time.Time{}, // skip "younger than 10s" filter
		}
	}
	return out
}

func formatL0Key(i int) string {
	return fmt.Sprintf("k%05d", i)
}
