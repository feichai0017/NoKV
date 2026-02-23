package compact

import (
	"bytes"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/kv"
	"github.com/stretchr/testify/require"
)

func ikey(s string, ts uint64) []byte {
	return kv.KeyWithTs([]byte(s), ts)
}

func TestIngestModeFlags(t *testing.T) {
	require.False(t, IngestNone.UsesIngest())
	require.True(t, IngestDrain.UsesIngest())
	require.True(t, IngestKeep.UsesIngest())
	require.True(t, IngestKeep.KeepsIngest())
	require.False(t, IngestDrain.KeepsIngest())
}

func TestIngestPicker(t *testing.T) {
	shards := []IngestShardView{
		{Index: 1, SizeBytes: 10},
		{Index: 2, SizeBytes: 30},
		{Index: 3, SizeBytes: 20, MaxAgeSec: 120, ValueDensity: 0.5},
	}
	order := PickShardOrder(IngestPickInput{Shards: shards})
	require.Equal(t, []int{2, 3, 1}, order)

	pick := PickShardByBacklog(IngestPickInput{Shards: shards})
	require.Equal(t, 3, pick)

	require.Equal(t, -1, PickShardByBacklog(IngestPickInput{}))
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

func TestStateCompareAndDelete(t *testing.T) {
	state := NewState(2)
	entry := StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.True(t, state.CompareAndAdd(LevelsLocked{}, entry))
	require.True(t, state.HasRanges())
	require.True(t, state.HasTable(1))
	require.Equal(t, int64(128), state.DelSize(0))
	require.True(t, state.Overlaps(0, entry.ThisRange))

	overlap := entry
	overlap.ThisRange = KeyRange{Left: ikey("a", 9), Right: ikey("b", 0)}
	require.False(t, state.CompareAndAdd(LevelsLocked{}, overlap))

	state.Delete(entry)
	require.False(t, state.HasRanges())
	require.False(t, state.HasTable(1))
	require.Zero(t, state.DelSize(0))
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
	require.True(t, bytes.HasPrefix(kr.Left, []byte("a")))
	require.True(t, bytes.HasPrefix(kr.Right, []byte("c")))

	left, right := OverlappingTables([]TableMeta{t1, t2, t3}, RangeForTables([]TableMeta{t2}))
	require.Equal(t, 0, left)
	require.Equal(t, 2, right)

	plan, ok := PlanForIngestFallback(2, []TableMeta{t1})
	require.True(t, ok)
	require.Equal(t, 2, plan.ThisLevel)

	plan, ok = PlanForRegular(1, tables, 2, []TableMeta{t3}, nil)
	require.True(t, ok)
	require.Equal(t, uint64(2), plan.TopIDs[0])

	old := time.Now().Add(-2 * time.Hour)
	t4 := TableMeta{ID: 4, MinKey: ikey("f", 10), MaxKey: ikey("g", 1), StaleSize: 12 << 20, CreatedAt: old, Size: 8 << 20}
	t5 := TableMeta{ID: 5, MinKey: ikey("h", 10), MaxKey: ikey("i", 1), StaleSize: 1, CreatedAt: old, Size: 8 << 20}
	plan, ok = PlanForMaxLevel(6, []TableMeta{t4, t5}, 20<<20, nil, time.Now())
	require.True(t, ok)
	require.Equal(t, uint64(4), plan.TopIDs[0])

	shard := []TableMeta{
		{ID: 7, MinKey: ikey("a", 9), MaxKey: ikey("b", 1), Size: 4 << 20},
		{ID: 8, MinKey: ikey("b", 9), MaxKey: ikey("c", 1), Size: 4 << 20},
		{ID: 9, MinKey: ikey("c", 9), MaxKey: ikey("d", 1), Size: 4 << 20},
	}
	plan, ok = PlanForIngestShard(0, shard, 1, []TableMeta{}, 4<<20, 1, nil)
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

	recent := time.Now().Add(-5 * time.Second)
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

func TestStateAddRangeAndDebug(t *testing.T) {
	state := NewState(1)
	kr := KeyRange{Left: ikey("a", 1), Right: ikey("b", 1)}
	state.AddRangeWithTables(0, kr, []uint64{10, 20})
	require.True(t, state.HasTable(10))
	require.Contains(t, kr.String(), "left=")
	require.NotEmpty(t, state.levels[0].debug())
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
