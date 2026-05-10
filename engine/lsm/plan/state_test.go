package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
)

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

func TestStateAddRangeAndDebug(t *testing.T) {
	state := NewState(1)
	kr := KeyRange{Left: ikey("a", 1), Right: ikey("b", 1)}
	state.AddRangeWithTables(0, kr, []uint64{10, 20})
	require.True(t, state.HasTable(10))
	require.Contains(t, kr.String(), "left=")
	require.True(t, state.HasRanges())
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

	require.Nil(t, state.Delete(entry))
	require.False(t, state.HasRanges())
	require.False(t, state.HasTable(1))
	require.Zero(t, state.DelSize(0))
}

func TestStateCompareAndDeleteIfKeyNotInRange(t *testing.T) {
	state := NewState(2)
	entry := StateEntry{
		ThisLevel: 0,
		NextLevel: 1,
		ThisRange: KeyRange{Left: ikey("a", 10), Right: ikey("b", 1)},
		NextRange: KeyRange{Left: ikey("c", 10), Right: ikey("d", 1)},
		ThisSize:  128,
		TableIDs:  []uint64{1, 2},
	}
	require.NotNil(t, state.Delete(entry))
}
