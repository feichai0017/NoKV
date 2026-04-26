package lsm

import (
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

func TestBuildL0SublevelsArrangesNonOverlappingTablesPerSublevel(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	// Three tables; first two overlap on key range, third is disjoint from both.
	a := buildTableWithEntry(t, lsm, 5001, "a", 1, "va")
	b := buildTableWithEntry(t, lsm, 5002, "a", 2, "va2") // overlaps a
	c := buildTableWithEntry(t, lsm, 5003, "z", 1, "vz")  // disjoint

	subs := buildL0Sublevels([]*table{a, b, c})
	require.Len(t, subs, 2, "expected two sublevels for overlapping a/b plus disjoint c")

	// First sublevel takes a (smallest fid wins ties for same MinKey) and c
	// (because c's MinKey > a's MaxKey, so c can sit alongside a).
	require.Len(t, subs[0], 2)
	require.Equal(t, a.fid, subs[0][0].fid)
	require.Equal(t, c.fid, subs[0][1].fid)

	// Second sublevel takes b alone.
	require.Len(t, subs[1], 1)
	require.Equal(t, b.fid, subs[1][0].fid)
}

func TestL0SublevelCandidateBinarySearch(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	a := buildTableWithEntry(t, lsm, 6001, "a", 1, "va")
	m := buildTableWithEntry(t, lsm, 6002, "m", 1, "vm")
	z := buildTableWithEntry(t, lsm, 6003, "z", 1, "vz")

	sub := l0Sublevel{a, m, z}

	keyA := []byte(kv.InternalKey(kv.CFDefault, []byte("a"), 0))
	keyM := []byte(kv.InternalKey(kv.CFDefault, []byte("m"), 0))
	keyZ := []byte(kv.InternalKey(kv.CFDefault, []byte("z"), 0))
	keyBefore := []byte(kv.InternalKey(kv.CFDefault, []byte("0"), 0))
	keyGap := []byte(kv.InternalKey(kv.CFDefault, []byte("g"), 0))

	require.Equal(t, a.fid, sub.candidate(keyA).fid)
	require.Equal(t, m.fid, sub.candidate(keyM).fid)
	require.Equal(t, z.fid, sub.candidate(keyZ).fid)
	require.Nil(t, sub.candidate(keyBefore), "key before all tables returns nil")
	require.Nil(t, sub.candidate(keyGap), "key in gap returns nil")
}

func TestL0SublevelsLookupReturnsCandidatePerSublevel(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	a := buildTableWithEntry(t, lsm, 7001, "a", 1, "va")
	b := buildTableWithEntry(t, lsm, 7002, "a", 2, "va2") // overlaps a -> separate sublevel
	c := buildTableWithEntry(t, lsm, 7003, "a", 3, "va3") // overlaps a/b -> third sublevel

	subs := buildL0Sublevels([]*table{a, b, c})
	require.Len(t, subs, 3)

	keyA := []byte(kv.InternalKey(kv.CFDefault, []byte("a"), 0))
	candidates := l0CandidateTables(subs, keyA)
	require.Len(t, candidates, 3)
	// Each sublevel contributes one candidate; later resolution by version
	// happens in searchL0SST.
	gotFids := map[uint64]struct{}{}
	for _, t := range candidates {
		gotFids[t.fid] = struct{}{}
	}
	require.Contains(t, gotFids, a.fid)
	require.Contains(t, gotFids, b.fid)
	require.Contains(t, gotFids, c.fid)
}

func TestSelectTablesForKeyL0UsesSublevels(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	// Disjoint tables across L0; without sublevels selectTablesForKey would
	// have to iterate every L0 table to test [Min, Max] containment.
	a := buildTableWithEntry(t, lsm, 8001, "a", 1, "va")
	m := buildTableWithEntry(t, lsm, 8002, "m", 1, "vm")
	z := buildTableWithEntry(t, lsm, 8003, "z", 1, "vz")

	l0 := lsm.levels.levels[0]
	l0.add(a)
	l0.add(m)
	l0.add(z)
	l0.Sort()

	require.Len(t, l0.l0Sublevels, 1, "three disjoint tables collapse into one sublevel")

	keyM := []byte(kv.InternalKey(kv.CFDefault, []byte("m"), 0))
	got := l0.selectTablesForKey(keyM, false)
	require.Len(t, got, 1)
	require.Equal(t, m.fid, got[0].fid)

	keyGap := []byte(kv.InternalKey(kv.CFDefault, []byte("g"), 0))
	none := l0.selectTablesForKey(keyGap, false)
	require.Empty(t, none, "key in the gap returns no candidates")
}

func TestL0GetReadPathReturnsHighestVersionAcrossSublevels(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	older := buildTableWithEntry(t, lsm, 9001, "k", 1, "v1")
	newer := buildTableWithEntry(t, lsm, 9002, "k", 5, "v5")
	other := buildTableWithEntry(t, lsm, 9003, "z", 1, "vz")

	l0 := lsm.levels.levels[0]
	l0.add(older)
	l0.add(newer)
	l0.add(other)
	l0.Sort()
	require.Greater(t, len(l0.l0Sublevels), 1, "overlapping older/newer must occupy distinct sublevels")

	keyK := []byte(kv.InternalKey(kv.CFDefault, []byte("k"), 100))
	entry, err := l0.Get(keyK)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("v5"), entry.Value, "highest version across sublevels wins")
	entry.DecrRef()
}

// BenchmarkL0SelectTablesForKeyLinear measures the cost of the legacy linear
// scan over L0 tables. We force the sublevel index to nil so selectTablesForKey
// falls back to getTablesForKeyLinear.
func BenchmarkL0SelectTablesForKeyLinear(b *testing.B) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const n = 64
	l0 := lsm.levels.levels[0]
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		tbl := buildTableWithEntry(&testing.T{}, lsm, uint64(20000+i), key, uint64(i+1), "v")
		l0.add(tbl)
	}
	l0.Sort()
	l0.l0Sublevels = nil // force legacy linear scan path

	target := []byte(kv.InternalKey(kv.CFDefault, []byte("k00031"), 0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l0.selectTablesForKey(target, false)
	}
}

// BenchmarkL0SelectTablesForKeyViaSublevels measures the same lookup using
// the sublevel binary-search path. The delta versus the linear bench is the
// per-Get speedup that L0 sublevels Phase A delivers.
func BenchmarkL0SelectTablesForKeyViaSublevels(b *testing.B) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	const n = 64
	l0 := lsm.levels.levels[0]
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%05d", i)
		tbl := buildTableWithEntry(&testing.T{}, lsm, uint64(30000+i), key, uint64(i+1), "v")
		l0.add(tbl)
	}
	l0.Sort()
	require.NotNil(b, l0.l0Sublevels, "sortTablesLocked should populate sublevels for L0")

	target := []byte(kv.InternalKey(kv.CFDefault, []byte("k00031"), 0))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = l0.selectTablesForKey(target, false)
	}
}
