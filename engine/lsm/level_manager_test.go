package lsm

import (
	"bytes"
	"os"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/pacer"
	"github.com/stretchr/testify/require"
)

func TestL0ReplaceTablesOrdering(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	t1 := buildTableWithEntry(t, lsm, 1, "C", 1, "old")
	t2 := buildTableWithEntry(t, lsm, 2, "A", 1, "old")
	t3 := buildTableWithEntry(t, lsm, 3, "B", 1, "old")
	t4 := buildTableWithEntry(t, lsm, 4, "A", 2, "new")

	levelHandler := lsm.levels.levels[0]
	levelHandler.tables = []*table{t1, t2, t3}
	toDel := []*table{t2, t3}
	toAdd := []*table{t4}
	require.NoError(t, levelHandler.replaceTables(toDel, toAdd))
	require.Equal(t, []*table{t1, t4}, levelHandler.tables)

	require.NoError(t, t1.DecrRef())
	require.NoError(t, t4.DecrRef())
}

func TestLevelManagerGetChoosesHighestVisibleVersionAcrossLevels(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	key := []byte("mvcc-cross-level")
	l0Delete := kv.NewInternalEntry(kv.CFDefault, key, 393, nil, kv.BitDelete, 0)
	l1Put := kv.NewInternalEntry(kv.CFDefault, key, 396, []byte("v396"), 0, 0)
	t0 := buildTableWithEntries(t, lsm, 11, l0Delete)
	t1 := buildTableWithEntries(t, lsm, 12, l1Put)
	lsm.levels.levels[0].tables = []*table{t0}
	lsm.levels.levels[0].Sort()
	lsm.levels.levels[1].tables = []*table{t1}
	lsm.levels.levels[1].Sort()

	got, err := lsm.levels.Get(kv.InternalKey(kv.CFDefault, key, 396))
	require.NoError(t, err)
	require.Equal(t, uint64(396), got.Version)
	require.Equal(t, []byte("v396"), got.Value)
	got.DecrRef()

	require.NoError(t, t0.DecrRef())
	require.NoError(t, t1.DecrRef())
}

func TestLevelHandlerRangeFilterPrunesPointAndBounds(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	lh := lsm.levels.levels[1]
	tblA := buildTableWithEntry(t, lsm, 101, "a", 1, "va")
	tblD := buildTableWithEntry(t, lsm, 102, "d", 1, "vd")
	tblG := buildTableWithEntry(t, lsm, 103, "g", 1, "vg")
	tblJ := buildTableWithEntry(t, lsm, 104, "j", 1, "vj")
	tblM := buildTableWithEntry(t, lsm, 105, "m", 1, "vm")
	tblP := buildTableWithEntry(t, lsm, 106, "p", 1, "vp")
	tblS := buildTableWithEntry(t, lsm, 107, "s", 1, "vs")
	tblV := buildTableWithEntry(t, lsm, 108, "v", 1, "vv")

	lh.tables = []*table{tblV, tblG, tblA, tblP, tblD, tblS, tblJ, tblM}
	lh.Sort()

	require.Equal(t, 8, lh.filter.SpanCount())

	point := lh.selectTablesForKey(kv.InternalKey(kv.CFDefault, []byte("d"), 5), true)
	require.Len(t, point, 1)
	require.Equal(t, uint64(102), point[0].FID())

	bounded := lh.selectTablesForBounds([]byte("c"), []byte("f"), true)
	require.Len(t, bounded, 1)
	require.Equal(t, uint64(102), bounded[0].FID())

	miss := lh.selectTablesForKey(kv.InternalKey(kv.CFDefault, []byte("z"), 5), true)
	require.Empty(t, miss)

	diag := lsm.Diagnostics()
	require.Equal(t, uint64(1), diag.RangeFilter.PointCandidates)
	require.Equal(t, uint64(15), diag.RangeFilter.PointPruned)
	require.Equal(t, uint64(1), diag.RangeFilter.BoundedCandidates)
	require.Equal(t, uint64(7), diag.RangeFilter.BoundedPruned)
	require.Equal(t, uint64(0), diag.RangeFilter.Fallbacks)

	for _, tbl := range []*table{tblA, tblD, tblG, tblJ, tblM, tblP, tblS, tblV} {
		require.NoError(t, tbl.DecrRef())
	}
}

func TestLevelHandlerAddRefreshesRangeFilter(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	lh := lsm.levels.levels[1]
	tblB := buildTableWithEntry(t, lsm, 301, "b", 1, "vb")
	tblA := buildTableWithEntry(t, lsm, 302, "a", 1, "va")

	lh.add(tblB)
	lh.add(tblA)

	require.Equal(t, 2, lh.filter.SpanCount())
	require.True(t, lh.filter.NonOverlapping())
	require.Equal(t, uint64(302), lh.tables[0].FID())
	require.Equal(t, uint64(301), lh.tables[1].FID())

	for _, tbl := range []*table{tblA, tblB} {
		require.NoError(t, tbl.DecrRef())
	}
}

func TestLevelHandlerL0BoundedMetricsRecordFallback(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	lh := lsm.levels.levels[0]
	tblA := buildTableWithEntry(t, lsm, 401, "a", 1, "va")
	tblD := buildTableWithEntry(t, lsm, 402, "d", 1, "vd")
	lh.tables = []*table{tblA, tblD}
	lh.Sort()

	iters := lh.iterators(&index.Options{
		IsAsc:      true,
		LowerBound: []byte("b"),
		UpperBound: []byte("e"),
	})
	for _, it := range iters {
		require.NoError(t, it.Close())
	}

	diag := lsm.Diagnostics()
	require.Equal(t, uint64(1), diag.RangeFilter.Fallbacks)

	for _, tbl := range []*table{tblA, tblD} {
		require.NoError(t, tbl.DecrRef())
	}
}

func TestLevelHandlerIteratorsRespectBoundsWithLanding(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	lh := lsm.levels.levels[1]
	tblA := buildTableWithEntry(t, lsm, 201, "a", 1, "va")
	tblD := buildTableWithEntry(t, lsm, 202, "d", 1, "vd")
	tblG := buildTableWithEntry(t, lsm, 203, "g", 1, "vg")
	landingB := buildTableWithEntry(t, lsm, 204, "b", 1, "vb")
	landingE := buildTableWithEntry(t, lsm, 205, "e", 1, "ve")

	lh.tables = []*table{tblA, tblD, tblG}
	lh.landing.AddBatch([]*table{landingB, landingE})
	lh.Sort()

	iters := lh.iterators(&index.Options{
		IsAsc:      true,
		LowerBound: []byte("c"),
		UpperBound: []byte("f"),
	})
	merge := NewMergeIterator(iters, false)
	defer func() { require.NoError(t, merge.Close()) }()

	var keys [][]byte
	for merge.Rewind(); merge.Valid(); merge.Next() {
		entry := merge.Item().Entry()
		_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		keys = append(keys, append([]byte(nil), userKey...))
	}
	require.Len(t, keys, 2)
	require.True(t, bytes.Equal(keys[0], []byte("d")) || bytes.Equal(keys[0], []byte("e")))
	require.True(t, bytes.Equal(keys[1], []byte("d")) || bytes.Equal(keys[1], []byte("e")))
	require.NotEqual(t, string(keys[0]), string(keys[1]))

	for _, tbl := range []*table{tblA, tblD, tblG, landingB, landingE} {
		require.NoError(t, tbl.DecrRef())
	}
}

func TestLevelHandlerIteratorsSkipLeadingEmptyBoundedTables(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() {
		require.NoError(t, lsm.Close())
		require.NoError(t, os.RemoveAll(lsm.option.WorkDir))
	}()

	lh := lsm.levels.levels[1]
	tblA := buildTableWithEntry(t, lsm, 501, "a", 1, "va")
	tblB := buildTableWithEntry(t, lsm, 502, "b", 1, "vb")
	tblC := buildTableWithEntry(t, lsm, 503, "c", 1, "vc")
	tblD := buildTableWithEntry(t, lsm, 504, "d", 1, "vd")

	lh.tables = []*table{tblA, tblB, tblC, tblD}
	lh.Sort()

	iters := lh.iterators(&index.Options{
		IsAsc:      true,
		LowerBound: []byte("c"),
		UpperBound: []byte("e"),
	})
	merge := NewMergeIterator(iters, false)
	defer func() { require.NoError(t, merge.Close()) }()

	var keys []string
	for merge.Rewind(); merge.Valid(); merge.Next() {
		entry := merge.Item().Entry()
		_, userKey, _, ok := kv.SplitInternalKey(entry.Key)
		require.True(t, ok)
		keys = append(keys, string(userKey))
	}
	require.Equal(t, []string{"c", "d"}, keys)

	for _, tbl := range []*table{tblA, tblB, tblC, tblD} {
		require.NoError(t, tbl.DecrRef())
	}
}

func TestCompactionPacerBypassesWhenL0IsNearStall(t *testing.T) {
	lm := &levelManager{
		opt: &Options{
			CompactionWriteBytesPerSec: 100,
			CompactionPacingBypassL0:   2,
		},
		compactionPacer: pacer.New(100),
		levels: []*levelHandler{
			{tables: []*table{{}, {}}},
		},
	}

	require.True(t, lm.compactionPacerBypassActive())
	require.Nil(t, lm.compactionPacerForBuild())
}
