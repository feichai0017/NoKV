// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package rangefilter

import (
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

// fakeTable implements Bounded for tests using user-key strings encoded as
// internal keys. Each table covers [Min, Max] inclusive.
type fakeTable struct {
	id  uint64
	min []byte
	max []byte
}

func (t *fakeTable) MinKey() []byte { return t.min }
func (t *fakeTable) MaxKey() []byte { return t.max }

func ikey(s string, ts uint64) []byte {
	return kv.InternalKey(kv.CFDefault, []byte(s), ts)
}

func newTable(id uint64, lo, hi string) *fakeTable {
	return &fakeTable{id: id, min: ikey(lo, 10), max: ikey(hi, 1)}
}

func TestBuildEmptyFilter(t *testing.T) {
	f := Build[*fakeTable](1, nil)
	require.Equal(t, 0, f.SpanCount())
	require.True(t, f.NonOverlapping())

	tbl, ok := f.TableForPoint(ikey("a", 5))
	require.False(t, ok)
	require.Nil(t, tbl)

	require.Nil(t, f.TablesForPoint(ikey("a", 5)))
	require.Nil(t, f.TablesForBounds(ikey("a", 5), ikey("z", 1)))
}

func TestBuildNonOverlappingDetection(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
		newTable(3, "k", "m"),
	}
	f := Build(1, tables)
	require.True(t, f.NonOverlapping())
	require.Equal(t, 3, f.SpanCount())
}

func TestBuildOverlappingDetection(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "f"),
		newTable(2, "d", "j"),
	}
	f := Build(1, tables)
	require.False(t, f.NonOverlapping())
}

func TestL0AlwaysOverlapping(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
	}
	// L0 explicitly disables non-overlapping fast path; even disjoint inputs
	// must report overlapping.
	f := Build(0, tables)
	require.False(t, f.NonOverlapping())
}

func TestTableForPointSingleHit(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
		newTable(3, "k", "m"),
	}
	f := Build(1, tables)
	tbl, ok := f.TableForPoint(ikey("f", 5))
	require.True(t, ok)
	require.Equal(t, uint64(2), tbl.id)
}

func TestTableForPointMiss(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
	}
	f := Build(1, tables)
	tbl, ok := f.TableForPoint(ikey("z", 5))
	require.False(t, ok)
	require.Nil(t, tbl)
}

func TestTableForPointReturnsZeroOnOverlapping(t *testing.T) {
	// TableForPoint requires NonOverlapping; on L0 it should not pick.
	tables := []*fakeTable{newTable(1, "a", "c")}
	f := Build(0, tables) // L0 → NonOverlapping=false
	_, ok := f.TableForPoint(ikey("a", 5))
	require.False(t, ok)
}

func TestTablesForPointWithOverlap(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "f"),
		newTable(2, "d", "j"),
		newTable(3, "k", "m"),
	}
	f := Build(0, tables)
	hits := f.TablesForPoint(ikey("e", 5))
	require.Len(t, hits, 2)
}

func TestTablesForBoundsEmptyMatchesAll(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
	}
	f := Build(1, tables)
	all := f.TablesForBounds(nil, nil)
	require.Len(t, all, 2)
}

func TestTablesForBoundsNarrow(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
		newTable(3, "k", "m"),
	}
	f := Build(1, tables)
	hits := f.TablesForBounds(ikey("d", 1), ikey("h", 1))
	require.Len(t, hits, 1)
	require.Equal(t, uint64(2), hits[0].id)
}

func TestTablesForBoundsLinearOverlapping(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "f"),
		newTable(2, "d", "j"),
	}
	f := Build(0, tables) // overlapping → linear path
	hits := f.TablesForBounds(ikey("e", 1), ikey("g", 1))
	require.Len(t, hits, 2)
}

func TestFilterByBounds(t *testing.T) {
	tables := []*fakeTable{
		newTable(1, "a", "c"),
		newTable(2, "e", "g"),
		newTable(3, "k", "m"),
	}
	hits := FilterByBounds(tables, ikey("d", 1), ikey("h", 1))
	require.Len(t, hits, 1)
	require.Equal(t, uint64(2), hits[0].id)

	all := FilterByBounds(tables, nil, nil)
	require.Len(t, all, 3)

	require.Nil(t, FilterByBounds[*fakeTable](nil, ikey("a", 1), ikey("z", 1)))
}

func TestGuideBaseKeyAndUserKey(t *testing.T) {
	internal := ikey("hello", 5)
	require.NotNil(t, GuideBaseKey(internal))
	require.Equal(t, []byte("hello"), GuideUserKey(internal))

	require.Nil(t, GuideBaseKey(nil))
	require.Nil(t, GuideUserKey(nil))

	// Non-internal key falls back to CFDefault base / itself.
	plain := []byte("plain")
	require.NotNil(t, GuideBaseKey(plain))
	require.Equal(t, plain, GuideUserKey(plain))
}

func TestMinSpanCountIsExported(t *testing.T) {
	require.Equal(t, 8, MinSpanCount)
}
