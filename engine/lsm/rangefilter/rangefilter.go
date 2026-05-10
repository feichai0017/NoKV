// Package rangefilter provides a per-level range pruning structure for the
// LSM read path: given a level's tables (each exposing MinKey/MaxKey), it
// answers "which tables can possibly contain key K" or "which tables overlap
// [lower, upper]". The package is engine-neutral and operates over any type
// satisfying the Bounded interface (the lsm package wires *table in).
package rangefilter

import (
	"bytes"
	"sort"

	"github.com/feichai0017/NoKV/engine/kv"
)

// MinSpanCount is the smallest span count where range pruning amortizes the
// extra bookkeeping cost. Levels with fewer tables fall back to a linear
// scan.
const MinSpanCount = 8

// Bounded is the minimal contract a table must satisfy to be range-filtered.
type Bounded interface {
	MinKey() []byte
	MaxKey() []byte
}

type span[T Bounded] struct {
	minBase []byte
	maxBase []byte
	tbl     T
	present bool
}

// Filter holds a sorted, possibly non-overlapping view of the input tables.
type Filter[T Bounded] struct {
	levelNum       int
	spans          []span[T]
	nonOverlapping bool
}

// Build constructs a Filter for the given level and table set. The level
// number is recorded for diagnostics; non-zero levels are assumed to be sorted
// and non-overlapping unless Build detects otherwise during validation.
func Build[T Bounded](levelNum int, tables []T) Filter[T] {
	filter := Filter[T]{
		levelNum:       levelNum,
		spans:          make([]span[T], 0, len(tables)),
		nonOverlapping: levelNum > 0,
	}
	for _, tbl := range tables {
		if any(tbl) == nil {
			continue
		}
		filter.spans = append(filter.spans, span[T]{
			minBase: GuideBaseKey(tbl.MinKey()),
			maxBase: GuideBaseKey(tbl.MaxKey()),
			tbl:     tbl,
			present: true,
		})
	}
	for i := 1; i < len(filter.spans); i++ {
		prev := filter.spans[i-1]
		cur := filter.spans[i]
		if bytes.Compare(prev.minBase, cur.minBase) > 0 ||
			bytes.Compare(prev.maxBase, cur.minBase) >= 0 {
			filter.nonOverlapping = false
			break
		}
	}
	return filter
}

// SpanCount returns the number of usable spans in this filter.
func (f Filter[T]) SpanCount() int { return len(f.spans) }

// NonOverlapping reports whether the spans form a strictly non-overlapping,
// sorted sequence (which enables binary search lookups).
func (f Filter[T]) NonOverlapping() bool { return f.nonOverlapping }

// TableForPoint returns the single table that may contain the key. Only
// applicable when NonOverlapping reports true.
func (f Filter[T]) TableForPoint(key []byte) (T, bool) {
	var zero T
	if len(f.spans) == 0 || len(key) == 0 {
		return zero, false
	}
	baseKey := GuideBaseKey(key)
	if len(baseKey) == 0 || !f.nonOverlapping {
		return zero, false
	}
	return f.tableForPointBaseKey(baseKey)
}

// TablesForPoint returns every table that may contain the key.
func (f Filter[T]) TablesForPoint(key []byte) []T {
	if len(f.spans) == 0 || len(key) == 0 {
		return nil
	}
	baseKey := GuideBaseKey(key)
	if len(baseKey) == 0 {
		return nil
	}
	if !f.nonOverlapping {
		out := make([]T, 0, len(f.spans))
		for _, sp := range f.spans {
			if !sp.present || !sp.covers(baseKey) {
				continue
			}
			out = append(out, sp.tbl)
		}
		return out
	}
	tbl, ok := f.tableForPointBaseKey(baseKey)
	if !ok {
		return nil
	}
	return []T{tbl}
}

func (f Filter[T]) tableForPointBaseKey(baseKey []byte) (T, bool) {
	var zero T
	idx := sort.Search(len(f.spans), func(i int) bool {
		return bytes.Compare(f.spans[i].maxBase, baseKey) >= 0
	})
	if idx >= len(f.spans) {
		return zero, false
	}
	sp := f.spans[idx]
	if !sp.present || !sp.covers(baseKey) {
		return zero, false
	}
	return sp.tbl, true
}

// TablesForBounds returns every table whose key range overlaps [lower, upper].
// Empty bounds match all tables.
func (f Filter[T]) TablesForBounds(lower, upper []byte) []T {
	if len(f.spans) == 0 {
		return nil
	}
	lower = GuideBaseKey(lower)
	upper = GuideBaseKey(upper)
	if len(lower) == 0 && len(upper) == 0 {
		out := make([]T, 0, len(f.spans))
		for _, sp := range f.spans {
			if sp.present {
				out = append(out, sp.tbl)
			}
		}
		return out
	}
	if !f.nonOverlapping {
		return f.filterLinear(lower, upper)
	}
	start := 0
	if len(lower) > 0 {
		start = sort.Search(len(f.spans), func(i int) bool {
			return bytes.Compare(f.spans[i].maxBase, lower) >= 0
		})
	}
	end := len(f.spans)
	if len(upper) > 0 {
		end = sort.Search(len(f.spans), func(i int) bool {
			return bytes.Compare(f.spans[i].minBase, upper) >= 0
		})
	}
	if start >= end {
		return nil
	}
	out := make([]T, 0, end-start)
	for _, sp := range f.spans[start:end] {
		if !sp.present || !sp.overlaps(lower, upper) {
			continue
		}
		out = append(out, sp.tbl)
	}
	return out
}

func (f Filter[T]) filterLinear(lower, upper []byte) []T {
	out := make([]T, 0, len(f.spans))
	for _, sp := range f.spans {
		if !sp.present || !sp.overlaps(lower, upper) {
			continue
		}
		out = append(out, sp.tbl)
	}
	return out
}

func (sp span[T]) covers(baseKey []byte) bool {
	if len(baseKey) == 0 || len(sp.minBase) == 0 || len(sp.maxBase) == 0 {
		return false
	}
	return bytes.Compare(baseKey, sp.minBase) >= 0 && bytes.Compare(baseKey, sp.maxBase) <= 0
}

func (sp span[T]) overlaps(lower, upper []byte) bool {
	if len(sp.minBase) == 0 || len(sp.maxBase) == 0 {
		return true
	}
	if len(lower) > 0 && bytes.Compare(sp.maxBase, lower) < 0 {
		return false
	}
	if len(upper) > 0 && bytes.Compare(sp.minBase, upper) >= 0 {
		return false
	}
	return true
}

// FilterByBounds returns the subset of tables whose [MinKey, MaxKey] overlaps
// the requested bounds. Empty bounds match all tables. This helper is for
// callers that don't keep a Filter cached (e.g. landing buffer ad-hoc scans).
func FilterByBounds[T Bounded](tables []T, lower, upper []byte) []T {
	lower = GuideBaseKey(lower)
	upper = GuideBaseKey(upper)
	if len(tables) == 0 {
		return nil
	}
	if len(lower) == 0 && len(upper) == 0 {
		out := make([]T, 0, len(tables))
		for _, tbl := range tables {
			if any(tbl) == nil {
				continue
			}
			out = append(out, tbl)
		}
		return out
	}
	out := make([]T, 0, len(tables))
	for _, tbl := range tables {
		if any(tbl) == nil {
			continue
		}
		sp := span[T]{
			minBase: GuideBaseKey(tbl.MinKey()),
			maxBase: GuideBaseKey(tbl.MaxKey()),
			present: true,
		}
		if sp.overlaps(lower, upper) {
			out = append(out, tbl)
		}
	}
	return out
}

// GuideBaseKey extracts the column-family-prefixed base key (without version)
// from an internal key, falling back to a CFDefault base when the input is a
// raw user key.
func GuideBaseKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if _, _, _, ok := kv.SplitInternalKey(key); ok {
		return kv.InternalToBaseKey(key)
	}
	return kv.BaseKey(kv.CFDefault, key)
}

// GuideUserKey returns the user-key portion of an internal key, or the input
// itself when it is not an internal key.
func GuideUserKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if _, userKey, _, ok := kv.SplitInternalKey(key); ok {
		return userKey
	}
	return key
}
