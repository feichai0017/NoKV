package lsm

import (
	"bytes"
	"sort"

	"github.com/feichai0017/NoKV/engine/kv"
)

type rangeFilterSpan struct {
	minBase []byte
	maxBase []byte
	tbl     *table
}

type rangeFilter struct {
	levelNum       int
	spans          []rangeFilterSpan
	nonOverlapping bool
}

// Small and overlapping levels rarely amortize the extra pruning work.
const rangeFilterMinSpanCount = 8

func buildRangeFilter(levelNum int, tables []*table) rangeFilter {
	filter := rangeFilter{
		levelNum:       levelNum,
		spans:          make([]rangeFilterSpan, 0, len(tables)),
		nonOverlapping: levelNum > 0,
	}
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		filter.spans = append(filter.spans, rangeFilterSpan{
			minBase: guideBaseKey(tbl.MinKey()),
			maxBase: guideBaseKey(tbl.MaxKey()),
			tbl:     tbl,
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

func guideBaseKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if _, _, _, ok := kv.SplitInternalKey(key); ok {
		return kv.InternalToBaseKey(key)
	}
	return kv.BaseKey(kv.CFDefault, key)
}

func guideUserKey(key []byte) []byte {
	if len(key) == 0 {
		return nil
	}
	if _, userKey, _, ok := kv.SplitInternalKey(key); ok {
		return userKey
	}
	return key
}

func (filter rangeFilter) tablesForPoint(key []byte) []*table {
	if len(filter.spans) == 0 || len(key) == 0 {
		return nil
	}
	baseKey := guideBaseKey(key)
	if len(baseKey) == 0 {
		return nil
	}
	if !filter.nonOverlapping {
		out := make([]*table, 0, len(filter.spans))
		for _, span := range filter.spans {
			if span.tbl == nil || !span.covers(baseKey) {
				continue
			}
			out = append(out, span.tbl)
		}
		return out
	}
	tbl := filter.tableForPointBaseKey(baseKey)
	if tbl == nil {
		return nil
	}
	return []*table{tbl}
}

func (filter rangeFilter) tableForPoint(key []byte) *table {
	if len(filter.spans) == 0 || len(key) == 0 {
		return nil
	}
	baseKey := guideBaseKey(key)
	if len(baseKey) == 0 || !filter.nonOverlapping {
		return nil
	}
	return filter.tableForPointBaseKey(baseKey)
}

func (filter rangeFilter) tableForPointBaseKey(baseKey []byte) *table {
	idx := sort.Search(len(filter.spans), func(i int) bool {
		return bytes.Compare(filter.spans[i].maxBase, baseKey) >= 0
	})
	if idx >= len(filter.spans) {
		return nil
	}
	span := filter.spans[idx]
	if !span.covers(baseKey) || span.tbl == nil {
		return nil
	}
	return span.tbl
}

func (filter rangeFilter) tablesForBounds(lower, upper []byte) []*table {
	if len(filter.spans) == 0 {
		return nil
	}
	lower = guideBaseKey(lower)
	upper = guideBaseKey(upper)
	if len(lower) == 0 && len(upper) == 0 {
		out := make([]*table, 0, len(filter.spans))
		for _, span := range filter.spans {
			if span.tbl != nil {
				out = append(out, span.tbl)
			}
		}
		return out
	}
	if !filter.nonOverlapping {
		return filter.filterLinear(lower, upper)
	}
	start := 0
	if len(lower) > 0 {
		start = sort.Search(len(filter.spans), func(i int) bool {
			return bytes.Compare(filter.spans[i].maxBase, lower) >= 0
		})
	}
	end := len(filter.spans)
	if len(upper) > 0 {
		end = sort.Search(len(filter.spans), func(i int) bool {
			return bytes.Compare(filter.spans[i].minBase, upper) >= 0
		})
	}
	if start >= end {
		return nil
	}
	out := make([]*table, 0, end-start)
	for _, span := range filter.spans[start:end] {
		if span.tbl == nil || !span.overlaps(lower, upper) {
			continue
		}
		out = append(out, span.tbl)
	}
	return out
}

func (filter rangeFilter) filterLinear(lower, upper []byte) []*table {
	out := make([]*table, 0, len(filter.spans))
	for _, span := range filter.spans {
		if span.tbl == nil || !span.overlaps(lower, upper) {
			continue
		}
		out = append(out, span.tbl)
	}
	return out
}

func (span rangeFilterSpan) covers(baseKey []byte) bool {
	if len(baseKey) == 0 || len(span.minBase) == 0 || len(span.maxBase) == 0 {
		return false
	}
	return bytes.Compare(baseKey, span.minBase) >= 0 && bytes.Compare(baseKey, span.maxBase) <= 0
}

func (span rangeFilterSpan) overlaps(lower, upper []byte) bool {
	if len(span.minBase) == 0 || len(span.maxBase) == 0 {
		return true
	}
	if len(lower) > 0 && bytes.Compare(span.maxBase, lower) < 0 {
		return false
	}
	if len(upper) > 0 && bytes.Compare(span.minBase, upper) >= 0 {
		return false
	}
	return true
}

func filterTablesByBounds(tables []*table, lower, upper []byte) []*table {
	lower = guideBaseKey(lower)
	upper = guideBaseKey(upper)
	if len(tables) == 0 {
		return nil
	}
	if len(lower) == 0 && len(upper) == 0 {
		out := make([]*table, 0, len(tables))
		for _, tbl := range tables {
			if tbl != nil {
				out = append(out, tbl)
			}
		}
		return out
	}
	out := make([]*table, 0, len(tables))
	for _, tbl := range tables {
		if tbl == nil {
			continue
		}
		span := rangeFilterSpan{
			minBase: guideBaseKey(tbl.MinKey()),
			maxBase: guideBaseKey(tbl.MaxKey()),
			tbl:     tbl,
		}
		if span.overlaps(lower, upper) {
			out = append(out, tbl)
		}
	}
	return out
}
