package lsm

import (
	"bytes"
	"sort"

	"github.com/feichai0017/NoKV/kv"
)

type rangeFilterSpan struct {
	minUser []byte
	maxUser []byte
	tbl     *table
}

type rangeFilter struct {
	levelNum       int
	spans          []rangeFilterSpan
	nonOverlapping bool
}

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
			minUser: guideUserKey(tbl.MinKey()),
			maxUser: guideUserKey(tbl.MaxKey()),
			tbl:     tbl,
		})
	}
	for i := 1; i < len(filter.spans); i++ {
		prev := filter.spans[i-1]
		cur := filter.spans[i]
		if bytes.Compare(prev.minUser, cur.minUser) > 0 ||
			bytes.Compare(prev.maxUser, cur.minUser) >= 0 {
			filter.nonOverlapping = false
			break
		}
	}
	return filter
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
	userKey := guideUserKey(key)
	if len(userKey) == 0 {
		return nil
	}
	if !filter.nonOverlapping {
		out := make([]*table, 0, len(filter.spans))
		for _, span := range filter.spans {
			if span.tbl == nil || !span.covers(userKey) {
				continue
			}
			out = append(out, span.tbl)
		}
		return out
	}
	idx := sort.Search(len(filter.spans), func(i int) bool {
		return bytes.Compare(filter.spans[i].maxUser, userKey) >= 0
	})
	if idx >= len(filter.spans) {
		return nil
	}
	span := filter.spans[idx]
	if !span.covers(userKey) || span.tbl == nil {
		return nil
	}
	return []*table{span.tbl}
}

func (filter rangeFilter) tablesForBounds(lower, upper []byte) []*table {
	if len(filter.spans) == 0 {
		return nil
	}
	lower = guideUserKey(lower)
	upper = guideUserKey(upper)
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
			return bytes.Compare(filter.spans[i].maxUser, lower) >= 0
		})
	}
	end := len(filter.spans)
	if len(upper) > 0 {
		end = sort.Search(len(filter.spans), func(i int) bool {
			return bytes.Compare(filter.spans[i].minUser, upper) >= 0
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

func (span rangeFilterSpan) covers(userKey []byte) bool {
	if len(userKey) == 0 || len(span.minUser) == 0 || len(span.maxUser) == 0 {
		return false
	}
	return bytes.Compare(userKey, span.minUser) >= 0 && bytes.Compare(userKey, span.maxUser) <= 0
}

func (span rangeFilterSpan) overlaps(lower, upper []byte) bool {
	if len(span.minUser) == 0 || len(span.maxUser) == 0 {
		return true
	}
	if len(lower) > 0 && bytes.Compare(span.maxUser, lower) < 0 {
		return false
	}
	if len(upper) > 0 && bytes.Compare(span.minUser, upper) >= 0 {
		return false
	}
	return true
}

func filterTablesByBounds(tables []*table, lower, upper []byte) []*table {
	lower = guideUserKey(lower)
	upper = guideUserKey(upper)
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
			minUser: guideUserKey(tbl.MinKey()),
			maxUser: guideUserKey(tbl.MaxKey()),
			tbl:     tbl,
		}
		if span.overlaps(lower, upper) {
			out = append(out, tbl)
		}
	}
	return out
}
