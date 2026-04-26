package lsm

import (
	"sort"

	"github.com/feichai0017/NoKV/engine/kv"
)

// l0Sublevel groups L0 tables whose key ranges do not overlap. Within one
// sublevel the tables are sorted ascending by MinKey, so a point read can
// binary-search to a single candidate per sublevel instead of scanning every
// L0 table.
//
// Sublevels are a Phase A read-path optimization: compaction picker and
// trivial move still treat L0 as a single physical level. The sublevel layout
// is rebuilt eagerly inside sortTablesLocked() each time L0 mutates so reads
// always see a consistent snapshot.
type l0Sublevel []*table

// buildL0Sublevels arranges tables into the minimum number of sublevels such
// that each sublevel contains only non-overlapping ranges. The greedy
// placement is order-stable: tables sort by (MinKey asc, fid asc), and each
// table goes into the first sublevel whose tail MaxKey strictly precedes the
// table MinKey. New tables (higher fid) tend to fall into higher sublevels.
//
// Complexity: O(N log N) for the sort plus O(N * S) for placement where S is
// the resulting sublevel count. For typical L0 sizes (10-30 tables, 3-6
// sublevels), this is trivially cheap.
func buildL0Sublevels(tables []*table) []l0Sublevel {
	if len(tables) == 0 {
		return nil
	}

	// Copy to avoid mutating the caller slice and filter nil entries.
	sorted := make([]*table, 0, len(tables))
	for _, t := range tables {
		if t != nil {
			sorted = append(sorted, t)
		}
	}
	if len(sorted) == 0 {
		return nil
	}

	sort.Slice(sorted, func(i, j int) bool {
		if c := kv.CompareBaseKeys(sorted[i].MinKey(), sorted[j].MinKey()); c != 0 {
			return c < 0
		}
		return sorted[i].fid < sorted[j].fid
	})

	sublevels := make([]l0Sublevel, 0, 4)
	for _, t := range sorted {
		placed := false
		for i := range sublevels {
			tail := sublevels[i][len(sublevels[i])-1]
			if kv.CompareBaseKeys(tail.MaxKey(), t.MinKey()) < 0 {
				sublevels[i] = append(sublevels[i], t)
				placed = true
				break
			}
		}
		if !placed {
			sublevels = append(sublevels, l0Sublevel{t})
		}
	}
	return sublevels
}

// candidate returns the at-most-one table within this sublevel whose key
// range covers key, or nil. Sublevel tables are sorted by MinKey so a binary
// search by MinKey followed by a MaxKey check is enough.
func (s l0Sublevel) candidate(key []byte) *table {
	if len(s) == 0 {
		return nil
	}
	idx := sort.Search(len(s), func(i int) bool {
		return kv.CompareBaseKeys(s[i].MinKey(), key) > 0
	})
	if idx == 0 {
		return nil
	}
	candidate := s[idx-1]
	if kv.CompareBaseKeys(key, candidate.MaxKey()) > 0 {
		return nil
	}
	return candidate
}

// l0CandidateTables returns up to one candidate table per sublevel whose
// range covers key. The returned slice contains at most len(sublevels) entries
// and may be empty if no sublevel covers the key.
func l0CandidateTables(sublevels []l0Sublevel, key []byte) []*table {
	if len(sublevels) == 0 {
		return nil
	}
	out := make([]*table, 0, len(sublevels))
	for _, sub := range sublevels {
		if t := sub.candidate(key); t != nil {
			out = append(out, t)
		}
	}
	return out
}
