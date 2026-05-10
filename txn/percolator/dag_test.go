package percolator

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestDependencyLevelsEmpty verifies the boundary case.
func TestDependencyLevelsEmpty(t *testing.T) {
	require.Empty(t, dependencyLevels(nil))
	require.Empty(t, dependencyLevels([][][]byte{}))
}

// TestDependencyLevelsAllDisjoint: every item touches a unique key, so
// they should all collapse to a single level (maximum parallelism).
func TestDependencyLevelsAllDisjoint(t *testing.T) {
	keys := [][][]byte{
		{[]byte("a")},
		{[]byte("b")},
		{[]byte("c")},
		{[]byte("d")},
	}
	levels := dependencyLevels(keys)
	require.Len(t, levels, 1)
	require.Equal(t, []int{0, 1, 2, 3}, levels[0])
}

// TestDependencyLevelsLinearChain: each item shares a key with the
// previous one; the result must be a chain, one item per level.
func TestDependencyLevelsLinearChain(t *testing.T) {
	keys := [][][]byte{
		{[]byte("a"), []byte("b")},
		{[]byte("b"), []byte("c")},
		{[]byte("c"), []byte("d")},
		{[]byte("d"), []byte("e")},
	}
	levels := dependencyLevels(keys)
	require.Len(t, levels, 4)
	for i, level := range levels {
		require.Equal(t, []int{i}, level)
	}
}

// TestDependencyLevelsConflictWithPrior: an item that conflicts with
// an earlier (non-immediate) item still inherits the right level.
func TestDependencyLevelsConflictWithPrior(t *testing.T) {
	// 0: {a}        level 0
	// 1: {b}        level 0 (no conflict with 0)
	// 2: {c}        level 0
	// 3: {a, c}     conflicts with 0 (level 0) and 2 (level 0) -> level 1
	keys := [][][]byte{
		{[]byte("a")},
		{[]byte("b")},
		{[]byte("c")},
		{[]byte("a"), []byte("c")},
	}
	levels := dependencyLevels(keys)
	require.Len(t, levels, 2)
	require.Equal(t, []int{0, 1, 2}, levels[0])
	require.Equal(t, []int{3}, levels[1])
}

// TestDependencyLevelsRaftOrderInLevel: within a level, indices stay
// in raft order. This is required so that downstream apply preserves
// input order for items that happen to be in the same level.
func TestDependencyLevelsRaftOrderInLevel(t *testing.T) {
	keys := [][][]byte{
		{[]byte("a")}, // 0 -> level 0
		{[]byte("a")}, // 1 -> level 1
		{[]byte("b")}, // 2 -> level 0
		{[]byte("b")}, // 3 -> level 1
		{[]byte("c")}, // 4 -> level 0
	}
	levels := dependencyLevels(keys)
	require.Len(t, levels, 2)
	require.Equal(t, []int{0, 2, 4}, levels[0])
	require.Equal(t, []int{1, 3}, levels[1])
}

// TestDependencyLevelsEmptyKeyIgnored: items with no keys (already
// short-circuited by preparation) should still get a level slot but
// have no influence on others.
func TestDependencyLevelsEmptyKeyIgnored(t *testing.T) {
	keys := [][][]byte{
		{[]byte("a")},
		{},          // ineligible / pre-resolved
		{[]byte("a")}, // conflicts with 0
	}
	levels := dependencyLevels(keys)
	// Item 1 has no keys, gets level 0. Item 2 conflicts with item 0
	// only (item 1 has no key contribution).
	require.Len(t, levels, 2)
	require.Equal(t, []int{0, 1}, levels[0])
	require.Equal(t, []int{2}, levels[1])
}

// TestDependencyLevelsMonotone: the level sequence in input order is
// non-strictly increasing for any pair (i, j) with i < j and a
// conflict. We assert this property over a randomised case to guard
// against future tweaks of the algorithm.
func TestDependencyLevelsMonotone(t *testing.T) {
	keys := [][][]byte{
		{[]byte("k1"), []byte("k2")},
		{[]byte("k3")},
		{[]byte("k1")},
		{[]byte("k4"), []byte("k5")},
		{[]byte("k2"), []byte("k3")},
		{[]byte("k5")},
	}
	levels := dependencyLevels(keys)
	// Compute per-item level
	itemLevel := make([]int, len(keys))
	for l, indices := range levels {
		for _, idx := range indices {
			itemLevel[idx] = l
		}
	}
	// For every conflicting pair (i, j) with i < j, level(j) > level(i)
	for i := range keys {
		for j := i + 1; j < len(keys); j++ {
			if !sharesKey(keys[i], keys[j]) {
				continue
			}
			require.Greaterf(t, itemLevel[j], itemLevel[i],
				"items %d and %d conflict but levels are %d and %d",
				i, j, itemLevel[i], itemLevel[j])
		}
	}
}

func sharesKey(a, b [][]byte) bool {
	set := make(map[string]struct{}, len(a))
	for _, k := range a {
		set[string(k)] = struct{}{}
	}
	for _, k := range b {
		if _, ok := set[string(k)]; ok {
			return true
		}
	}
	return false
}

// TestNonEmptyKeyCount sanity-checks the helper used to decide whether
// scheduling is worth it.
func TestNonEmptyKeyCount(t *testing.T) {
	require.Equal(t, 0, nonEmptyKeyCount(nil))
	require.Equal(t, 0, nonEmptyKeyCount([][][]byte{{}, {}}))
	require.Equal(t, 1, nonEmptyKeyCount([][][]byte{{[]byte("a")}}))
	require.Equal(t, 2, nonEmptyKeyCount([][][]byte{{[]byte("a")}, {}, {[]byte("b")}}))
}
