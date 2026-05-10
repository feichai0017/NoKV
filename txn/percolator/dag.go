package percolator

// dependencyLevels groups a sequence of items into topological waves
// based on their key conflicts. Items in the same level are mutually
// non-conflicting and can be processed in parallel; items in different
// levels must be processed level-by-level in ascending order to
// preserve raft-log ordering for conflicting operations.
//
// The algorithm is intentionally simple: walk items in raft order,
// tracking the most recent level that touched each key. A new item's
// level is one greater than the highest level among any of its keys
// that have been seen, or 0 if none have. This is conservative:
// it produces a correct topological order but does not always
// achieve maximum parallelism (e.g. an item that conflicts only
// with the previous item but not with two-back is forced to wait
// for the chain rather than slot in earlier).
//
// Complexity: O(N * K) where N is item count and K is average
// keys-per-item. For typical fsmeta batches (N=64, K=2-4) this is
// in the low microseconds, dwarfed by the per-item LSM read cost
// the parallel layer is amortizing.
//
// Returned slice has one entry per level, each holding the indices
// of the original items that belong to that level. Within a level,
// indices appear in raft order. Levels themselves are returned in
// ascending order.
func dependencyLevels(itemKeys [][][]byte) [][]int {
	if len(itemKeys) == 0 {
		return nil
	}
	// nodeLevel[i] = the topological level assigned to item i
	nodeLevel := make([]int, len(itemKeys))
	// keyLevel maps the byte key to the most recent level that touched it
	keyLevel := make(map[string]int, len(itemKeys)*2)
	maxLevel := 0
	for i, keys := range itemKeys {
		level := 0
		for _, k := range keys {
			if len(k) == 0 {
				continue
			}
			if l, ok := keyLevel[string(k)]; ok && l+1 > level {
				level = l + 1
			}
		}
		nodeLevel[i] = level
		if level > maxLevel {
			maxLevel = level
		}
		for _, k := range keys {
			if len(k) == 0 {
				continue
			}
			keyLevel[string(k)] = level
		}
	}
	levels := make([][]int, maxLevel+1)
	for i, l := range nodeLevel {
		levels[l] = append(levels[l], i)
	}
	return levels
}

// nonEmptyKeyCount counts the items in a slice that have a non-empty
// key set. Items with no keys (e.g. ineligible requests that already
// produced a result during preparation) do not need scheduling.
func nonEmptyKeyCount(itemKeys [][][]byte) int {
	n := 0
	for _, keys := range itemKeys {
		if len(keys) > 0 {
			n++
		}
	}
	return n
}
