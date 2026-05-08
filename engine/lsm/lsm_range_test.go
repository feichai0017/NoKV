package lsm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

// TestLSMBoundedRangeMultiLevel exercises bounded merge iteration with data
// interleaved across physical tiers: even keys in the deepest level, odd keys
// one level above, newer versions of a subset in L0, and the newest writes in
// the mutable MemTable. A range tombstone hides a slice across multiple tiers.
//
// Invariants protected:
//   - MergeIterator yields correct ascending/descending order when keys from
//     different tiers interleave (even/odd alternation).
//   - LowerBound/UpperBound correctly restrict the visible key range.
//   - Range tombstones hide covered keys regardless of which tier they reside in.
//   - Newer versions in upper tiers shadow older versions in lower tiers.
func TestLSMBoundedRangeMultiLevel(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	defer func() { require.NoError(t, lsm.Close()) }()

	maxLevel := lsm.option.MaxLevelNum - 1

	// compactL0To forces a compaction from L0 to the specified level.
	// This gives us deterministic control over which physical tier data lands in.
	compactL0To := func(level int) {
		t.Helper()
		cd := buildCompactDef(lsm, 0, 0, level)
		if ok := lsm.levels.fillTables(cd); !ok {
			t.Fatalf("expected L0->L%d compaction plan", level)
		}
		if err := lsm.levels.runCompactDef(0, 0, *cd); err != nil {
			t.Fatalf("runCompactDef L0->L%d: %v", level, err)
		}
		require.Nil(t, lsm.levels.compactState.Delete(cd.stateEntry()))
	}

	// --- Even keys (key00, key02, ..., key98) @ version 10 -> maxLevel ---
	for i := 0; i < 100; i += 2 {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 10, []byte("even"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)
	compactL0To(maxLevel)

	// --- Odd keys (key01, key03, ..., key99) @ version 20 -> maxLevel-1 ---
	for i := 1; i < 100; i += 2 {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 20, []byte("odd"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)
	compactL0To(maxLevel - 1)

	// --- Newer versions of key50~key59 @ version 30 -> L0 ---
	for i := 50; i < 60; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 30, []byte("l0-new"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)

	// --- Newest versions of key90~key99 @ version 40 -> mutable MemTable ---
	for i := 90; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 40, []byte("mem-new"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Range tombstone [key30, key50) @ version 50.
	// Hides key30~key49 across both maxLevel (even) and maxLevel-1 (odd).
	rtEntry := kv.NewInternalEntry(kv.CFDefault, []byte("key30"), 50, []byte("key50"), kv.BitRangeDelete, 0)
	require.NoError(t, lsm.Set(rtEntry))
	rtEntry.DecrRef()

	rtv := lsm.PinRangeTombstoneView()
	defer rtv.Close()

	// Verify tombstone coverage boundaries.
	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key30"), 10))
	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key49"), 20))
	require.False(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key29"), 10))
	require.False(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key50"), 30))

	// Bounded ascending scan [key10, key95).
	lower := []byte("key10")
	upper := []byte("key95")

	iters := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { require.NoError(t, mit.Close()) }()

	var resultsAsc []string
	for mit.Rewind(); mit.Valid(); mit.Next() {
		e := mit.Item().Entry()
		userKey := splitIterUserKey(t, e.Key)
		if bytes.Compare(userKey, upper) >= 0 {
			break
		}
		if bytes.Compare(userKey, lower) < 0 {
			continue
		}
		if e.IsRangeDelete() {
			continue
		}
		if rtv.IsKeyCovered(kv.CFDefault, userKey, e.Version) {
			continue
		}
		if len(resultsAsc) > 0 && resultsAsc[len(resultsAsc)-1] == string(userKey) {
			continue
		}
		resultsAsc = append(resultsAsc, string(userKey))
	}

	// Expected: key10~key29 (interleaved even/odd), key30~key49 hidden,
	// key50~key94 visible (with newer versions from L0/Mem shadowing old ones).
	var expectAsc []string
	for i := 10; i < 30; i++ {
		expectAsc = append(expectAsc, fmt.Sprintf("key%02d", i))
	}
	for i := 50; i < 95; i++ {
		expectAsc = append(expectAsc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectAsc, resultsAsc)

	// Bounded descending scan [key10, key95).
	itersDesc := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: false})
	mitDesc := NewMergeIterator(itersDesc, true)
	defer func() { require.NoError(t, mitDesc.Close()) }()

	var resultsDesc []string
	for mitDesc.Rewind(); mitDesc.Valid(); mitDesc.Next() {
		e := mitDesc.Item().Entry()
		userKey := splitIterUserKey(t, e.Key)
		if bytes.Compare(userKey, lower) < 0 {
			break
		}
		if bytes.Compare(userKey, upper) >= 0 {
			continue
		}
		if e.IsRangeDelete() {
			continue
		}
		if rtv.IsKeyCovered(kv.CFDefault, userKey, e.Version) {
			continue
		}
		if len(resultsDesc) > 0 && resultsDesc[len(resultsDesc)-1] == string(userKey) {
			continue
		}
		resultsDesc = append(resultsDesc, string(userKey))
	}

	var expectDesc []string
	for i := 94; i >= 50; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	for i := 29; i >= 10; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectDesc, resultsDesc)
}

// TestLSMBoundedRangeSeek verifies Seek behavior on the raw MergeIterator.
// Invariant: Seek positions the iterator at the first element >= target key.
// Bounds enforcement is at the runtime/DBIterator layer, not the raw iterator.
func TestLSMBoundedRangeSeek(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { require.NoError(t, lsm.Close()) }()

	for i := 10; i <= 20; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, []byte(fmt.Sprintf("val%02d", i)), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	iters := lsm.NewIterators(&index.Options{LowerBound: []byte("key12"), UpperBound: []byte("key18"), IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { require.NoError(t, mit.Close()) }()

	// Seek below data range — lands on first key (>= semantic).
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key09"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key10", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek inside bounds.
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key15"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key15", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek to last key.
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key20"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key20", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek beyond all data — iterator exhausted.
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key21"), kv.MaxVersion))
	require.False(t, mit.Valid())
}

// TestLSMBoundedRangeEmptyResult verifies that scanning a range with no
// matching keys yields zero results without panics.
// Invariant: Out-of-data-range bounds produce an empty result set cleanly.
func TestLSMBoundedRangeEmptyResult(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { require.NoError(t, lsm.Close()) }()

	const n = 9
	for i := 0; i < n; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, []byte("val"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Data is key00~key08. Test empty ranges at different positions.
	emptyRanges := []struct {
		lower []byte
		upper []byte
	}{
		{[]byte("key09"), []byte("key19")}, // after all data
		{[]byte("key03"), []byte("key03")}, // zero-width range
	}

	for _, r := range emptyRanges {
		iters := lsm.NewIterators(&index.Options{LowerBound: r.lower, UpperBound: r.upper, IsAsc: true})
		mit := NewMergeIterator(iters, false)

		count := 0
		for mit.Rewind(); mit.Valid(); mit.Next() {
			userKey := splitIterUserKey(t, mit.Item().Entry().Key)
			if bytes.Compare(userKey, r.lower) >= 0 &&
				bytes.Compare(userKey, r.upper) < 0 {
				count++
			}
		}
		require.Equal(t, 0, count, "range [%s, %s) should be empty", r.lower, r.upper)
		require.NoError(t, mit.Close())
	}
}
