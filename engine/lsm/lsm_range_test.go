package lsm

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

// TestLSMBoundedRangeIteration exercises bounded merge iteration across
// mutable memtable and immutable memtable (rotated but not yet flushed).
// Invariant: MergeIterator with LowerBound/UpperBound + RangeTombstoneView
// correctly yields only visible, in-bounds keys in both ascending and
// descending order.
func TestLSMBoundedRangeIteration(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	// Write keys: key00 .. key49 -> rotate to immutable memtable
	for i := 0; i < 50; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		v := []byte(fmt.Sprintf("val%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, v, 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Rotate: key00~key49 now in immutable memtable
	lsm.shards[0].lock.Lock()
	_, err := lsm.rotateShardLocked(lsm.shards[0])
	lsm.shards[0].lock.Unlock()
	require.NoError(t, err)

	// Write keys: key50 .. key99 -> mutable memtable
	for i := 50; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		v := []byte(fmt.Sprintf("val%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 101, v, 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Range tombstone [key35, key65) covers keys across both layers
	rtEntry := kv.NewInternalEntry(kv.CFDefault, []byte("key35"), 102, []byte("key65"), kv.BitRangeDelete, 0)
	require.NoError(t, lsm.Set(rtEntry))
	rtEntry.DecrRef()

	rtv := lsm.PinRangeTombstoneView()
	defer rtv.Close()

	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key35"), 100))
	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key49"), 100))
	require.True(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key50"), 101))
	require.False(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key34"), 100))
	require.False(t, rtv.IsKeyCovered(kv.CFDefault, []byte("key65"), 101))

	lower := []byte("key25")
	upper := []byte("key85")

	// Ascending scan
	iters := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { _ = mit.Close() }()

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

	var expectAsc []string
	for i := 25; i < 35; i++ {
		expectAsc = append(expectAsc, fmt.Sprintf("key%02d", i))
	}
	for i := 65; i < 85; i++ {
		expectAsc = append(expectAsc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectAsc, resultsAsc)

	// Descending scan
	itersDesc := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: false})
	mitDesc := NewMergeIterator(itersDesc, true)
	defer func() { _ = mitDesc.Close() }()

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
	for i := 84; i >= 65; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	for i := 34; i >= 25; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectDesc, resultsDesc)
}

// TestLSMBoundedRangeMultiLevel exercises bounded range iteration with data
// physically distributed across MemTable, L0 SSTable, and lower levels (L1/L2)
// via explicit compaction. This proves the merge path reads from real on-disk
// SSTables, not just in-memory structures.
// Invariant: After flush + compaction, bounded iteration still yields correct
// results across all physical storage tiers.
func TestLSMBoundedRangeMultiLevel(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	defer func() { _ = lsm.Close() }()

	maxLevel := lsm.option.MaxLevelNum - 1

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

	// --- Layer 1: Write key00~key29 -> flush to L0 -> compact to L2 (deepest) ---
	for i := 0; i < 30; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 10, []byte("L2"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)
	compactL0To(maxLevel) // now in deepest level

	// --- Layer 2: Write key30~key59 -> flush to L0 -> compact to L1 ---
	for i := 30; i < 60; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 20, []byte("L1"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)
	compactL0To(maxLevel - 1) // one level above deepest

	// --- Layer 3: Write key60~key89 -> flush to L0 (stays in L0) ---
	for i := 60; i < 90; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 30, []byte("L0"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}
	require.NoError(t, lsm.Rotate())
	waitForL0(t, lsm)

	// --- Layer 4: Write key90~key99 -> stays in mutable MemTable ---
	for i := 90; i < 100; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 40, []byte("mem"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	// Bounded scan [key10, key95) ascending - spans all 4 tiers
	lower := []byte("key10")
	upper := []byte("key95")

	iters := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { _ = mit.Close() }()

	var results []string
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
		if len(results) > 0 && results[len(results)-1] == string(userKey) {
			continue
		}
		results = append(results, string(userKey))
	}

	// Expect key10~key94 (85 keys total)
	var expect []string
	for i := 10; i < 95; i++ {
		expect = append(expect, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expect, results)

	// Descending scan over same range
	itersDesc := lsm.NewIterators(&index.Options{LowerBound: lower, UpperBound: upper, IsAsc: false})
	mitDesc := NewMergeIterator(itersDesc, true)
	defer func() { _ = mitDesc.Close() }()

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
		if len(resultsDesc) > 0 && resultsDesc[len(resultsDesc)-1] == string(userKey) {
			continue
		}
		resultsDesc = append(resultsDesc, string(userKey))
	}

	var expectDesc []string
	for i := 94; i >= 10; i-- {
		expectDesc = append(expectDesc, fmt.Sprintf("key%02d", i))
	}
	require.Equal(t, expectDesc, resultsDesc)
}

// TestLSMBoundedRangeSeek verifies Seek behavior on the raw MergeIterator.
// Invariant: Seek positions the iterator at the exact requested internal key
// regardless of bounds (bounds are enforced at the runtime layer).
func TestLSMBoundedRangeSeek(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	for i := 10; i <= 20; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, []byte(fmt.Sprintf("val%02d", i)), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	iters := lsm.NewIterators(&index.Options{LowerBound: []byte("key12"), UpperBound: []byte("key18"), IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { _ = mit.Close() }()

	// Seek before LowerBound
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key10"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key10", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek inside bounds
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key15"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key15", string(splitIterUserKey(t, mit.Item().Entry().Key)))

	// Seek past UpperBound
	mit.Seek(kv.InternalKey(kv.CFDefault, []byte("key19"), kv.MaxVersion))
	require.True(t, mit.Valid())
	require.Equal(t, "key19", string(splitIterUserKey(t, mit.Item().Entry().Key)))
}

// TestLSMBoundedRangeEmptyResult verifies that scanning a range with no
// matching keys yields zero results without panics.
// Invariant: Out-of-data-range bounds produce an empty result set cleanly.
func TestLSMBoundedRangeEmptyResult(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	for i := 0; i < 5; i++ {
		k := []byte(fmt.Sprintf("key%02d", i))
		e := kv.NewInternalEntry(kv.CFDefault, k, 100, []byte("val"), 0, 0)
		require.NoError(t, lsm.Set(e))
		e.DecrRef()
	}

	iters := lsm.NewIterators(&index.Options{LowerBound: []byte("key05"), UpperBound: []byte("key09"), IsAsc: true})
	mit := NewMergeIterator(iters, false)
	defer func() { _ = mit.Close() }()

	count := 0
	for mit.Rewind(); mit.Valid(); mit.Next() {
		userKey := splitIterUserKey(t, mit.Item().Entry().Key)
		if bytes.Compare(userKey, []byte("key09")) >= 0 {
			break
		}
		if bytes.Compare(userKey, []byte("key05")) < 0 {
			continue
		}
		count++
	}
	require.Equal(t, 0, count)
}
