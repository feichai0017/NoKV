package lsm

import (
	"bytes"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

type sliceItem struct {
	entry *kv.Entry
}

func (si sliceItem) Entry() *kv.Entry {
	return si.entry
}

type sliceIterator struct {
	entries []*kv.Entry
	idx     int
	reverse bool
}

func splitIterUserKey(t *testing.T, internal []byte) []byte {
	t.Helper()
	_, userKey, _, ok := kv.SplitInternalKey(internal)
	require.True(t, ok)
	return userKey
}

func (it *sliceIterator) Next() {
	if it.reverse {
		it.idx--
		return
	}
	if it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *sliceIterator) Valid() bool {
	if it.reverse {
		return it.idx >= 0 && it.idx < len(it.entries)
	}
	return it.idx < len(it.entries)
}

func (it *sliceIterator) Rewind() {
	if it.reverse {
		it.idx = len(it.entries) - 1
		return
	}
	it.idx = 0
}

func (it *sliceIterator) Item() utils.Item {
	if !it.Valid() {
		return nil
	}
	return sliceItem{entry: it.entries[it.idx]}
}

func (it *sliceIterator) Close() error {
	return nil
}

func (it *sliceIterator) Seek(key []byte) {
	if it.reverse {
		it.idx = -1
		for i := len(it.entries) - 1; i >= 0; i-- {
			if bytes.Compare(it.entries[i].Key, key) <= 0 {
				it.idx = i
				break
			}
		}
		return
	}
	it.idx = sort.Search(len(it.entries), func(i int) bool {
		return bytes.Compare(it.entries[i].Key, key) >= 0
	})
}

func TestMergeIteratorForwardAndReverse(t *testing.T) {
	left := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("a"), 1)},
		{Key: kv.InternalKey(kv.CFDefault, []byte("c"), 1)},
	}}
	right := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("b"), 1)},
		{Key: kv.InternalKey(kv.CFDefault, []byte("c"), 1)},
	}}

	mi := NewMergeIterator([]utils.Iterator{left, right}, false)
	var keys []string
	mi.Rewind()
	for mi.Valid() {
		keys = append(keys, string(splitIterUserKey(t, mi.Item().Entry().Key)))
		mi.Next()
	}
	require.Equal(t, []string{"a", "b", "c"}, keys)

	revLeft := &sliceIterator{entries: left.entries, reverse: true}
	revRight := &sliceIterator{entries: right.entries, reverse: true}
	rev := NewMergeIterator([]utils.Iterator{revLeft, revRight}, true)
	keys = keys[:0]
	rev.Rewind()
	for rev.Valid() {
		keys = append(keys, string(splitIterUserKey(t, rev.Item().Entry().Key)))
		rev.Next()
	}
	require.Equal(t, []string{"c", "b", "a"}, keys)
}

func TestMergeIteratorSeekAndClose(t *testing.T) {
	left := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("a"), 1)},
		{Key: kv.InternalKey(kv.CFDefault, []byte("b"), 1)},
		{Key: kv.InternalKey(kv.CFDefault, []byte("d"), 1)},
	}}
	right := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("c"), 1)},
	}}

	mi := NewMergeIterator([]utils.Iterator{left, right}, false)
	mi.Seek(kv.InternalKey(kv.CFDefault, []byte("c"), 1))
	require.True(t, mi.Valid())
	require.Equal(t, "c", string(splitIterUserKey(t, mi.Item().Entry().Key)))
	require.NoError(t, mi.Close())
}

func TestMergeIteratorEmptyInputIsSafe(t *testing.T) {
	mi := NewMergeIterator(nil, false)
	require.NotNil(t, mi)
	require.NotPanics(t, func() { mi.Rewind() })
	require.NotPanics(t, func() { mi.Seek([]byte("k")) })
	require.NotPanics(t, func() { mi.Next() })
	require.False(t, mi.Valid())
	require.Nil(t, mi.Item())
	require.NoError(t, mi.Close())
}

func TestLSMNewIterators(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entry := utils.BuildEntry()
	require.NoError(t, lsm.Set(entry))

	iters := lsm.NewIterators(&utils.Options{IsAsc: true})
	require.NotEmpty(t, iters)

	levelIters := lsm.levels.iterators(&utils.Options{IsAsc: true})
	for _, it := range levelIters {
		if it == nil {
			continue
		}
		it.Rewind()
		_ = it.Close()
	}

	for _, it := range iters {
		if it == nil {
			continue
		}
		it.Rewind()
		if it.Valid() {
			require.NotNil(t, it.Item())
			it.Next()
		}
		it.Seek(entry.Key)
		_ = it.Close()
	}

}

func TestConcatIteratorSeekAndNext(t *testing.T) {
	tbl := &table{
		minKey: kv.InternalKey(kv.CFDefault, []byte("a"), 1),
		maxKey: kv.InternalKey(kv.CFDefault, []byte("z"), 1),
	}
	entries := []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("b"), 1), Value: []byte("vb")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("d"), 1), Value: []byte("vd")},
	}
	iter := &sliceIterator{entries: entries}

	ci := NewConcatIterator([]*table{tbl}, &utils.Options{IsAsc: true})
	ci.iters[0] = iter
	ci.setIdx(0)

	ci.Rewind()
	if !ci.Valid() {
		t.Fatalf("expected concat iterator to be valid after rewind")
	}
	if ci.Item() == nil {
		t.Fatalf("expected non-nil item")
	}

	ci.Seek(kv.InternalKey(kv.CFDefault, []byte("c"), 1))
	if !ci.Valid() {
		t.Fatalf("expected concat iterator valid after seek")
	}
	got := splitIterUserKey(t, ci.Item().Entry().Key)
	if string(got) != "d" {
		t.Fatalf("expected seek to land on d, got %q", string(got))
	}

	ci.Next()
	if ci.Valid() {
		t.Fatalf("expected iterator to be exhausted")
	}
}

func TestBlockIteratorReverse(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builder := newTableBuiler(&builderOpt)

	// Add test data
	for i := range 10 {
		key := kv.InternalKey(kv.CFDefault, []byte{byte('a' + i)}, 1)
		value := []byte{byte('v'), byte('0' + i)}
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := utils.FileNameSSTable(lsm.option.WorkDir, 1)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	// Test forward iteration
	forwardIter := tbl.NewIterator(&utils.Options{IsAsc: true})
	defer func() { require.NoError(t, forwardIter.Close()) }()

	var forwardKeys []byte
	forwardIter.Rewind()
	for forwardIter.Valid() {
		e := forwardIter.Item().Entry()
		forwardKeys = append(forwardKeys, splitIterUserKey(t, e.Key)[0])
		forwardIter.Next()
	}
	require.Equal(t, "abcdefghij", string(forwardKeys))

	// Test reverse iteration
	reverseIter := tbl.NewIterator(&utils.Options{IsAsc: false})
	defer func() { require.NoError(t, reverseIter.Close()) }()

	var reverseKeys []byte
	reverseIter.Rewind()
	for reverseIter.Valid() {
		e := reverseIter.Item().Entry()
		reverseKeys = append(reverseKeys, splitIterUserKey(t, e.Key)[0])
		reverseIter.Next()
	}
	require.Equal(t, "jihgfedcba", string(reverseKeys))
}

func TestTableIteratorReverseSeek(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builder := newTableBuiler(&builderOpt)

	// Add test data with multiple blocks
	for i := range 20 {
		key := kv.InternalKey(kv.CFDefault, []byte{byte('a' + i)}, 1)
		value := bytes.Repeat([]byte{byte('v'), byte('0' + i%10)}, 24)
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := utils.FileNameSSTable(lsm.option.WorkDir, 2)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	// Test forward seek
	forwardIter := tbl.NewIterator(&utils.Options{IsAsc: true})
	defer func() { require.NoError(t, forwardIter.Close()) }()

	forwardIter.Seek(kv.InternalKey(kv.CFDefault, []byte{'e'}, 1))
	require.True(t, forwardIter.Valid())
	e := forwardIter.Item().Entry()
	require.Equal(t, byte('e'), splitIterUserKey(t, e.Key)[0])

	// Test reverse seek
	reverseIter := tbl.NewIterator(&utils.Options{IsAsc: false})
	defer func() { require.NoError(t, reverseIter.Close()) }()

	reverseIter.Seek(kv.InternalKey(kv.CFDefault, []byte{'e'}, 1))
	require.True(t, reverseIter.Valid())
	e = reverseIter.Item().Entry()
	require.Equal(t, byte('e'), splitIterUserKey(t, e.Key)[0])

	// Continue reverse iteration
	var keys []byte
	for i := 0; i < 5 && reverseIter.Valid(); i++ {
		e := reverseIter.Item().Entry()
		keys = append(keys, splitIterUserKey(t, e.Key)[0])
		reverseIter.Next()
	}
	require.Equal(t, "edcba", string(keys))
}

func TestTableIteratorReverseMultiBlock(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builder := newTableBuiler(&builderOpt)

	// Add enough data to create multiple blocks
	for i := range 30 {
		key := kv.InternalKey(kv.CFDefault, []byte{byte('a' + i)}, 1)
		value := bytes.Repeat([]byte{byte('v')}, 48)
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := utils.FileNameSSTable(lsm.option.WorkDir, 3)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	// Test reverse iteration across multiple blocks
	reverseIter := tbl.NewIterator(&utils.Options{IsAsc: false})
	defer func() { require.NoError(t, reverseIter.Close()) }()

	reverseIter.Rewind()
	require.True(t, reverseIter.Valid())

	var keys []byte
	count := 0
	for reverseIter.Valid() && count < 30 {
		e := reverseIter.Item().Entry()
		keys = append(keys, splitIterUserKey(t, e.Key)[0])
		reverseIter.Next()
		count++
	}

	// Verify we got all keys in reverse order
	require.Equal(t, 30, len(keys))
	for i := range 30 {
		expected := byte('a' + 29 - i)
		require.Equal(t, expected, keys[i], "key at position %d should be %c, got %c", i, expected, keys[i])
	}
}
