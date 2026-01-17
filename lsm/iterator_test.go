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
		{Key: kv.KeyWithTs([]byte("a"), 1)},
		{Key: kv.KeyWithTs([]byte("c"), 1)},
	}}
	right := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.KeyWithTs([]byte("b"), 1)},
		{Key: kv.KeyWithTs([]byte("c"), 1)},
	}}

	mi := NewMergeIterator([]utils.Iterator{left, right}, false)
	var keys []string
	mi.Rewind()
	for mi.Valid() {
		keys = append(keys, string(kv.ParseKey(mi.Item().Entry().Key)))
		mi.Next()
	}
	require.Equal(t, []string{"a", "b", "c"}, keys)

	revLeft := &sliceIterator{entries: left.entries, reverse: true}
	revRight := &sliceIterator{entries: right.entries, reverse: true}
	rev := NewMergeIterator([]utils.Iterator{revLeft, revRight}, true)
	keys = keys[:0]
	rev.Rewind()
	for rev.Valid() {
		keys = append(keys, string(kv.ParseKey(rev.Item().Entry().Key)))
		rev.Next()
	}
	require.Equal(t, []string{"c", "b", "a"}, keys)
}

func TestMergeIteratorSeekAndClose(t *testing.T) {
	left := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.KeyWithTs([]byte("a"), 1)},
		{Key: kv.KeyWithTs([]byte("b"), 1)},
		{Key: kv.KeyWithTs([]byte("d"), 1)},
	}}
	right := &sliceIterator{entries: []*kv.Entry{
		{Key: kv.KeyWithTs([]byte("c"), 1)},
	}}

	mi := NewMergeIterator([]utils.Iterator{left, right}, false)
	mi.Seek(kv.KeyWithTs([]byte("c"), 1))
	require.True(t, mi.Valid())
	require.Equal(t, "c", string(kv.ParseKey(mi.Item().Entry().Key)))
	require.NoError(t, mi.Close())
}

func TestLSMNewIterators(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer lsm.Close()

	entry := utils.BuildEntry()
	require.NoError(t, lsm.Set(entry))

	iters := lsm.NewIterators(&utils.Options{IsAsc: true})
	require.NotEmpty(t, iters)

	levelIters := lsm.levels.NewIterators(&utils.Options{IsAsc: true})
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

	wrapped := &Iterator{iters: iters}
	wrapped.Rewind()
	_ = wrapped.Item()
	wrapped.Next()
	_ = wrapped.Valid()
	wrapped.Seek(entry.Key)
	require.NoError(t, wrapped.Close())
}
