// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package iterator

import (
	"bytes"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/stretchr/testify/require"
)

type sliceItem struct {
	entry *kv.Entry
}

func (si sliceItem) Entry() *kv.Entry { return si.entry }

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

func (it *sliceIterator) Item() index.Item {
	if !it.Valid() {
		return nil
	}
	return sliceItem{entry: it.entries[it.idx]}
}

func (it *sliceIterator) Close() error { return nil }

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

	mi := NewMergeIterator([]index.Iterator{left, right}, false)
	var keys []string
	mi.Rewind()
	for mi.Valid() {
		keys = append(keys, string(splitIterUserKey(t, mi.Item().Entry().Key)))
		mi.Next()
	}
	require.Equal(t, []string{"a", "b", "c"}, keys)

	revLeft := &sliceIterator{entries: left.entries, reverse: true}
	revRight := &sliceIterator{entries: right.entries, reverse: true}
	rev := NewMergeIterator([]index.Iterator{revLeft, revRight}, true)
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

	mi := NewMergeIterator([]index.Iterator{left, right}, false)
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
