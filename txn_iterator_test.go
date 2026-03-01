package NoKV

import (
	"bytes"
	"sort"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

type stubItem struct {
	entry *kv.Entry
}

func (si stubItem) Entry() *kv.Entry {
	return si.entry
}

type stubIterator struct {
	entries     []*kv.Entry
	idx         int
	reverse     bool
	nextCalls   int
	rewindCalls int
	seekCalls   int
}

func (it *stubIterator) Next() {
	it.nextCalls++
	if it.reverse {
		if it.idx >= 0 {
			it.idx--
		}
		return
	}
	if it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *stubIterator) Valid() bool {
	if it.reverse {
		return it.idx >= 0 && it.idx < len(it.entries)
	}
	return it.idx < len(it.entries)
}

func (it *stubIterator) Rewind() {
	it.rewindCalls++
	if it.reverse {
		it.idx = len(it.entries) - 1
		return
	}
	it.idx = 0
}

func (it *stubIterator) Item() utils.Item {
	if !it.Valid() {
		return nil
	}
	return stubItem{entry: it.entries[it.idx]}
}

func (it *stubIterator) Close() error {
	return nil
}

func (it *stubIterator) Seek(key []byte) {
	it.seekCalls++
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

func TestReadTsIteratorFiltersAndSeek(t *testing.T) {
	entries := []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("a"), 10)},
		{Key: kv.InternalKey(kv.CFDefault, []byte("a"), 6)},
		{Key: kv.InternalKey(kv.CFDefault, []byte("b"), 5)},
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	base := &stubIterator{entries: entries}
	ri := newReadTsIterator(base, 7)

	require.True(t, ri.Valid())
	require.Equal(t, uint64(6), kv.ParseTs(ri.Item().Entry().Key))

	ri.Seek(kv.InternalKey(kv.CFDefault, []byte("a"), 10))
	require.True(t, ri.Valid())
	require.Equal(t, uint64(6), kv.ParseTs(ri.Item().Entry().Key))

	ri.Next()
	require.True(t, ri.Valid())
	require.Equal(t, uint64(5), kv.ParseTs(ri.Item().Entry().Key))

	ri.Rewind()
	require.True(t, ri.Valid())
	require.Equal(t, uint64(6), kv.ParseTs(ri.Item().Entry().Key))

	require.NoError(t, ri.Close())
}

func TestTxnIteratorSeekForwardUsesUnderlyingSeek(t *testing.T) {
	entries := []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("a"), 5), Value: []byte("va")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("b"), 5), Value: []byte("vb")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("c"), 5), Value: []byte("vc")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("d"), 5), Value: []byte("vd")},
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	base := &stubIterator{entries: entries}
	it := &TxnIterator{
		iitr:   base,
		readTs: 10,
		opt:    IteratorOptions{},
	}
	it.item.e = &it.entry

	latestTs := it.Seek([]byte("c"))

	require.True(t, it.Valid())
	require.Equal(t, []byte("c"), it.Item().Entry().Key)
	require.Equal(t, uint64(5), latestTs)
	require.Equal(t, 1, base.seekCalls)
	require.Equal(t, 0, base.rewindCalls)
	require.Equal(t, 0, base.nextCalls)
}

func TestTxnIteratorSeekReverseUsesUnderlyingSeek(t *testing.T) {
	entries := []*kv.Entry{
		{Key: kv.InternalKey(kv.CFDefault, []byte("a"), 5), Value: []byte("va")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("b"), 5), Value: []byte("vb")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("c"), 5), Value: []byte("vc")},
		{Key: kv.InternalKey(kv.CFDefault, []byte("d"), 5), Value: []byte("vd")},
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].Key, entries[j].Key) < 0
	})

	base := &stubIterator{entries: entries, reverse: true}
	it := &TxnIterator{
		iitr:   base,
		readTs: 10,
		opt:    IteratorOptions{Reverse: true},
	}
	it.item.e = &it.entry

	latestTs := it.Seek([]byte("c"))

	require.True(t, it.Valid())
	require.Equal(t, []byte("c"), it.Item().Entry().Key)
	require.Equal(t, uint64(5), latestTs)
	require.Equal(t, 1, base.seekCalls)
	require.Equal(t, 0, base.rewindCalls)
	require.Equal(t, 0, base.nextCalls)
}

func TestPendingWritesIterator(t *testing.T) {
	entryA := kv.NewEntry([]byte("a"), []byte("v1"))
	entryB := kv.NewEntry([]byte("b"), []byte("v2"))
	txn := &Txn{
		update:        true,
		readTs:        42,
		pendingWrites: map[string]*kv.Entry{"a": entryA, "b": entryB},
	}

	itr := txn.newPendingWritesIterator(false)
	require.NotNil(t, itr)
	itr.Rewind()
	require.True(t, itr.Valid())
	require.NotNil(t, itr.Item())
	_, key, ts := kv.SplitInternalKey(itr.Key())
	require.Equal(t, []byte("a"), key)
	require.Equal(t, txn.readTs, ts)
	val := itr.Value()
	require.Equal(t, []byte("v1"), val.Value)
	require.Equal(t, txn.readTs, val.Version)

	itr.Next()
	_, key, _ = kv.SplitInternalKey(itr.Key())
	require.Equal(t, []byte("b"), key)

	itr.Seek(kv.InternalKey(kv.CFDefault, []byte("b"), txn.readTs))
	require.True(t, itr.Valid())
	_, key, _ = kv.SplitInternalKey(itr.Key())
	require.Equal(t, []byte("b"), key)

	require.NoError(t, itr.Close())

	rev := txn.newPendingWritesIterator(true)
	require.NotNil(t, rev)
	rev.Rewind()
	require.True(t, rev.Valid())
	_, key, _ = kv.SplitInternalKey(rev.Key())
	require.Equal(t, []byte("b"), key)

	empty := &Txn{update: false}
	require.Nil(t, empty.newPendingWritesIterator(false))
}

func TestTxnDeleteAndReadTs(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.SetEntry(kv.NewEntry([]byte("k"), []byte("v")))
		}))

		view := db.NewTransaction(false)
		require.Equal(t, view.readTs, view.ReadTs())
		view.Discard()

		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.Delete([]byte("k"))
		}))
		err := db.View(func(txn *Txn) error {
			_, err := txn.Get([]byte("k"))
			return err
		})
		require.ErrorIs(t, err, utils.ErrKeyNotFound)
	})
}

func TestTxnKeyIteratorAndValidForPrefix(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		txn := db.NewTransaction(true)
		defer txn.Discard()
		require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("apple"), []byte("v1"))))
		require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("apricot"), []byte("v2"))))
		require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("banana"), []byte("v3"))))

		iter := txn.NewKeyIterator([]byte("apple"), IteratorOptions{})
		defer iter.Close()
		iter.Rewind()
		require.True(t, iter.Valid())
		item := iter.Item()
		require.NotNil(t, item)
		require.Equal(t, "apple", string(kv.ParseKey(item.Entry().Key)))
		require.True(t, iter.ValidForPrefix([]byte("app")))
		require.False(t, iter.ValidForPrefix([]byte("ban")))

		require.Panics(t, func() {
			txn.NewKeyIterator([]byte("x"), IteratorOptions{Prefix: []byte("x")})
		})
	})
}

func TestExceedsSize(t *testing.T) {
	key := bytes.Repeat([]byte("k"), maxKeySize+1)
	require.Error(t, exceedsSize("Key", maxKeySize, key))
}

func TestTxnIteratorBounds(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k1"), []byte("v1"))))
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k3"), []byte("v3"))))
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k5"), []byte("v5"))))
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k7"), []byte("v7"))))
			return nil
		}))

		t.Run("Forward with bounds", func(t *testing.T) {
			txn := db.NewTransaction(false)
			defer txn.Discard()

			opt := IteratorOptions{
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := txn.NewIterator(opt)
			defer iter.Close()

			iter.Rewind()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k3"), iter.Item().Entry().Key)

			iter.Next()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k5"), iter.Item().Entry().Key)

			iter.Next()
			require.False(t, iter.Valid(), "Should not encounter k7 due to exclusive UpperBound")
		})

		t.Run("Reverse with bounds", func(t *testing.T) {
			txn := db.NewTransaction(false)
			defer txn.Discard()

			opt := IteratorOptions{
				Reverse:    true,
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := txn.NewIterator(opt)
			defer iter.Close()

			iter.Rewind()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k5"), iter.Item().Entry().Key)

			iter.Next()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k3"), iter.Item().Entry().Key)

			iter.Next()
			require.False(t, iter.Valid(), "Should not encounter k1 due to inclusive LowerBound in reverse direction")
		})

		t.Run("Seek bounds enforcement", func(t *testing.T) {
			txn := db.NewTransaction(false)
			defer txn.Discard()

			opt := IteratorOptions{
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := txn.NewIterator(opt)
			defer iter.Close()

			// Out of upper bound seek -> should invalidate
			iter.Seek([]byte("k9"))
			require.False(t, iter.Valid())

			// Out of lower bound seek -> should clamp to lower bound
			iter.Seek([]byte("k1"))
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k3"), iter.Item().Entry().Key)
		})

		t.Run("Reverse Seek bounds enforcement", func(t *testing.T) {
			txn := db.NewTransaction(false)
			defer txn.Discard()

			opt := IteratorOptions{
				Reverse:    true,
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := txn.NewIterator(opt)
			defer iter.Close()

			// Out of lower bound seek -> should invalidate
			iter.Seek([]byte("k1"))
			require.False(t, iter.Valid())

			// Out of upper bound seek -> should clamp to upper bound
			iter.Seek([]byte("k9"))
			require.True(t, iter.Valid())
			// Reverse Seek to k9 clamps to k7. However, since UpperBound is exclusive, it should yield k5,
			// or if the iterator is positioned at k7 (invalid), the .advance() will skip to k5!
			require.Equal(t, []byte("k5"), iter.Item().Entry().Key)
		})
	})
}

func TestDBIteratorBounds(t *testing.T) {
	runNoKVTest(t, nil, func(t *testing.T, db *DB) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k1"), []byte("v1"))))
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k3"), []byte("v3"))))
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k5"), []byte("v5"))))
			require.NoError(t, txn.SetEntry(kv.NewEntry([]byte("k7"), []byte("v7"))))
			return nil
		}))

		t.Run("Forward with bounds", func(t *testing.T) {
			opt := &utils.Options{
				IsAsc:      true,
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := db.NewIterator(opt)
			defer func() { require.NoError(t, iter.Close()) }()

			iter.Rewind()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k3"), kv.ParseKey(iter.Item().Entry().Key))

			iter.Next()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k5"), kv.ParseKey(iter.Item().Entry().Key))

			iter.Next()
			require.False(t, iter.Valid(), "Should not encounter k7 due to exclusive UpperBound")
		})

		t.Run("Reverse with bounds", func(t *testing.T) {
			opt := &utils.Options{
				IsAsc:      false,
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := db.NewIterator(opt)
			defer func() { require.NoError(t, iter.Close()) }()

			iter.Rewind()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k5"), kv.ParseKey(iter.Item().Entry().Key))

			iter.Next()
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k3"), kv.ParseKey(iter.Item().Entry().Key))

			iter.Next()
			require.False(t, iter.Valid(), "Should not encounter k1 due to inclusive LowerBound in reverse direction")
		})

		t.Run("Seek bounds enforcement", func(t *testing.T) {
			opt := &utils.Options{
				IsAsc:      true,
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := db.NewIterator(opt)
			defer func() { require.NoError(t, iter.Close()) }()

			// Out of upper bound seek -> should invalidate
			iter.Seek([]byte("k9"))
			require.False(t, iter.Valid())

			// Out of lower bound seek -> should clamp to lower bound
			iter.Seek([]byte("k1"))
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k3"), kv.ParseKey(iter.Item().Entry().Key))
		})

		t.Run("Reverse Seek bounds enforcement", func(t *testing.T) {
			opt := &utils.Options{
				IsAsc:      false,
				LowerBound: []byte("k3"),
				UpperBound: []byte("k7"),
			}
			iter := db.NewIterator(opt)
			defer iter.Close()

			// Out of lower bound seek -> should invalidate
			iter.Seek([]byte("k1"))
			require.False(t, iter.Valid())

			// Out of upper bound seek -> should clamp to upper bound
			iter.Seek([]byte("k9"))
			require.True(t, iter.Valid())
			require.Equal(t, []byte("k5"), kv.ParseKey(iter.Item().Entry().Key))
		})
	})
}
