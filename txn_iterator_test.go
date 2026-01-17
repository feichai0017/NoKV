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
	entries []*kv.Entry
	idx     int
}

func (it *stubIterator) Next() {
	if it.idx < len(it.entries) {
		it.idx++
	}
}

func (it *stubIterator) Valid() bool {
	return it.idx < len(it.entries)
}

func (it *stubIterator) Rewind() {
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
