package NoKV

import (
	"bytes"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
	"math"
	"sync/atomic"
)

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type TxnIterator struct {
	iitr   utils.Iterator
	txn    *Txn
	readTs uint64

	opt  IteratorOptions
	item *Item

	lastKey []byte // Used to skip over multiple versions of the same key.

	closed bool

	latestTs uint64
}

type IteratorOptions struct {
	Reverse        bool // Direction of iteration. False is forward, true is backward.
	AllVersions    bool // Fetch all valid versions of the same key.
	InternalAccess bool // Used to allow internal access to keys.

	// The following option is used to narrow down the SSTables that iterator
	// picks up. If Prefix is specified, only tables which could have this
	// prefix are picked based on their range of keys.
	prefixIsKey bool   // If set, use the prefix for bloom filter lookup.
	Prefix      []byte // Only iterate over this given prefix.
	SinceTs     uint64 // Only read data that has version > SinceTs.
}

// NewIterator 方法会生成一个新的事务迭代器。
// 在 Option 中，可以设置只迭代 Key，或者迭代 Key-Value

func (txn *Txn) NewIterator(opt IteratorOptions) *TxnIterator {
	if txn.discarded {
		panic("Transaction has already been discarded")
	}
	if txn.db.IsClosed() {
		panic(utils.ErrDBClosed.Error())
	}

	// Keep track of the number of active iterators.
	atomic.AddInt32(&txn.numIterators, 1)

	var iters []utils.Iterator
	if itr := txn.newPendingWritesIterator(opt.Reverse); itr != nil {
		iters = append(iters, itr)
	}

	for _, iter := range txn.db.lsm.NewIterators(nil) {
		iters = append(iters, iter)
	}

	res := &TxnIterator{
		txn:    txn,
		iitr:   lsm.NewMergeIterator(iters, opt.Reverse),
		opt:    opt,
		readTs: txn.readTs,
	}
	return res
}

// NewKeyIterator is just like NewIterator, but allows the user to iterate over all versions of a
// single key. Internally, it sets the Prefix option in provided opt, and uses that prefix to
// additionally run bloom filter lookups before picking tables from the LSM tree.
func (txn *Txn) NewKeyIterator(key []byte, opt IteratorOptions) *TxnIterator {
	if len(opt.Prefix) > 0 {
		panic("opt.Prefix should be nil for NewKeyIterator.")
	}
	opt.Prefix = key // This key must be without the timestamp.
	opt.prefixIsKey = true
	opt.AllVersions = true
	return txn.NewIterator(opt)
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *TxnIterator) Item() *Item {
	tx := it.txn
	tx.addReadKey(it.item.Entry().Key)
	return it.item
}

// Valid returns false when iteration is done.
func (it *TxnIterator) Valid() bool {
	if it.item == nil {
		return false
	}
	if it.opt.prefixIsKey {
		return bytes.Equal(it.item.Entry().Key, it.opt.Prefix)
	}
	return bytes.HasPrefix(it.item.Entry().Key, it.opt.Prefix)
}

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *TxnIterator) ValidForPrefix(prefix []byte) bool {
	return it.Valid() && bytes.HasPrefix(it.item.Entry().Key, prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *TxnIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	if it.iitr == nil {
		atomic.AddInt32(&it.txn.numIterators, -1)
		return
	}

	it.iitr.Close()

	// TODO: We could handle this error.
	_ = it.txn.db.vlog.decrIteratorCount()
	atomic.AddInt32(&it.txn.numIterators, -1)
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *TxnIterator) Next() {
	if it.iitr == nil {
		return
	}
	it.iitr.Next()
}

// Seek would seek to the provided key if present. If absent, it would seek to the next
// smallest key greater than the provided key if iterating in the forward direction.
// Behavior would be reversed if iterating backwards.
func (it *TxnIterator) Seek(key []byte) uint64 {
	if it.iitr == nil {
		return it.latestTs
	}
	if len(key) > 0 {
		it.txn.addReadKey(key)
	}

	it.lastKey = it.lastKey[:0]
	if len(key) == 0 {
		key = it.opt.Prefix
	}
	if len(key) == 0 {
		it.iitr.Rewind()
		return it.latestTs
	}

	if !it.opt.Reverse {
		// Using maxUint64 instead of it.readTs because we want seek to return latestTs of the key.
		// All the keys with ts > readTs will be discarded for iteration by the prefetch function.
		key = utils.KeyWithTs(key, math.MaxUint64)
	} else {
		key = utils.KeyWithTs(key, 0)
	}
	it.iitr.Seek(key)

	return it.latestTs
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *TxnIterator) Rewind() {
	it.Seek(nil)
}