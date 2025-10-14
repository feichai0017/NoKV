package NoKV

import (
	"bytes"
	"sync/atomic"

	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
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

type readTsIterator struct {
	iter   utils.Iterator
	readTs uint64
}

func newReadTsIterator(iter utils.Iterator, readTs uint64) *readTsIterator {
	ri := &readTsIterator{
		iter:   iter,
		readTs: readTs,
	}
	ri.ensureVisible()
	return ri
}

func (ri *readTsIterator) ensureVisible() {
	for ri.iter.Valid() {
		item := ri.iter.Item()
		if item == nil || item.Entry() == nil {
			ri.iter.Next()
			continue
		}
		if utils.ParseTs(item.Entry().Key) > ri.readTs {
			ri.iter.Next()
			continue
		}
		break
	}
}

func (ri *readTsIterator) Next() {
	ri.iter.Next()
	ri.ensureVisible()
}

func (ri *readTsIterator) Valid() bool {
	return ri.iter.Valid()
}

func (ri *readTsIterator) Rewind() {
	ri.iter.Rewind()
	ri.ensureVisible()
}

func (ri *readTsIterator) Item() utils.Item {
	return ri.iter.Item()
}

func (ri *readTsIterator) Close() error {
	return ri.iter.Close()
}

func (ri *readTsIterator) Seek(key []byte) {
	ri.iter.Seek(key)
	ri.ensureVisible()
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

	atomic.AddInt32(&txn.numIterators, 1)

	var iters []utils.Iterator
	if itr := txn.newPendingWritesIterator(opt.Reverse); itr != nil {
		iters = append(iters, itr)
	}

	lsmOpt := &utils.Options{IsAsc: !opt.Reverse}
	if len(opt.Prefix) > 0 {
		lsmOpt.Prefix = opt.Prefix
	}

	for _, iter := range txn.db.lsm.NewIterators(lsmOpt) {
		iters = append(iters, newReadTsIterator(iter, txn.readTs))
	}

	ti := &TxnIterator{
		txn:    txn,
		iitr:   lsm.NewMergeIterator(iters, opt.Reverse),
		opt:    opt,
		readTs: txn.readTs,
	}
	ti.advance()
	return ti
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

func (it *TxnIterator) advance() {
	it.item = nil
	for it.iitr != nil && it.iitr.Valid() {
		item := it.iitr.Item()
		if item == nil || item.Entry() == nil {
			it.iitr.Next()
			continue
		}
		entry := item.Entry()
		userKey := utils.ParseKey(entry.Key)
		version := utils.ParseTs(entry.Key)
		if version > it.readTs {
			it.iitr.Next()
			continue
		}
		if it.opt.SinceTs > 0 && version <= it.opt.SinceTs {
			it.iitr.Next()
			continue
		}
		if len(it.opt.Prefix) > 0 {
			if it.opt.prefixIsKey {
				if !bytes.Equal(userKey, it.opt.Prefix) {
					it.iitr.Next()
					continue
				}
			} else if !bytes.HasPrefix(userKey, it.opt.Prefix) {
				it.iitr.Next()
				continue
			}
		}
		if !it.opt.AllVersions {
			if len(it.lastKey) > 0 && bytes.Equal(it.lastKey, userKey) {
				it.iitr.Next()
				continue
			}
		}
		materialized := it.materializeEntry(entry)
		if materialized == nil {
			it.iitr.Next()
			continue
		}
		it.lastKey = append(it.lastKey[:0], userKey...)
		it.item = &Item{e: materialized}
		it.latestTs = materialized.Version
		return
	}
	it.latestTs = 0
}

func (it *TxnIterator) materializeEntry(entry *utils.Entry) *utils.Entry {
	userKey := utils.ParseKey(entry.Key)
	version := utils.ParseTs(entry.Key)
	dst := &utils.Entry{
		Key:       utils.SafeCopy(nil, userKey),
		Value:     nil,
		Meta:      entry.Meta,
		ExpiresAt: entry.ExpiresAt,
		Version:   version,
	}
	if utils.IsValuePtr(entry) {
		var vp utils.ValuePtr
		vp.Decode(entry.Value)
		val, cb, err := it.txn.db.vlog.read(&vp)
		if err != nil {
			utils.RunCallback(cb)
			return nil
		}
		dst.Value = utils.SafeCopy(nil, val)
		utils.RunCallback(cb)
	} else {
		dst.Value = utils.SafeCopy(nil, entry.Value)
	}
	if isDeletedOrExpired(dst.Meta, dst.ExpiresAt) {
		return nil
	}
	return dst
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *TxnIterator) Item() *Item {
	if it.item == nil {
		return nil
	}
	it.txn.addReadKey(it.item.Entry().Key)
	return it.item
}

// Valid returns false when iteration is done.
func (it *TxnIterator) Valid() bool {
	return it.item != nil
}

// ValidForPrefix returns false when iteration is done
// or when the current key is not prefixed by the specified prefix.
func (it *TxnIterator) ValidForPrefix(prefix []byte) bool {
    return it.Valid() && bytes.HasPrefix(utils.ParseKey(it.item.Entry().Key), prefix)
}

// Close would close the iterator. It is important to call this when you're done with iteration.
func (it *TxnIterator) Close() {
	if it.closed {
		return
	}
	it.closed = true
	if it.iitr != nil {
		_ = it.iitr.Close()
	}
	atomic.AddInt32(&it.txn.numIterators, -1)
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *TxnIterator) Next() {
	if it.iitr == nil {
		it.item = nil
		it.latestTs = 0
		return
	}
	it.iitr.Next()
	it.advance()
}

// Seek would seek to the provided key if present. If absent, it would seek to the next
// smallest key greater than the provided key if iterating in the forward direction.
// Behavior would be reversed if iterating backwards.
func (it *TxnIterator) Seek(key []byte) uint64 {
	it.lastKey = it.lastKey[:0]
	if it.iitr == nil {
		it.item = nil
		it.latestTs = 0
		return it.latestTs
	}
	it.iitr.Rewind()
	it.advance()
	if !it.opt.Reverse {
		for it.Valid() && bytes.Compare(it.item.Entry().Key, key) < 0 {
			it.iitr.Next()
			it.advance()
		}
	} else {
		for it.Valid() && bytes.Compare(it.item.Entry().Key, key) > 0 {
			it.iitr.Next()
			it.advance()
		}
	}
	if !it.Valid() {
		it.latestTs = 0
	}
	return it.latestTs
}

// Rewind would rewind the iterator cursor all the way to zero-th position, which would be the
// smallest key if iterating forward, and largest if iterating backward. It does not keep track of
// whether the cursor started with a Seek().
func (it *TxnIterator) Rewind() {
	it.lastKey = it.lastKey[:0]
	if it.iitr == nil {
		it.item = nil
		it.latestTs = 0
		return
	}
	it.iitr.Rewind()
	it.advance()
}
