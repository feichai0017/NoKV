package NoKV

import (
	"bytes"
	"sync/atomic"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
)

// Iterator helps iterating over the KV pairs in a lexicographically sorted order.
type TxnIterator struct {
	iitr   utils.Iterator
	txn    *Txn
	readTs uint64

	opt      IteratorOptions
	item     Item
	entry    kv.Entry
	valueBuf []byte

	lastKey []byte // Used to skip over multiple versions of the same key.

	closed bool

	latestTs uint64

	pool  *iteratorPool
	ctx   *iteratorContext
	valid bool
}

// IteratorOptions defines an exported API type.
type IteratorOptions struct {
	Reverse        bool // Direction of iteration. False is forward, true is backward.
	AllVersions    bool // Fetch all valid versions of the same key.
	InternalAccess bool // Used to allow internal access to keys.
	KeyOnly        bool // Avoid eager value materialisation.

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
		if kv.ParseTs(item.Entry().Key) > ri.readTs {
			ri.iter.Next()
			continue
		}
		break
	}
}

// Next is part of the exported receiver API.
func (ri *readTsIterator) Next() {
	ri.iter.Next()
	ri.ensureVisible()
}

// Valid is part of the exported receiver API.
func (ri *readTsIterator) Valid() bool {
	return ri.iter.Valid()
}

// Rewind is part of the exported receiver API.
func (ri *readTsIterator) Rewind() {
	ri.iter.Rewind()
	ri.ensureVisible()
}

// Item is part of the exported receiver API.
func (ri *readTsIterator) Item() utils.Item {
	return ri.iter.Item()
}

// Close is part of the exported receiver API.
func (ri *readTsIterator) Close() error {
	return ri.iter.Close()
}

// Seek is part of the exported receiver API.
func (ri *readTsIterator) Seek(key []byte) {
	ri.iter.Seek(key)
	ri.ensureVisible()
}

// NewIterator creates a transactional iterator.
// Options can request key-only iteration or full key/value iteration.
func (txn *Txn) NewIterator(opt IteratorOptions) *TxnIterator {
	if txn.discarded {
		panic("Transaction has already been discarded")
	}
	if txn.db.IsClosed() {
		panic(utils.ErrDBClosed.Error())
	}

	atomic.AddInt32(&txn.numIterators, 1)

	var (
		ctx   *iteratorContext
		iters []utils.Iterator
	)
	if txn.db != nil && txn.db.iterPool != nil {
		ctx = txn.db.iterPool.get()
		iters = ctx.iters[:0]
	}
	if iters == nil {
		iters = make([]utils.Iterator, 0)
	}
	if itr := txn.newPendingWritesIterator(opt.Reverse); itr != nil {
		iters = append(iters, itr)
	}

	lsmOpt := &utils.Options{IsAsc: !opt.Reverse}
	if len(opt.Prefix) > 0 {
		lsmOpt.Prefix = opt.Prefix
	}
	lsmOpt.OnlyUseKey = opt.KeyOnly

	for _, iter := range txn.db.lsm.NewIterators(lsmOpt) {
		iters = append(iters, newReadTsIterator(iter, txn.readTs))
	}
	if ctx != nil {
		ctx.iters = iters
	}

	ti := &TxnIterator{
		txn:    txn,
		iitr:   lsm.NewMergeIterator(iters, opt.Reverse),
		opt:    opt,
		readTs: txn.readTs,
		pool:   txn.db.iterPool,
		ctx:    ctx,
	}
	ti.item.e = &ti.entry
	if txn.db != nil {
		ti.item.vlog = txn.db.vlog
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
	it.valid = false
	for it.iitr != nil && it.iitr.Valid() {
		item := it.iitr.Item()
		if item == nil || item.Entry() == nil {
			it.iitr.Next()
			continue
		}
		it.item.valueBuf = it.item.valueBuf[:0]
		entry := item.Entry()
		baseKey := kv.ParseKey(entry.Key)
		cf, userKey, _ := kv.DecodeKeyCF(baseKey)
		version := kv.ParseTs(entry.Key)
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
		if !it.materializeEntry(entry, cf, userKey, version) {
			it.iitr.Next()
			continue
		}
		it.lastKey = append(it.lastKey[:0], userKey...)
		it.valid = true
		if it.txn != nil {
			encoded := kv.EncodeKeyWithCF(it.entry.CF, it.entry.Key)
			it.txn.addReadKey(encoded)
			if it.txn.db != nil {
				it.txn.db.recordRead(it.entry.Key)
			}
		}
		it.latestTs = it.entry.Version
		return
	}
	it.latestTs = 0
}

func (it *TxnIterator) materializeEntry(entry *kv.Entry, cf kv.ColumnFamily, userKey []byte, version uint64) bool {
	if entry == nil {
		return false
	}
	it.entry.Key = append(it.entry.Key[:0], userKey...)
	it.entry.CF = cf
	it.entry.Meta = entry.Meta
	it.entry.ExpiresAt = entry.ExpiresAt
	it.entry.Version = version
	if kv.IsValuePtr(entry) {
		if it.opt.KeyOnly {
			it.entry.Value = entry.Value
			it.item.valueBuf = it.item.valueBuf[:0]
		} else {
			var vp kv.ValuePtr
			vp.Decode(entry.Value)
			val, cb, err := it.txn.db.vlog.read(&vp)
			if err != nil {
				kv.RunCallback(cb)
				return false
			}
			it.valueBuf = append(it.valueBuf[:0], val...)
			kv.RunCallback(cb)
			it.entry.Value = it.valueBuf
			it.entry.Meta &^= kv.BitValuePointer
			it.item.valueBuf = it.entry.Value
		}
	} else {
		it.entry.Value = append(it.entry.Value[:0], entry.Value...)
		it.item.valueBuf = it.entry.Value
	}
	if isDeletedOrExpired(it.entry.Meta, it.entry.ExpiresAt) {
		return false
	}
	it.item.e = &it.entry
	return true
}

// Item returns pointer to the current key-value pair.
// This item is only valid until it.Next() gets called.
func (it *TxnIterator) Item() *Item {
	if !it.valid {
		return nil
	}
	return &it.item
}

// Valid returns false when iteration is done.
func (it *TxnIterator) Valid() bool {
	return it.valid
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
	if it.iitr != nil {
		_ = it.iitr.Close()
	}
	it.valid = false
	it.valueBuf = it.valueBuf[:0]
	it.item.valueBuf = it.item.valueBuf[:0]
	if it.pool != nil && it.ctx != nil {
		it.pool.put(it.ctx)
	}
	it.ctx = nil
	atomic.AddInt32(&it.txn.numIterators, -1)
}

// Next would advance the iterator by one. Always check it.Valid() after a Next()
// to ensure you have access to a valid it.Item().
func (it *TxnIterator) Next() {
	if it.iitr == nil {
		it.valid = false
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
	if len(key) > 0 && it.txn != nil {
		encoded := kv.EncodeKeyWithCF(kv.CFDefault, key)
		it.txn.addReadKey(encoded)
	}
	it.lastKey = it.lastKey[:0]
	if it.iitr == nil {
		it.valid = false
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
		it.valid = false
		it.latestTs = 0
		return
	}
	it.iitr.Rewind()
	it.advance()
}
