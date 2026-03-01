package NoKV

import (
	"bytes"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
)

// DBIterator wraps the merged LSM iterators and optionally resolves value-log pointers.
type DBIterator struct {
	iitr utils.Iterator
	vlog *valueLog
	pool *iteratorPool
	ctx  *iteratorContext
	// keyOnly avoids eager value log materialisation when true.
	keyOnly bool

	lowerBound []byte
	upperBound []byte
	isAsc      bool
	// seekOutOfRange marks Seek calls that intentionally invalidated the
	// iterator due to bounds checks. While set, Next must not advance from a
	// stale cursor position and resurrect validity.
	seekOutOfRange bool

	entry    kv.Entry
	item     Item
	valueBuf []byte
	valid    bool
}

// Item is the user-facing iterator item backed by an entry and optional vlog reader.
type Item struct {
	e        *kv.Entry
	vlog     *valueLog
	valueBuf []byte
}

// Entry returns the current entry view for this iterator item.
func (it *Item) Entry() *kv.Entry {
	return it.e
}

// ValueCopy returns a copy of the current value into dst (if provided).
// Mirrors Badger's semantics to aid callers expecting defensive copies.
func (it *Item) ValueCopy(dst []byte) ([]byte, error) {
	if it == nil || it.e == nil {
		return nil, utils.ErrKeyNotFound
	}
	val := it.e.Value
	if kv.IsValuePtr(it.e) {
		if it.vlog == nil {
			return nil, utils.ErrKeyNotFound
		}
		var vp kv.ValuePtr
		vp.Decode(val)
		fetched, cb, err := it.vlog.read(&vp)
		if cb != nil {
			defer kv.RunCallback(cb)
		}
		if err != nil {
			return nil, err
		}
		it.valueBuf = append(it.valueBuf[:0], fetched...)
		dst = append(dst[:0], it.valueBuf...)
		it.e.Value = it.valueBuf
		it.e.Meta &^= kv.BitValuePointer
		return dst, nil
	}
	if len(val) == 0 {
		return dst[:0], nil
	}
	dst = append(dst[:0], val...)
	return dst, nil
}

// NewIterator creates a DB-level iterator over user keys in the default column family.
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	if opt == nil {
		opt = &utils.Options{}
	}
	keyOnly := opt.OnlyUseKey
	ctx := db.iterPool.get()
	ctx.iters = append(ctx.iters, db.lsm.NewIterators(opt)...)
	itr := &DBIterator{
		vlog:       db.vlog,
		pool:       db.iterPool,
		ctx:        ctx,
		keyOnly:    keyOnly,
		lowerBound: opt.LowerBound,
		upperBound: opt.UpperBound,
		isAsc:      opt.IsAsc,
	}
	itr.item.vlog = db.vlog
	itr.item.e = &itr.entry
	itr.iitr = lsm.NewMergeIterator(ctx.iters, !opt.IsAsc)
	return itr
}

// NewInternalIterator returns an iterator over internal keys (CF marker + user key + timestamp).
// Callers must interpret kv.Entry.Key using kv.SplitInternalKey.
func (db *DB) NewInternalIterator(opt *utils.Options) utils.Iterator {
	if opt == nil {
		opt = &utils.Options{}
	}
	iters := db.lsm.NewIterators(opt)
	return lsm.NewMergeIterator(iters, !opt.IsAsc)
}

// Next advances to the next visible key/value pair.
func (iter *DBIterator) Next() {
	if iter == nil || iter.iitr == nil {
		return
	}
	if iter.seekOutOfRange {
		iter.valid = false
		return
	}
	iter.iitr.Next()
	iter.populate()
}

// Valid reports whether the iterator currently points at a valid item.
func (iter *DBIterator) Valid() bool {
	if iter == nil {
		return false
	}
	return iter.valid
}

// Rewind positions the iterator at the first or last key based on scan direction.
func (iter *DBIterator) Rewind() {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.seekOutOfRange = false
	iter.iitr.Rewind()
	iter.populate()
}

// Seek positions the iterator at the first key >= key in default column family order.
func (iter *DBIterator) Seek(key []byte) {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.seekOutOfRange = false

	// Clamping
	if iter.isAsc {
		if len(iter.upperBound) > 0 && bytes.Compare(key, iter.upperBound) >= 0 {
			iter.valid = false
			iter.seekOutOfRange = true
			return
		}
		if len(iter.lowerBound) > 0 && bytes.Compare(key, iter.lowerBound) < 0 {
			key = iter.lowerBound
		}
	} else {
		if len(iter.lowerBound) > 0 && bytes.Compare(key, iter.lowerBound) < 0 {
			iter.valid = false
			iter.seekOutOfRange = true
			return
		}
		if len(iter.upperBound) > 0 && bytes.Compare(key, iter.upperBound) >= 0 {
			key = iter.upperBound
		}
	}

	// Convert user key to internal key for seeking.
	// We use MaxUint64 as version to seek to the latest version of the key.
	// We default to CFDefault as DBIterator currently doesn't support specifying CF.
	internalKey := kv.InternalKey(kv.CFDefault, key, nonTxnMaxVersion)
	iter.iitr.Seek(internalKey)
	iter.populate()
}

// Item returns the currently materialized item, or nil when iterator is invalid.
func (iter *DBIterator) Item() utils.Item {
	if iter == nil || !iter.valid {
		return nil
	}
	return &iter.item
}

// Close releases underlying iterators and returns pooled iterator context.
func (iter *DBIterator) Close() error {
	if iter == nil {
		return nil
	}
	var err error
	if iter.iitr != nil {
		err = iter.iitr.Close()
		iter.iitr = nil
	}
	iter.valid = false
	iter.seekOutOfRange = false
	iter.valueBuf = iter.valueBuf[:0]
	if iter.pool != nil && iter.ctx != nil {
		iter.pool.put(iter.ctx)
	}
	iter.ctx = nil
	return err
}

func (iter *DBIterator) populate() {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.valid = false
	iter.item.valueBuf = iter.item.valueBuf[:0]
	for iter.iitr.Valid() {
		item := iter.iitr.Item()
		if item == nil {
			iter.iitr.Next()
			continue
		}
		entry := item.Entry()
		_, userKey, _ := kv.SplitInternalKey(entry.Key)

		// Skip entries below lower bound in forward mode, or invalidate in reverse
		if len(iter.lowerBound) > 0 && bytes.Compare(userKey, iter.lowerBound) < 0 {
			if !iter.isAsc {
				iter.valid = false
				return
			}
			iter.iitr.Next()
			continue
		}
		// Skip entries above upper bound in reverse mode, or invalidate in forward
		if len(iter.upperBound) > 0 && bytes.Compare(userKey, iter.upperBound) >= 0 {
			if iter.isAsc {
				iter.valid = false
				return
			}
			iter.iitr.Next()
			continue
		}

		if iter.materialize(entry) {
			iter.valid = true
			return
		}
		iter.iitr.Next()
	}
}

func (iter *DBIterator) materialize(src *kv.Entry) bool {
	if iter == nil || src == nil {
		return false
	}
	if src.IsDeletedOrExpired() {
		return false
	}
	iter.entry = *src
	cf, userKey, ts := kv.SplitInternalKey(iter.entry.Key)
	iter.entry.Key = userKey
	iter.entry.CF = cf
	if ts != 0 {
		iter.entry.Version = ts
	}
	if kv.IsValuePtr(src) {
		if iter.keyOnly {
			// Leave pointer encoded; defer value fetch to Item.ValueCopy.
			iter.entry.Value = src.Value
			iter.item.valueBuf = iter.item.valueBuf[:0]
		} else {
			var vp kv.ValuePtr
			vp.Decode(src.Value)
			val, cb, err := iter.vlog.read(&vp)
			if cb != nil {
				defer kv.RunCallback(cb)
			}
			if err != nil {
				return false
			}
			iter.valueBuf = append(iter.valueBuf[:0], val...)
			iter.entry.Value = iter.valueBuf
			iter.entry.Meta &^= kv.BitValuePointer
			iter.item.valueBuf = iter.entry.Value
		}
	} else {
		if src.Value == nil || src.IsDeletedOrExpired() {
			return false
		}
		iter.entry.Value = src.Value
		iter.item.valueBuf = iter.entry.Value
	}
	iter.item.e = &iter.entry
	return true
}
