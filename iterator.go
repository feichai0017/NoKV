package NoKV

import (
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
)

type DBIterator struct {
	iitr utils.Iterator
	vlog *valueLog
	pool *iteratorPool
	ctx  *iteratorContext

	entry    utils.Entry
	item     Item
	valueBuf []byte
	valid    bool
}

type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}

// ValueCopy returns a copy of the current value into dst (if provided).
// Mirrors Badger's semantics to aid callers expecting defensive copies.
func (it *Item) ValueCopy(dst []byte) ([]byte, error) {
	if it == nil || it.e == nil {
		return nil, utils.ErrKeyNotFound
	}
	val := it.e.Value
	if len(val) == 0 {
		return dst[:0], nil
	}
	dst = append(dst[:0], val...)
	return dst, nil
}
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	if opt == nil {
		opt = &utils.Options{}
	}
	ctx := db.iterPool.get()
	ctx.iters = append(ctx.iters, db.lsm.NewIterators(opt)...)
	itr := &DBIterator{
		vlog: db.vlog,
		pool: db.iterPool,
		ctx:  ctx,
	}
	itr.item.e = &itr.entry
	itr.iitr = lsm.NewMergeIterator(ctx.iters, opt.IsAsc)
	return itr
}

func (iter *DBIterator) Next() {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.iitr.Next()
	iter.populate()
}

func (iter *DBIterator) Valid() bool {
	if iter == nil {
		return false
	}
	return iter.valid
}

func (iter *DBIterator) Rewind() {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.iitr.Rewind()
	iter.populate()
}

func (iter *DBIterator) Seek(key []byte) {
	if iter == nil || iter.iitr == nil {
		return
	}
	iter.iitr.Seek(key)
	iter.populate()
}

func (iter *DBIterator) Item() utils.Item {
	if iter == nil || !iter.valid {
		return nil
	}
	return &iter.item
}

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
	iter.valueBuf = iter.valueBuf[:0]
	if iter.pool != nil && iter.ctx != nil {
		iter.pool.put(iter.ctx)
	}
	iter.ctx = nil
	return err
}

func (iter *DBIterator) populate() {
	iter.valid = false
	if iter == nil || iter.iitr == nil {
		return
	}
	for iter.iitr.Valid() {
		item := iter.iitr.Item()
		if item == nil {
			iter.iitr.Next()
			continue
		}
		if iter.materialize(item.Entry()) {
			iter.valid = true
			return
		}
		iter.iitr.Next()
	}
}

func (iter *DBIterator) materialize(src *utils.Entry) bool {
	if iter == nil || src == nil {
		return false
	}
	if src.IsDeletedOrExpired() {
		return false
	}
	iter.entry = *src
	if utils.IsValuePtr(src) {
		var vp utils.ValuePtr
		vp.Decode(src.Value)
		val, cb, err := iter.vlog.read(&vp)
		if cb != nil {
			defer utils.RunCallback(cb)
		}
		if err != nil || len(val) == 0 {
			return false
		}
		iter.valueBuf = append(iter.valueBuf[:0], val...)
		iter.entry.Value = iter.valueBuf
	} else {
		if src.Value == nil || src.IsDeletedOrExpired() {
			return false
		}
		iter.entry.Value = src.Value
	}
	iter.item.e = &iter.entry
	return true
}
