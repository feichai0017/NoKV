package NoKV

import (
	"github.com/feichai0017/NoKV/lsm"
	"github.com/feichai0017/NoKV/utils"
)

type DBIterator struct {
	iitr utils.Iterator
	vlog *valueLog
}
type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}
func (db *DB) NewIterator(opt *utils.Options) utils.Iterator {
	iters := make([]utils.Iterator, 0)
	iters = append(iters, db.lsm.NewIterators(opt)...)

	res := &DBIterator{
		vlog: db.vlog,
		iitr: lsm.NewMergeIterator(iters, opt.IsAsc),
	}
	return res
}

func (iter *DBIterator) Next() {
	iter.iitr.Next()
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	}
}
func (iter *DBIterator) Valid() bool {
	return iter.iitr.Valid()
}
func (iter *DBIterator) Rewind() {
	iter.iitr.Rewind()
	for ; iter.Valid() && iter.Item() == nil; iter.iitr.Next() {
	}
}
func (iter *DBIterator) Item() utils.Item {
	// 检查从lsm拿到的value是否是value ptr,是则从vlog中拿值
	e := iter.iitr.Item().Entry()
	var value []byte

	if e != nil && utils.IsValuePtr(e) {
		var vp utils.ValuePtr
		vp.Decode(e.Value)
		result, cb, err := iter.vlog.read(&vp)
		defer utils.RunCallback(cb)
		if err != nil {
			return nil
		}
		value = utils.SafeCopy(nil, result)
	}

	if e.IsDeletedOrExpired() || value == nil {
		return nil
	}

	res := &utils.Entry{
		Key:          e.Key,
		Value:        value,
		ExpiresAt:    e.ExpiresAt,
		Meta:         e.Meta,
		Version:      e.Version,
		Offset:       e.Offset,
		Hlen:         e.Hlen,
		ValThreshold: e.ValThreshold,
	}
	return res
}
func (iter *DBIterator) Close() error {
	return iter.iitr.Close()
}
func (iter *DBIterator) Seek(key []byte) {
}