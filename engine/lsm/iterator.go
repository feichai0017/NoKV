package lsm

import (
	"github.com/feichai0017/NoKV/engine/index"
)

// NewIterators builds iterators over mutable/immutable memtables across all
// shards plus the SST levels. Per-shard ordering is preserved (active first,
// then immutables newest-to-oldest); cross-shard ordering is resolved by
// MVCC timestamps at the merge layer.
func (lsm *LSM) NewIterators(opt *index.Options) []index.Iterator {
	iters := make([]index.Iterator, 0)
	for _, s := range lsm.shards {
		s.lock.RLock()
		mem := s.memTable
		immutables := append([]*memTable(nil), s.immutables...)
		s.lock.RUnlock()
		if mem != nil {
			iters = append(iters, mem.NewIterator(opt))
		}
		for _, imm := range immutables {
			if imm == nil {
				continue
			}
			iters = append(iters, imm.NewIterator(opt))
		}
	}
	iters = append(iters, lsm.levels.iterators(opt)...)
	return iters
}

type memIterator struct {
	innerIter index.Iterator
}

// NewIterator creates an iterator over entries stored in this memtable.
func (m *memTable) NewIterator(opt *index.Options) index.Iterator {
	if m == nil || m.index == nil {
		return nil
	}
	inner := m.index.NewIterator(opt)
	if inner == nil {
		return nil
	}
	return &memIterator{innerIter: inner}
}

func (iter *memIterator) Next() {
	if iter.innerIter == nil {
		return
	}
	iter.innerIter.Next()
}

func (iter *memIterator) Valid() bool {
	if iter.innerIter == nil {
		return false
	}
	return iter.innerIter.Valid()
}

func (iter *memIterator) Rewind() {
	if iter.innerIter == nil {
		return
	}
	iter.innerIter.Rewind()
}

func (iter *memIterator) Item() index.Item {
	if iter.innerIter == nil {
		return nil
	}
	return iter.innerIter.Item()
}

func (iter *memIterator) Close() error {
	if iter.innerIter == nil {
		return nil
	}
	return iter.innerIter.Close()
}

func (iter *memIterator) Seek(key []byte) {
	if iter.innerIter == nil {
		return
	}
	iter.innerIter.Seek(key)
}
