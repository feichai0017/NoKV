package lsm

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/utils"
)

// Iterator defines an exported API type.
type Iterator struct {
	iters []utils.Iterator
}

// Item defines an exported API type.
type Item struct {
	e *kv.Entry
}

// Entry is part of the exported receiver API.
func (it *Item) Entry() *kv.Entry {
	return it.e
}

// create iterators
func (lsm *LSM) NewIterators(opt *utils.Options) []utils.Iterator {
	iter := &Iterator{}
	iter.iters = make([]utils.Iterator, 0)
	lsm.lock.RLock()
	mem := lsm.memTable
	immutables := append([]*memTable(nil), lsm.immutables...)
	lsm.lock.RUnlock()
	if mem != nil {
		iter.iters = append(iter.iters, mem.NewIterator(opt))
	}
	for _, imm := range immutables {
		if imm == nil {
			continue
		}
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.iterators(opt)...)
	return iter.iters
}

// Next is part of the exported receiver API.
func (iter *Iterator) Next() {
	iter.iters[0].Next()
}

// Valid is part of the exported receiver API.
func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}

// Rewind is part of the exported receiver API.
func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}

// Item is part of the exported receiver API.
func (iter *Iterator) Item() utils.Item {
	return iter.iters[0].Item()
}

// Close is part of the exported receiver API.
func (iter *Iterator) Close() error {
	return nil
}

// Seek is part of the exported receiver API.
func (iter *Iterator) Seek(key []byte) {
}

// memtable iterator
type memIterator struct {
	innerIter utils.Iterator
}

// NewIterator creates a new value for the API.
func (m *memTable) NewIterator(opt *utils.Options) utils.Iterator {
	if m == nil || m.index == nil {
		return nil
	}
	inner := m.index.NewIterator(opt)
	if inner == nil {
		return nil
	}
	return &memIterator{innerIter: inner}
}

// Next is part of the exported receiver API.
func (iter *memIterator) Next() {
	if iter.innerIter == nil {
		return
	}
	iter.innerIter.Next()
}

// Valid is part of the exported receiver API.
func (iter *memIterator) Valid() bool {
	if iter.innerIter == nil {
		return false
	}
	return iter.innerIter.Valid()
}

// Rewind is part of the exported receiver API.
func (iter *memIterator) Rewind() {
	if iter.innerIter == nil {
		return
	}
	iter.innerIter.Rewind()
}

// Item is part of the exported receiver API.
func (iter *memIterator) Item() utils.Item {
	if iter.innerIter == nil {
		return nil
	}
	return iter.innerIter.Item()
}

// Close is part of the exported receiver API.
func (iter *memIterator) Close() error {
	if iter.innerIter == nil {
		return nil
	}
	return iter.innerIter.Close()
}

// Seek is part of the exported receiver API.
func (iter *memIterator) Seek(key []byte) {
	if iter.innerIter == nil {
		return
	}
	iter.innerIter.Seek(key)
}

// NewIterators creates a new value for the API.
func (lm *levelManager) NewIterators(options *utils.Options) []utils.Iterator {
	return lm.iterators(options)
}

// ConcatIterator merge multiple iterators into one
type ConcatIterator struct {
	idx     int // Which iterator is active now.
	cur     utils.Iterator
	iters   []utils.Iterator // Corresponds to tables.
	tables  []*table         // Disregarding reversed, this is in ascending order.
	options *utils.Options   // Valid options are REVERSED and NOCACHE.
}

// NewConcatIterator creates a new concatenated iterator
func NewConcatIterator(tbls []*table, opt *utils.Options) *ConcatIterator {
	iters := make([]utils.Iterator, len(tbls))
	return &ConcatIterator{
		options: opt,
		iters:   iters,
		tables:  tbls,
		idx:     -1, // Not really necessary because s.it.Valid()=false, but good to have.
	}
}

func (s *ConcatIterator) setIdx(idx int) {
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
		return
	}
	if s.iters[idx] == nil {
		s.iters[idx] = s.tables[idx].NewIterator(s.options)
	}
	s.cur = s.iters[s.idx]
}

// Rewind implements Interface
func (s *ConcatIterator) Rewind() {
	if len(s.iters) == 0 {
		return
	}
	if s.options.IsAsc {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.iters) - 1)
	}
	s.cur.Rewind()
}

// Valid implements y.Interface
func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

// Item _
func (s *ConcatIterator) Item() utils.Item {
	return s.cur.Item()
}

// Seek brings us to element >= key if reversed is false. Otherwise, <= key.
func (s *ConcatIterator) Seek(key []byte) {
	var idx int
	if s.options.IsAsc {
		idx = sort.Search(len(s.tables), func(i int) bool {
			return utils.CompareKeys(s.tables[i].MaxKey(), key) >= 0
		})
	} else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return utils.CompareKeys(s.tables[n-1-i].MinKey(), key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	// For reversed=false, we know s.tables[i-1].Biggest() < key. Thus, the
	// previous table cannot possibly contain key.
	s.setIdx(idx)
	s.cur.Seek(key)
}

// Next advances our concat iterator.
func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		// Nothing to do. Just stay with the current table.
		return
	}
	for { // In case there are empty tables.
		if s.options.IsAsc {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			// End of list. Valid will become false.
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

// Close implements y.Interface.
func (s *ConcatIterator) Close() error {
	for _, it := range s.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return fmt.Errorf("ConcatIterator:%+v", err)
		}
	}
	return nil
}

// MergeIterator merges multiple iterators into a single ordered stream.
// NOTE: MergeIterator owns the array of iterators and is responsible for closing them.
type MergeIterator struct {
	left  node
	right node
	small *node

	curKey  []byte
	reverse bool
}

type node struct {
	valid bool
	entry *kv.Entry
	iter  utils.Iterator

	// The two iterators are type asserted from `y.Iterator`, used to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than calling the
	// interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (n *node) setIterator(iter utils.Iterator) {
	n.iter = iter
	// It's okay if the type assertion below fails and n.merge/n.concat are set to nil.
	// We handle the nil values of merge and concat in all the methods.
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

func (n *node) setKey() {
	if n.iter == nil {
		n.valid = false
		n.entry = nil
		return
	}
	switch {
	case n.merge != nil:
		n.valid = n.merge.small != nil && n.merge.small.valid && n.merge.small.entry != nil
		n.entry = nil
		if n.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		n.entry = nil
		if n.valid {
			item := n.concat.Item()
			utils.CondPanic(item == nil, fmt.Errorf("concat iterator valid but item nil (type=%T)", n.concat))
			entry := item.Entry()
			utils.CondPanic(entry == nil, fmt.Errorf("concat iterator valid but entry nil (type=%T)", n.concat))
			n.entry = entry
		}
	default:
		n.valid = n.iter.Valid()
		n.entry = nil
		if n.valid {
			item := n.iter.Item()
			utils.CondPanic(item == nil, fmt.Errorf("iterator valid but item nil (type=%T)", n.iter))
			entry := item.Entry()
			utils.CondPanic(entry == nil, fmt.Errorf("iterator valid but entry nil (type=%T)", n.iter))
			n.entry = entry
		}
	}
}

func (n *node) next() {
	if n.iter == nil {
		n.valid = false
		n.entry = nil
		return
	}
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *node) rewind() {
	if n.iter == nil {
		n.valid = false
		n.entry = nil
		return
	}
	n.iter.Rewind()
	n.setKey()
}

func (n *node) seek(key []byte) {
	if n.iter == nil {
		n.valid = false
		n.entry = nil
		return
	}
	n.iter.Seek(key)
	n.setKey()
}

func (mi *MergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}
	cmp := utils.CompareKeys(mi.small.entry.Key, mi.bigger().entry.Key)
	switch {
	case cmp == 0: // Both the keys are equal.
		// In case of same keys, move the right iterator ahead.
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0: // Small is less than bigger().
		if mi.reverse {
			mi.swapSmall()
		}
		return
	default: // bigger() is less than small.
		if mi.reverse {
			// Do nothing since we're iterating in reverse. Small currently points to
			// the bigger key and that's okay in reverse iteration.
		} else {
			mi.swapSmall()
		}
		return
	}
}

func (mi *MergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.entry.Key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

func (mi *MergeIterator) setCurrent() {
	if mi.small.valid && mi.small.entry == nil {
		mi.small.valid = false
		mi.small.entry = nil
		return
	}
	if mi.small.valid {
		mi.curKey = append(mi.curKey[:0], mi.small.entry.Key...)
	}
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

// Seek brings us to element with key >= given key.
func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

// Valid returns whether the MergeIterator is at a valid element.
func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}

// Key returns the key associated with the current iterator.
func (mi *MergeIterator) Item() utils.Item {
	return mi.small.iter.Item()
}

// Close implements Iterator.
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return utils.WarpErr("MergeIterator", err1)
	}
	return utils.WarpErr("MergeIterator", err2)
}

// NewMergeIterator creates a merge iterator.
func NewMergeIterator(iters []utils.Iterator, reverse bool) utils.Iterator {
	switch len(iters) {
	case 0:
		return &Iterator{}
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// Assign left iterator randomly. This will be fixed when user calls rewind/seek.
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	return NewMergeIterator(
		[]utils.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}
