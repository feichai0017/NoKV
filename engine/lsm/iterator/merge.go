// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package iterator

import (
	"bytes"
	"fmt"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
)

// emptyIterator is the no-op iterator returned by NewMergeIterator when no
// inputs are supplied.
type emptyIterator struct{}

func (*emptyIterator) Next()            {}
func (*emptyIterator) Rewind()          {}
func (*emptyIterator) Seek([]byte)      {}
func (*emptyIterator) Valid() bool      { return false }
func (*emptyIterator) Item() index.Item { return nil }
func (*emptyIterator) Close() error     { return nil }

// MergeIterator merges multiple iterators into a single ordered stream.
// MergeIterator owns the array of iterators and is responsible for closing them.
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
	iter  index.Iterator

	// Type-asserted from index.Iterator to inline more function calls.
	// Calling functions on concrete types is much faster (about 25-30%) than
	// calling the interface's function.
	merge  *MergeIterator
	concat *ConcatIterator
}

func (n *node) setFromMerge() {
	n.valid = n.merge.small != nil && n.merge.small.valid && n.merge.small.entry != nil
	n.entry = nil
	if n.valid {
		n.entry = n.merge.small.entry
	}
}

func (n *node) setFromConcat() {
	n.valid = n.concat.Valid()
	n.entry = nil
	if !n.valid {
		return
	}
	item := n.concat.Item()
	utils.CondPanicFunc(item == nil, func() error {
		return fmt.Errorf("concat iterator valid but item nil (type=%T)", n.concat)
	})
	entry := item.Entry()
	utils.CondPanicFunc(entry == nil, func() error {
		return fmt.Errorf("concat iterator valid but entry nil (type=%T)", n.concat)
	})
	n.entry = entry
}

func (n *node) setFromIter() {
	n.valid = n.iter.Valid()
	n.entry = nil
	if !n.valid {
		return
	}
	item := n.iter.Item()
	utils.CondPanicFunc(item == nil, func() error {
		return fmt.Errorf("iterator valid but item nil (type=%T)", n.iter)
	})
	entry := item.Entry()
	utils.CondPanicFunc(entry == nil, func() error {
		return fmt.Errorf("iterator valid but entry nil (type=%T)", n.iter)
	})
	n.entry = entry
}

func (n *node) setIterator(iter index.Iterator) {
	n.iter = iter
	// It's okay if the type assertion below fails and n.merge/n.concat are
	// nil; the methods handle that case.
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
		n.setFromMerge()
	case n.concat != nil:
		n.setFromConcat()
	default:
		n.setFromIter()
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
		n.setFromMerge()
	case n.concat != nil:
		n.concat.Next()
		n.setFromConcat()
	default:
		n.iter.Next()
		n.setFromIter()
	}
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
	cmp := kv.CompareInternalKeys(mi.small.entry.Key, mi.bigger().entry.Key)
	switch {
	case cmp == 0: // Both keys are equal; advance the right iterator.
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0: // small < bigger.
		if mi.reverse {
			mi.swapSmall()
		}
		return
	default: // bigger < small.
		if !mi.reverse {
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

// Rewind seeks to the first element (or the last for a reverse iterator).
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

// Item returns the current entry.
func (mi *MergeIterator) Item() index.Item {
	return mi.small.iter.Item()
}

// Close closes both child iterators, joining errors.
func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return fmt.Errorf("merge iterator close left: %w", err1)
	}
	if err2 != nil {
		return fmt.Errorf("merge iterator close right: %w", err2)
	}
	return nil
}

// NewMergeIterator constructs a balanced binary tree of MergeIterator nodes
// over iters. nil entries are filtered out.
func NewMergeIterator(iters []index.Iterator, reverse bool) index.Iterator {
	filtered := iters[:0]
	for _, it := range iters {
		if it != nil {
			filtered = append(filtered, it)
		}
	}
	return newMergeIterator(filtered, reverse)
}

func newMergeIterator(iters []index.Iterator, reverse bool) index.Iterator {
	switch len(iters) {
	case 0:
		return &emptyIterator{}
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		mi.small = &mi.left
		return mi
	}
	mid := len(iters) / 2
	mi := &MergeIterator{
		reverse: reverse,
	}
	mi.left.setIterator(newMergeIterator(iters[:mid], reverse))
	mi.right.setIterator(newMergeIterator(iters[mid:], reverse))
	mi.small = &mi.left
	return mi
}
