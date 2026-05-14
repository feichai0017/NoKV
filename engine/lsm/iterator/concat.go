// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package iterator owns the LSM merge/concat iterators that compose
// per-source iterators into a single ordered stream.
package iterator

import (
	"fmt"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/table"
)

// ConcatIterator iterates a slice of non-overlapping tables in order.
type ConcatIterator struct {
	idx     int // Which iterator is active now.
	cur     index.Iterator
	tables  []*table.Table // Disregarding reversed, this is in ascending order.
	options *index.Options // Valid options are REVERSED and NOCACHE.
}

// NewConcatIterator builds a ConcatIterator over tbls.
func NewConcatIterator(tbls []*table.Table, opt *index.Options) *ConcatIterator {
	return &ConcatIterator{
		options: opt,
		tables:  tbls,
		idx:     -1,
	}
}

func (s *ConcatIterator) setIdx(idx int) {
	if idx == s.idx && s.cur != nil {
		return
	}
	_ = s.closeCurrent()
	s.idx = idx
	if idx < 0 || idx >= len(s.tables) {
		s.cur = nil
		return
	}
	s.cur = s.tables[idx].NewIterator(s.options)
}

func (s *ConcatIterator) closeCurrent() error {
	if s.cur == nil {
		return nil
	}
	err := s.cur.Close()
	s.cur = nil
	return err
}

// Rewind resets to the first (or last in reverse) table's first valid entry.
func (s *ConcatIterator) Rewind() {
	if len(s.tables) == 0 {
		return
	}
	if s.options.IsAsc {
		s.setIdx(0)
	} else {
		s.setIdx(len(s.tables) - 1)
	}
	if s.cur == nil {
		return
	}
	s.cur.Rewind()
	s.advanceToValidCurrent()
}

// Valid reports whether the iterator currently points at an entry.
func (s *ConcatIterator) Valid() bool {
	return s.cur != nil && s.cur.Valid()
}

// Item returns the current entry.
func (s *ConcatIterator) Item() index.Item {
	return s.cur.Item()
}

// Seek positions the iterator at the first key >= target (or <= target in reverse).
func (s *ConcatIterator) Seek(key []byte) {
	n := len(s.tables)
	if n == 0 {
		s.setIdx(-1)
		return
	}
	var idx int
	if s.options.IsAsc {
		// First table whose max key >= target.
		lo, hi := 0, n
		for lo < hi {
			mid := lo + (hi-lo)/2
			if kv.CompareInternalKeys(s.tables[mid].MaxKey(), key) >= 0 {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		idx = lo
	} else {
		// Last table whose min key <= target.
		lo, hi := 0, n
		for lo < hi {
			mid := lo + (hi-lo)/2
			if kv.CompareInternalKeys(s.tables[mid].MinKey(), key) > 0 {
				hi = mid
			} else {
				lo = mid + 1
			}
		}
		idx = lo - 1
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	s.setIdx(idx)
	if s.cur == nil {
		return
	}
	s.cur.Seek(key)
	s.advanceToValidCurrent()
}

// Next advances to the next entry, crossing into the next table when needed.
func (s *ConcatIterator) Next() {
	s.cur.Next()
	if s.cur.Valid() {
		return
	}
	for { // In case there are empty tables.
		if s.options.IsAsc {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

func (s *ConcatIterator) advanceToValidCurrent() {
	for s.cur != nil && !s.cur.Valid() {
		if s.options.IsAsc {
			s.setIdx(s.idx + 1)
		} else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			return
		}
		s.cur.Rewind()
	}
}

// Close releases the active sub-iterator.
func (s *ConcatIterator) Close() error {
	if err := s.closeCurrent(); err != nil {
		return fmt.Errorf("ConcatIterator:%+v", err)
	}
	return nil
}
