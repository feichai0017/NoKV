// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"github.com/panjf2000/ants/v2"
)

// Pool wraps ants.Pool without owning hidden global state.
type Pool struct {
	p    *ants.Pool
	size int
}

// NewPool creates a pool with the given size. If size<=0, defaults to 1.
func NewPool(size int, name string) *Pool {
	if size <= 0 {
		size = 1
	}
	p, _ := ants.NewPool(size, ants.WithPreAlloc(true))
	return &Pool{
		p:    p,
		size: size,
	}
}

// Submit runs fn in the pool.
func (pl *Pool) Submit(fn func()) error {
	if pl == nil || pl.p == nil || fn == nil {
		return nil
	}
	return pl.p.Submit(func() {
		fn()
	})
}

// Release frees resources.
func (pl *Pool) Release() {
	if pl == nil || pl.p == nil {
		return
	}
	pl.p.Release()
}

// Size reports configured worker count.
func (pl *Pool) Size() int { return pl.size }
