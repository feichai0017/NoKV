// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"slices"
	"sync"

	"github.com/feichai0017/NoKV/fsmeta/exec/compile"
)

type AdmissionLatches struct {
	global  sync.RWMutex
	stripes [64]admissionLatchStripe
}

type admissionLatchStripe struct {
	mu      sync.Mutex
	latches map[string]*admissionLatch
}

type admissionLatch struct {
	mu   sync.Mutex
	refs int
}

type heldAdmissionLatch struct {
	key    string
	stripe *admissionLatchStripe
	latch  *admissionLatch
}

func NewAdmissionLatches() *AdmissionLatches {
	return &AdmissionLatches{}
}

func (l *AdmissionLatches) Lock(op compile.MaterializedOp) func() {
	if l == nil {
		return func() {}
	}
	keys, broad := admissionLatchKeys(op)
	if broad {
		l.global.Lock()
		return l.global.Unlock
	}
	l.global.RLock()
	held := make([]heldAdmissionLatch, 0, len(keys))
	for _, key := range keys {
		stripe := &l.stripes[admissionLatchStripeIndex(key)]
		stripe.mu.Lock()
		if stripe.latches == nil {
			stripe.latches = make(map[string]*admissionLatch)
		}
		latch := stripe.latches[key]
		if latch == nil {
			latch = &admissionLatch{}
			stripe.latches[key] = latch
		}
		latch.refs++
		stripe.mu.Unlock()
		held = append(held, heldAdmissionLatch{key: key, stripe: stripe, latch: latch})
	}
	for _, item := range held {
		item.latch.mu.Lock()
	}
	return func() {
		for i := len(held) - 1; i >= 0; i-- {
			item := held[i]
			item.latch.mu.Unlock()
			item.stripe.mu.Lock()
			item.latch.refs--
			if item.latch.refs == 0 {
				delete(item.stripe.latches, item.key)
			}
			item.stripe.mu.Unlock()
		}
		l.global.RUnlock()
	}
}

func admissionLatchKeys(op compile.MaterializedOp) ([]string, bool) {
	keys := make([]string, 0, len(op.Footprint.ConflictKeys))
	for _, ref := range op.Footprint.ConflictKeys {
		if ref.Mode == compile.KeyAccessReadPrefix || len(ref.Key) == 0 {
			return nil, true
		}
		keys = append(keys, string(ref.Key))
	}
	if len(keys) == 0 {
		return nil, true
	}
	slices.Sort(keys)
	return slices.Compact(keys), false
}

func admissionLatchStripeIndex(key string) uint64 {
	var h uint64 = 14695981039346656037
	for i := 0; i < len(key); i++ {
		h ^= uint64(key[i])
		h *= 1099511628211
	}
	return h % 64
}
