// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package peras

import (
	"sync"

	fsperas "github.com/feichai0017/NoKV/experimental/peras/exec"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
)

type epochTable struct {
	mu      sync.Mutex
	holders map[uint64]*fsperas.Holder
	grants  map[uint64]rootproto.PerasAuthorityGrant
}

func newEpochTable() *epochTable {
	return &epochTable{
		holders: make(map[uint64]*fsperas.Holder),
		grants:  make(map[uint64]rootproto.PerasAuthorityGrant),
	}
}

func (t *epochTable) holder(grant rootproto.PerasAuthorityGrant) (*fsperas.Holder, bool) {
	if t == nil {
		return nil, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	holder := t.holders[grant.EpochID]
	if holder == nil {
		return nil, false
	}
	t.grants[grant.EpochID] = grant
	return holder, true
}

func (t *epochTable) installHolder(grant rootproto.PerasAuthorityGrant, holder *fsperas.Holder) *fsperas.Holder {
	if t == nil || holder == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if current := t.holders[grant.EpochID]; current != nil {
		t.grants[grant.EpochID] = grant
		return current
	}
	t.holders[grant.EpochID] = holder
	t.grants[grant.EpochID] = grant
	return holder
}

func (t *epochTable) grant(epochID uint64) (rootproto.PerasAuthorityGrant, bool) {
	if t == nil {
		return rootproto.PerasAuthorityGrant{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	grant, ok := t.grants[epochID]
	return grant, ok
}

func (t *epochTable) updateGrant(grant rootproto.PerasAuthorityGrant) {
	if t == nil || !grant.Valid() {
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.holders[grant.EpochID] == nil {
		return
	}
	t.grants[grant.EpochID] = grant
}

func (t *epochTable) holderSnapshot() []*fsperas.Holder {
	if t == nil {
		return nil
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]*fsperas.Holder, 0, len(t.holders))
	for _, holder := range t.holders {
		out = append(out, holder)
	}
	return out
}

func (t *epochTable) stats() (holders int, pending int) {
	if t == nil {
		return 0, 0
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	holders = len(t.holders)
	for _, holder := range t.holders {
		pending += holder.Pending()
	}
	return holders, pending
}
