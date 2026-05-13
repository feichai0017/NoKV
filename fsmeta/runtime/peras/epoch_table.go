package peras

import (
	"sync"

	fsperas "github.com/feichai0017/NoKV/fsmeta/exec/peras"
)

type epochTable struct {
	mu      sync.Mutex
	holders map[uint64]*fsperas.Holder
	grants  map[uint64]AuthorityGrant
}

func newEpochTable() *epochTable {
	return &epochTable{
		holders: make(map[uint64]*fsperas.Holder),
		grants:  make(map[uint64]AuthorityGrant),
	}
}

func (t *epochTable) holder(grant AuthorityGrant) (*fsperas.Holder, bool) {
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

func (t *epochTable) installHolder(grant AuthorityGrant, holder *fsperas.Holder) *fsperas.Holder {
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

func (t *epochTable) grant(epochID uint64) (AuthorityGrant, bool) {
	if t == nil {
		return AuthorityGrant{}, false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	grant, ok := t.grants[epochID]
	return grant, ok
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
