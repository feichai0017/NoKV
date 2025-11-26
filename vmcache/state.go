package vmcache

import "sync/atomic"

const (
	stateUnlocked  = 0
	stateMaxShared = 252
	stateLocked    = 253
	stateMarked    = 254
	stateEvicted   = 255
)

type pageState struct {
	stateAndVersion atomic.Uint64
}

func (p *pageState) init() {
	p.stateAndVersion.Store(encodeStateVersion(0, stateEvicted))
}

func (p *pageState) load() uint64 {
	return p.stateAndVersion.Load()
}

func (p *pageState) state(v uint64) uint64 {
	return v >> 56
}

func (p *pageState) sameVersion(v uint64, newState uint64) uint64 {
	return (v<<8)>>8 | newState<<56
}

func (p *pageState) nextVersion(v uint64, newState uint64) uint64 {
	return ((v<<8)>>8 + 1) | newState<<56
}

func encodeStateVersion(version uint64, state uint64) uint64 {
	return version | state<<56
}

func (p *pageState) tryLockX(old uint64) bool {
	return p.stateAndVersion.CompareAndSwap(old, p.sameVersion(old, stateLocked))
}

func (p *pageState) unlockX() {
	for {
		old := p.stateAndVersion.Load()
		if p.state(old) != stateLocked {
			return
		}
		if p.stateAndVersion.CompareAndSwap(old, p.nextVersion(old, stateUnlocked)) {
			return
		}
	}
}

func (p *pageState) unlockXEvicted() {
	for {
		old := p.stateAndVersion.Load()
		if p.state(old) != stateLocked {
			return
		}
		if p.stateAndVersion.CompareAndSwap(old, p.nextVersion(old, stateEvicted)) {
			return
		}
	}
}

func (p *pageState) tryLockS(old uint64) bool {
	s := p.state(old)
	switch s {
	case stateUnlocked:
		return p.stateAndVersion.CompareAndSwap(old, p.sameVersion(old, 1))
	case stateMarked:
		return p.stateAndVersion.CompareAndSwap(old, p.sameVersion(old, 1))
	default:
		if s > 0 && s < stateMaxShared {
			return p.stateAndVersion.CompareAndSwap(old, p.sameVersion(old, s+1))
		}
	}
	return false
}

func (p *pageState) unlockS() {
	for {
		old := p.stateAndVersion.Load()
		s := p.state(old)
		if s == 0 || s >= stateLocked {
			return
		}
		if p.stateAndVersion.CompareAndSwap(old, p.sameVersion(old, s-1)) {
			return
		}
	}
}

func (p *pageState) tryMark(old uint64) bool {
	if p.state(old) != stateUnlocked {
		return false
	}
	return p.stateAndVersion.CompareAndSwap(old, p.sameVersion(old, stateMarked))
}
