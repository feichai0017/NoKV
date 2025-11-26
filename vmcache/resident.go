package vmcache

import "sync"

type residentSet struct {
	mu    sync.RWMutex
	ids   []uint64
	slots []int
	used  int
	mask  int
	empty uint64
	tomb  uint64
}

func newResident(capacity int) *residentSet {
	size := nextPow2(capacity * 2)
	ids := make([]uint64, size)
	slots := make([]int, size)
	empty := uint64(^uint64(0))
	for i := range ids {
		ids[i] = empty
		slots[i] = -1
	}
	return &residentSet{
		ids:   ids,
		slots: slots,
		mask:  size - 1,
		empty: empty,
		tomb:  empty - 1,
	}
}

func nextPow2(v int) int {
	n := 1
	for n < v {
		n <<= 1
	}
	return n
}

func (r *residentSet) get(id uint64) (int, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	pos := hash64(id) & uint64(r.mask)
	for {
		cur := r.ids[pos]
		if cur == r.empty {
			return -1, false
		}
		if cur == id {
			return r.slots[pos], true
		}
		pos = (pos + 1) & uint64(r.mask)
	}
}

func (r *residentSet) insert(id uint64, slot int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.insertLocked(id, slot)
}

func (r *residentSet) insertLocked(id uint64, slot int) {
	pos := hash64(id) & uint64(r.mask)
	for {
		cur := r.ids[pos]
		if cur == r.empty || cur == r.tomb {
			r.ids[pos] = id
			r.slots[pos] = slot
			r.used++
			return
		}
		pos = (pos + 1) & uint64(r.mask)
	}
}

func (r *residentSet) remove(id uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	pos := hash64(id) & uint64(r.mask)
	for {
		cur := r.ids[pos]
		if cur == r.empty {
			return
		}
		if cur == id {
			r.ids[pos] = r.tomb
			r.slots[pos] = -1
			return
		}
		pos = (pos + 1) & uint64(r.mask)
	}
}

func (r *residentSet) len() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.used
}

func (r *residentSet) iterate(fn func(id uint64, slot int)) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	for i := range r.ids {
		if r.ids[i] != r.empty && r.ids[i] != r.tomb {
			fn(r.ids[i], r.slots[i])
		}
	}
}

func hash64(k uint64) uint64 {
	const m = uint64(0xc6a4a7935bd1e995)
	const r = uint64(47)
	h := uint64(0x8445d61a4e774912) ^ uint64(0x9e3779b97f4a7c15)
	k *= m
	k ^= k >> r
	k *= m
	h ^= k
	h *= m
	h ^= h >> r
	h *= m
	h ^= h >> r
	return h
}
