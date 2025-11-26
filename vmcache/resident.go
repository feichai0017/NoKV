package vmcache

import "sync"

const residentShards = 64

type residentSet struct {
	shards []residentShard
}

type residentShard struct {
	mu sync.RWMutex
	m  map[uint64]int
}

func newResident(capacity int) *residentSet {
	shards := make([]residentShard, residentShards)
	per := capacity/residentShards + 1
	for i := range shards {
		shards[i].m = make(map[uint64]int, per)
	}
	return &residentSet{shards: shards}
}

func (r *residentSet) shard(id uint64) *residentShard {
	return &r.shards[hash64(id)&(residentShards-1)]
}

func (r *residentSet) get(id uint64) (int, bool) {
	s := r.shard(id)
	s.mu.RLock()
	slot, ok := s.m[id]
	s.mu.RUnlock()
	return slot, ok
}

func (r *residentSet) insert(id uint64, slot int) {
	s := r.shard(id)
	s.mu.Lock()
	s.m[id] = slot
	s.mu.Unlock()
}

func (r *residentSet) remove(id uint64) {
	s := r.shard(id)
	s.mu.Lock()
	delete(s.m, id)
	s.mu.Unlock()
}

func (r *residentSet) len() int {
	total := 0
	for i := range r.shards {
		s := &r.shards[i]
		s.mu.RLock()
		total += len(s.m)
		s.mu.RUnlock()
	}
	return total
}
