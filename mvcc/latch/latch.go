package latch

import (
	"sort"
	"sync"

	"github.com/feichai0017/NoKV/utils"
)

// Manager provides hashed latches on keys to serialize conflicting
// operations. It mirrors the lightweight latch manager used in TinyKV.
type Manager struct {
	mu      sync.Mutex
	stripes []sync.Mutex
}

// Guard releases acquired latches when done.
type Guard struct {
	manager *Manager
	slots   []int
}

// NewManager allocates a latch manager with the specified number of stripes.
func NewManager(size int) *Manager {
	if size <= 0 {
		size = 256
	}
	return &Manager{stripes: make([]sync.Mutex, size)}
}

// Acquire locks the stripes associated with the provided keys. The returned
// guard must be released to avoid deadlocks.
func (m *Manager) Acquire(keys [][]byte) *Guard {
	if m == nil || len(keys) == 0 {
		return &Guard{}
	}
	indices := make([]int, 0, len(keys))
body:
	for _, key := range keys {
		if len(key) == 0 {
			continue
		}
		h := utils.MemHash(key)
		idx := int(h % uint64(len(m.stripes)))
		// deduplicate identical indices for identical keys
		for _, existing := range indices {
			if existing == idx {
				continue body
			}
		}
		indices = append(indices, idx)
	}
	if len(indices) == 0 {
		return &Guard{}
	}
	sort.Ints(indices)
	for _, idx := range indices {
		m.stripes[idx].Lock()
	}
	return &Guard{manager: m, slots: indices}
}

// Release unlocks previously acquired latches. It is safe to call multiple
// times.
func (g *Guard) Release() {
	if g == nil || g.manager == nil || len(g.slots) == 0 {
		return
	}
	for i := len(g.slots) - 1; i >= 0; i-- {
		g.manager.stripes[g.slots[i]].Unlock()
	}
	g.manager = nil
	g.slots = nil
}
