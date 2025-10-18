package store

import "sync"

var (
	storeRegistryMu sync.Mutex
	storeRegistry   []*Store
)

// RegisterStore records the store instance for observers such as CLI tools.
func RegisterStore(st *Store) {
	if st == nil {
		return
	}
	storeRegistryMu.Lock()
	storeRegistry = append(storeRegistry, st)
	storeRegistryMu.Unlock()
}

// Stores returns the list of registered stores.
func Stores() []*Store {
	storeRegistryMu.Lock()
	defer storeRegistryMu.Unlock()
	out := make([]*Store, len(storeRegistry))
	copy(out, storeRegistry)
	return out
}
