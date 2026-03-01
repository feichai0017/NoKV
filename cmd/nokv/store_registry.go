package main

import (
	"sync"

	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

var (
	runtimeStoresMu sync.RWMutex
	runtimeStores   []runtimeStoreRecord
)

const (
	// runtimeModeDevStandalone marks a local process where raftstore is used
	// without an external PD control plane.
	runtimeModeDevStandalone = "dev-standalone"
	// runtimeModeClusterPD marks a process that is attached to PD and must treat
	// PD as the runtime control-plane source of truth.
	runtimeModeClusterPD = "cluster-pd"
)

// runtimeStoreRecord tracks a registered store plus its runtime mode so helper
// commands can enforce mode-specific behavior.
type runtimeStoreRecord struct {
	store *storepkg.Store
	mode  string
}

// registerRuntimeStore keeps backward compatibility for call sites that do not
// care about runtime mode. It defaults to standalone semantics.
func registerRuntimeStore(st *storepkg.Store) {
	registerRuntimeStoreWithMode(st, runtimeModeDevStandalone)
}

// registerRuntimeStoreWithMode records a store and the mode in which it is
// running. Re-registering updates mode in place.
func registerRuntimeStoreWithMode(st *storepkg.Store, mode string) {
	if st == nil {
		return
	}
	mode = normalizeRuntimeMode(mode)
	runtimeStoresMu.Lock()
	defer runtimeStoresMu.Unlock()
	for i := range runtimeStores {
		if runtimeStores[i].store == st {
			runtimeStores[i].mode = mode
			return
		}
	}
	runtimeStores = append(runtimeStores, runtimeStoreRecord{
		store: st,
		mode:  mode,
	})
}

// unregisterRuntimeStore removes a previously registered store entry.
func unregisterRuntimeStore(st *storepkg.Store) {
	if st == nil {
		return
	}
	runtimeStoresMu.Lock()
	defer runtimeStoresMu.Unlock()
	for i := range runtimeStores {
		if runtimeStores[i].store == st {
			runtimeStores = append(runtimeStores[:i], runtimeStores[i+1:]...)
			return
		}
	}
}

// runtimeStoreSnapshot returns the currently registered stores.
func runtimeStoreSnapshot() []*storepkg.Store {
	runtimeStoresMu.RLock()
	defer runtimeStoresMu.RUnlock()
	out := make([]*storepkg.Store, len(runtimeStores))
	for i := range runtimeStores {
		out[i] = runtimeStores[i].store
	}
	return out
}

// normalizeRuntimeMode constrains mode values to known constants.
func normalizeRuntimeMode(mode string) string {
	switch mode {
	case runtimeModeClusterPD:
		return runtimeModeClusterPD
	default:
		return runtimeModeDevStandalone
	}
}
