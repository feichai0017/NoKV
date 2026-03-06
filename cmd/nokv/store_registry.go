package main

import (
	"slices"
	"sync"

	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

var (
	runtimeStoresMu sync.RWMutex
	runtimeStores   []*storepkg.Store
)

// registerRuntimeStore records a running store instance.
func registerRuntimeStore(st *storepkg.Store) {
	if st == nil {
		return
	}
	runtimeStoresMu.Lock()
	defer runtimeStoresMu.Unlock()
	if slices.Contains(runtimeStores, st) {
		return
	}
	runtimeStores = append(runtimeStores, st)
}

// unregisterRuntimeStore removes a previously registered store entry.
func unregisterRuntimeStore(st *storepkg.Store) {
	if st == nil {
		return
	}
	runtimeStoresMu.Lock()
	defer runtimeStoresMu.Unlock()
	for i := range runtimeStores {
		if runtimeStores[i] == st {
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
	copy(out, runtimeStores)
	return out
}
