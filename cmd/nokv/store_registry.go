package main

import (
	"sync"

	storepkg "github.com/feichai0017/NoKV/raftstore/store"
)

var (
	runtimeStoresMu sync.RWMutex
	runtimeStores   []*storepkg.Store
)

func registerRuntimeStore(st *storepkg.Store) {
	if st == nil {
		return
	}
	runtimeStoresMu.Lock()
	defer runtimeStoresMu.Unlock()
	for _, existing := range runtimeStores {
		if existing == st {
			return
		}
	}
	runtimeStores = append(runtimeStores, st)
}

func unregisterRuntimeStore(st *storepkg.Store) {
	if st == nil {
		return
	}
	runtimeStoresMu.Lock()
	defer runtimeStoresMu.Unlock()
	for i, existing := range runtimeStores {
		if existing == st {
			runtimeStores = append(runtimeStores[:i], runtimeStores[i+1:]...)
			return
		}
	}
}

func runtimeStoreSnapshot() []*storepkg.Store {
	runtimeStoresMu.RLock()
	defer runtimeStoresMu.RUnlock()
	out := make([]*storepkg.Store, len(runtimeStores))
	copy(out, runtimeStores)
	return out
}
