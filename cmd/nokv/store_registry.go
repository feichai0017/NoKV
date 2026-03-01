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
	runtimeModeDevStandalone = "dev-standalone"
	runtimeModeClusterPD     = "cluster-pd"
)

type runtimeStoreRecord struct {
	store *storepkg.Store
	mode  string
}

func registerRuntimeStore(st *storepkg.Store) {
	registerRuntimeStoreWithMode(st, runtimeModeDevStandalone)
}

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

func runtimeStoreSnapshot() []*storepkg.Store {
	runtimeStoresMu.RLock()
	defer runtimeStoresMu.RUnlock()
	out := make([]*storepkg.Store, len(runtimeStores))
	for i := range runtimeStores {
		out[i] = runtimeStores[i].store
	}
	return out
}

func runtimeStoreMode(st *storepkg.Store) string {
	if st == nil {
		return runtimeModeDevStandalone
	}
	runtimeStoresMu.RLock()
	defer runtimeStoresMu.RUnlock()
	for i := range runtimeStores {
		if runtimeStores[i].store == st {
			return runtimeStores[i].mode
		}
	}
	return runtimeModeDevStandalone
}

func normalizeRuntimeMode(mode string) string {
	switch mode {
	case runtimeModeClusterPD:
		return runtimeModeClusterPD
	default:
		return runtimeModeDevStandalone
	}
}
