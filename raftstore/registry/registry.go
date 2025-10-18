package registry

import (
    "fmt"
    "sync"

    "github.com/feichai0017/NoKV/manifest"
)

type PeerFactory func(meta manifest.RegionMeta) (any, error)

type Registry struct {
    mu       sync.RWMutex
    factories map[string]PeerFactory
}

func New() *Registry {
    return &Registry{factories: make(map[string]PeerFactory)}
}

func (r *Registry) Register(name string, factory PeerFactory) {
    r.mu.Lock()
    defer r.mu.Unlock()
    r.factories[name] = factory
}

func (r *Registry) Create(name string, meta manifest.RegionMeta) (any, error) {
    r.mu.RLock()
    defer r.mu.RUnlock()
    factory, ok := r.factories[name]
    if !ok {
        return nil, fmt.Errorf("registry: peer type %s not found", name)
    }
    return factory(meta)
}

func (r *Registry) Names() []string {
    r.mu.RLock()
    defer r.mu.RUnlock()
    names := make([]string, 0, len(r.factories))
    for name := range r.factories {
        names = append(names, name)
    }
    return names
}
