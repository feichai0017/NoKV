package runtime

import "sync"

// Module is the minimum lifecycle hook a runtime or research module
// must implement before it can be attached to the DB composition root.
//
// The interface is intentionally tiny: attached modules own their own
// state and cleanup, while the DB only coordinates lifecycle boundaries.
type Module interface {
	Close()
}

// Registry tracks optional runtime or research modules attached to one
// DB instance without baking module-specific fields into DB itself.
//
// This is a platform boundary, not a plugin framework. It exists to
// keep DB from accumulating one-off subsystem maps and locks as the
// repository grows.
type Registry struct {
	mu      sync.Mutex
	modules map[Module]struct{}
}

func (r *Registry) Register(m Module) {
	if r == nil || m == nil {
		return
	}
	r.mu.Lock()
	if r.modules == nil {
		r.modules = make(map[Module]struct{})
	}
	r.modules[m] = struct{}{}
	r.mu.Unlock()
}

func (r *Registry) Unregister(m Module) {
	if r == nil || m == nil {
		return
	}
	r.mu.Lock()
	delete(r.modules, m)
	r.mu.Unlock()
}

func (r *Registry) CloseAll() {
	if r == nil {
		return
	}
	r.mu.Lock()
	modules := make([]Module, 0, len(r.modules))
	for m := range r.modules {
		modules = append(modules, m)
	}
	r.modules = nil
	r.mu.Unlock()
	for _, m := range modules {
		if m != nil {
			m.Close()
		}
	}
}

func (r *Registry) Count() int {
	if r == nil {
		return 0
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.modules)
}

func (r *Registry) Cleared() bool {
	if r == nil {
		return true
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.modules == nil
}
