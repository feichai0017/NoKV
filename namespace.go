package NoKV

import (
	"sync"
)

const defaultNamespaceShards = 16

// NamespaceOptions reserves construction-time knobs for future namespace
// experiments. The current handle only registers a DB-scoped lifecycle module.
type NamespaceOptions struct {
	Shards int
}

func (o NamespaceOptions) withDefaults() NamespaceOptions {
	if o.Shards <= 0 {
		o.Shards = defaultNamespaceShards
	}
	return o
}

// NamespaceHandle is the minimal registration shell kept at the DB boundary
// while the previous namespace listing prototype has been retired.
type NamespaceHandle struct {
	db  *DB
	opt NamespaceOptions

	closeOnce sync.Once
}

// Namespace registers a DB-scoped namespace handle.
//
// The historical listing implementation has been removed. This hook remains so
// future namespace or DFS-oriented experiments can still attach lifecycle-bound
// state to the DB composition root without reintroducing package-specific
// fields into DB itself.
func (db *DB) Namespace(opt NamespaceOptions) *NamespaceHandle {
	if db == nil {
		return nil
	}
	opt = opt.withDefaults()
	h := &NamespaceHandle{
		db:  db,
		opt: opt,
	}
	db.runtimeModules.Register(h)
	return h
}

func (h *NamespaceHandle) Close() {
	if h == nil {
		return
	}
	h.closeOnce.Do(func() {
		if h.db != nil {
			h.db.runtimeModules.Unregister(h)
		}
	})
}
