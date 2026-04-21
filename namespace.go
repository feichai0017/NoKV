package NoKV

import (
	"sync"
)

// NamespaceOptions reserves construction-time knobs for future namespace
// experiments. The current handle only registers a DB-scoped lifecycle module.
type NamespaceOptions struct {
	// Shards is reserved for future namespace experiments. It is intentionally
	// ignored while only the registration shell remains.
	Shards int
}

// NamespaceHandle is the minimal registration shell kept at the DB boundary
// while the previous namespace listing prototype has been retired.
type NamespaceHandle struct {
	db *DB

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
	h := &NamespaceHandle{
		db: db,
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
