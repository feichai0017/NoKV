package NoKV

import (
	"sync"

	ns "github.com/feichai0017/NoKV/namespace"
)

const defaultNamespaceShards = 16

// NamespaceOptions configures the top-level namespace listing facade.
type NamespaceOptions struct {
	Shards int
}

func (o NamespaceOptions) withDefaults() NamespaceOptions {
	if o.Shards <= 0 {
		o.Shards = defaultNamespaceShards
	}
	return o
}

// NamespaceHandle exposes the namespace listing module as a first-class DB capability
// without pushing namespace-specific logic into the storage-engine core.
type NamespaceHandle struct {
	store *ns.Store
	db    *DB

	closeOnce sync.Once
}

// Namespace returns a namespace listing facade backed by the current DB.
//
// The facade keeps the existing design boundary intact:
// - full-path truth remains authoritative
// - listing state remains companion state
// - all reads/writes still flow through the existing WAL + mutable tier + LSM path
func (db *DB) Namespace(opt NamespaceOptions) *NamespaceHandle {
	if db == nil {
		return nil
	}
	opt = opt.withDefaults()
	h := &NamespaceHandle{
		store: ns.NewStore(ns.NewNoKVStore(db), opt.Shards),
		db:    db,
	}
	db.runtimeModules.Register(h)
	return h
}

func (h *NamespaceHandle) Create(path []byte, kind ns.EntryKind, meta []byte) error {
	if h == nil || h.store == nil {
		return ns.ErrInvalidPath
	}
	return h.store.Create(path, kind, meta)
}

func (h *NamespaceHandle) Delete(path []byte) error {
	if h == nil || h.store == nil {
		return ns.ErrInvalidPath
	}
	return h.store.Delete(path)
}

func (h *NamespaceHandle) Lookup(path []byte) ([]byte, error) {
	if h == nil || h.store == nil {
		return nil, ns.ErrInvalidPath
	}
	return h.store.Lookup(path)
}

// List is the default strict namespace listing API.
//
// It only returns entries from currently covered intervals. Callers that need
// repair/bootstrap semantics should use RepairAndList explicitly.
func (h *NamespaceHandle) List(parent []byte, cursor ns.Cursor, limit int) ([]ns.Entry, ns.Cursor, ns.ListStats, error) {
	if h == nil || h.store == nil {
		return nil, ns.Cursor{}, ns.ListStats{}, ns.ErrInvalidPath
	}
	return h.store.List(parent, cursor, limit)
}

// RepairAndList explicitly repairs uncovered namespace state before retrying
// the strict read path.
func (h *NamespaceHandle) RepairAndList(parent []byte, cursor ns.Cursor, limit int) ([]ns.Entry, ns.Cursor, ns.ListStats, error) {
	if h == nil || h.store == nil {
		return nil, ns.Cursor{}, ns.ListStats{}, ns.ErrInvalidPath
	}
	return h.store.RepairAndList(parent, cursor, limit)
}

func (h *NamespaceHandle) Materialize(parent []byte) (ns.MaterializeStats, error) {
	if h == nil || h.store == nil {
		return ns.MaterializeStats{}, ns.ErrInvalidPath
	}
	return h.store.Materialize(parent)
}

func (h *NamespaceHandle) MaterializeReadPlane(parent []byte, maxPageEntries int) (ns.ReadRoot, []ns.ReadPage, error) {
	if h == nil || h.store == nil {
		return ns.ReadRoot{}, nil, ns.ErrInvalidPath
	}
	return h.store.MaterializeReadPlane(parent, maxPageEntries)
}

func (h *NamespaceHandle) LoadReadPlaneView(parent []byte) (ns.ReadPlaneView, bool, error) {
	if h == nil || h.store == nil {
		return ns.ReadPlaneView{}, false, ns.ErrInvalidPath
	}
	root, pages, ok, err := h.store.LoadReadPlane(parent)
	if err != nil || !ok {
		return ns.ReadPlaneView{}, ok, err
	}
	view, err := ns.NewReadPlaneView(root, pages)
	if err != nil {
		return ns.ReadPlaneView{}, false, err
	}
	return view, true, nil
}

func (h *NamespaceHandle) MaterializeDeltaPages(parent []byte, maxDeltaPages int) (ns.MaterializeStats, error) {
	if h == nil || h.store == nil {
		return ns.MaterializeStats{}, ns.ErrInvalidPath
	}
	return h.store.MaterializeDeltaPages(parent, maxDeltaPages)
}

func (h *NamespaceHandle) Stats(parent []byte) (ns.ListingStats, error) {
	if h == nil || h.store == nil {
		return ns.ListingStats{}, ns.ErrInvalidPath
	}
	return h.store.Stats(parent)
}

// Verify checks membership drift between truth and the current read-plane view.
//
// It is intentionally narrower than a full certificate verifier: it validates
// whether read-plane membership still matches truth, not whether all
// publication/certification invariants hold.
func (h *NamespaceHandle) Verify(parent []byte) (ns.VerifyStats, error) {
	if h == nil || h.store == nil {
		return ns.VerifyStats{}, ns.ErrInvalidPath
	}
	return h.store.Verify(parent)
}

func (h *NamespaceHandle) Rebuild(parent []byte) (ns.RebuildStats, error) {
	if h == nil || h.store == nil {
		return ns.RebuildStats{}, ns.ErrInvalidPath
	}
	return h.store.Rebuild(parent)
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
