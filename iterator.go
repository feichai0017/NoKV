package NoKV

// DB-side facades for the user-facing iterator. The iterator state
// machine + Item materialization live in runtime/iterator; this file
// only wires DB internals (lsm, vlog, iterPool) into the construction
// path so callers can keep writing db.NewIterator(...) / db.NewInternalIterator(...).

import (
	"github.com/feichai0017/NoKV/engine/index"
	iter "github.com/feichai0017/NoKV/runtime/iterator"
)

// NewIterator creates a DB-level iterator over user keys in the default column family.
func (db *DB) NewIterator(opt *index.Options) index.Iterator {
	return iter.New(iter.Deps{
		Storage: db.lsm,
		Vlog:    db.vlog,
		Pool:    db.iterPool,
	}, opt)
}

// NewInternalIterator returns an iterator over internal keys (CF marker + user key + timestamp).
// Callers should decode kv.Entry.Key via kv.SplitInternalKey and handle ok=false.
func (db *DB) NewInternalIterator(opt *index.Options) index.Iterator {
	return iter.NewInternal(db.lsm, opt)
}
