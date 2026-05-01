package storage

import (
	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
)

// Store is the narrow internal storage surface used by Percolator and
// raftstore MVCC maintenance code.
type Store interface {
	// ApplyInternalEntries persists one caller-provided batch of internal
	// entries. Implementations used by raft apply must preserve the batch as
	// one atomic storage group; maintenance callers retry whole batches.
	ApplyInternalEntries(entries []*kv.Entry) error
	GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
	NewInternalIterator(opt *index.Options) index.Iterator
}
