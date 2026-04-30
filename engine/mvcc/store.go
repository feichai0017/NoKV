package mvcc

import (
	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
)

// Store is the common internal MVCC storage surface shared by raftstore,
// Percolator, and runtime maintenance tasks.
type Store interface {
	ApplyInternalEntries(entries []*kv.Entry) error
	GetInternalEntry(cf kv.ColumnFamily, key []byte, version uint64) (*kv.Entry, error)
	NewInternalIterator(opt *index.Options) index.Iterator
}
