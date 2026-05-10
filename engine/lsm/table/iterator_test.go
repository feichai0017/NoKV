package table

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/vfs"
)

// TestTableIteratorSeekAndIteratorPrefetch exercises the iterator's lazy
// SSTable handle reopen and the prefetch-enabled forward path. It pokes at
// internal state (mu, closeSSTableLocked, idx, keyCount, maxVersion,
// hasBloom) so it lives inside the table package.
func TestTableIteratorSeekAndIteratorPrefetch(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-iter")
	if err != nil {
		t.Fatalf("mkdir tmp: %v", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	opts := newTestOptions(dir)
	opts.BlockSize = 64
	opts.BloomFalsePositive = 0.01
	rt := newTestRuntime(opts)
	builder := NewBuilder(rt.Options())

	for i := range 20 {
		key := kv.InternalKey(kv.CFDefault, fmt.Appendf(nil, "k%02d", i), 1)
		value := bytes.Repeat([]byte{'v'}, 48)
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := vfs.FileNameSSTable(dir, 1)
	tbl, err := Open(rt, tableName, builder)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if tbl == nil {
		t.Fatalf("expected table from builder, got nil")
	}
	defer func() { _ = tbl.DecrRef() }()

	// Force a handle close + cache eviction; subsequent metadata reads must
	// transparently reopen and repopulate.
	tbl.mu.Lock()
	tbl.closeSSTableLocked()
	tbl.mu.Unlock()

	tbl.idx.Store(nil)
	rt.Cache().DelIndex(tbl.FID())
	tbl.keyCount = 0
	tbl.maxVersion = 0
	tbl.hasBloom = false

	if tbl.KeyCount() == 0 {
		t.Fatalf("expected key count to be available after lazy reload")
	}
	if tbl.MaxVersionVal() == 0 {
		t.Fatalf("expected max version to be available after lazy reload")
	}
	if !tbl.HasBloomFilter() {
		t.Fatalf("expected bloom filter to be available after lazy reload")
	}

	idx := tbl.index()
	if idx == nil {
		t.Fatalf("expected table index")
	}
	if _, ok := tbl.blockOffset(len(idx.GetOffsets())); !ok {
		t.Fatalf("expected block offset lookup to succeed")
	}

	it := tbl.NewIterator(&index.Options{IsAsc: true, PrefetchBlocks: 1, PrefetchWorkers: 1})
	tblIter, ok := it.(*Iterator)
	if !ok {
		t.Fatalf("expected table iterator, got %T", it)
	}
	tblIter.Rewind()
	if !tblIter.Valid() {
		t.Fatalf("expected iterator to be valid after rewind")
	}
	if tblIter.bi != nil {
		_ = tblIter.bi.Rewind()
	}
	seekKey := kv.InternalKey(kv.CFDefault, []byte("k10"), 1)
	tblIter.Seek(seekKey)
	if tblIter.Valid() {
		_ = tblIter.Item()
	}
	tblIter.Next()
	_ = tblIter.Valid()
	if err := tblIter.Close(); err != nil {
		t.Fatalf("iterator close: %v", err)
	}

	it = tbl.NewIterator(&index.Options{IsAsc: false})
	tblIter = it.(*Iterator)
	tblIter.Rewind()
	if tblIter.Valid() {
		_ = tblIter.Item()
	}
	tblIter.Seek(seekKey)
	_ = tblIter.Valid()
	_ = tblIter.Close()
}
