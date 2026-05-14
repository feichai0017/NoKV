// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package table

import (
	"bytes"
	"fmt"
	"os"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	cachepkg "github.com/feichai0017/NoKV/engine/lsm/cache"
	"github.com/feichai0017/NoKV/engine/vfs"
	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"github.com/stretchr/testify/require"
)

func splitTableUserKey(t *testing.T, internal []byte) []byte {
	t.Helper()
	_, userKey, _, ok := kv.SplitInternalKey(internal)
	require.True(t, ok)
	return userKey
}

func newTestOptions(workDir string) Options {
	return Options{
		WorkDir:            workDir,
		SSTableMaxSize:     1 << 20,
		BlockSize:          4 << 10,
		BloomFalsePositive: 0.01,
	}
}

// TestTableReverseIteration exercises reverse iteration over a single table.
func TestTableReverseIteration(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	rt := newTestRuntime(newTestOptions(dir))
	builder := NewBuilder(rt.Options())
	for i := range 10 {
		key := []byte{byte('a' + i)}
		builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 1), []byte("value")))
	}

	tableName := vfs.FileNameSSTable(dir, 1)
	tbl, err := Open(rt, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("reverse iteration with Rewind", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Rewind()
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, splitTableUserKey(t, it.Item().Entry().Key)...)
		}
		require.Equal(t, "jihgfedcba", string(keys))
	})

	t.Run("reverse iteration with Seek", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.InternalKey(kv.CFDefault, []byte("f"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("f"), splitTableUserKey(t, it.Item().Entry().Key))

		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, splitTableUserKey(t, it.Item().Entry().Key)...)
		}
		require.Equal(t, "fedcba", string(keys))
	})

	t.Run("forward iteration for comparison", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		it.Rewind()
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, splitTableUserKey(t, it.Item().Entry().Key)...)
		}
		require.Equal(t, "abcdefghij", string(keys))
	})

	t.Run("reverse seek to first key", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.InternalKey(kv.CFDefault, []byte("a"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("a"), splitTableUserKey(t, it.Item().Entry().Key))
		it.Next()
		require.False(t, it.Valid())
	})

	t.Run("reverse seek to last key", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.InternalKey(kv.CFDefault, []byte("j"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("j"), splitTableUserKey(t, it.Item().Entry().Key))
	})
}

func TestTableSearchCompressedBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-compressed")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opts := newTestOptions(dir)
	opts.BlockCompression = CompressionSnappy
	rt := newTestRuntime(opts)
	builder := NewBuilder(rt.Options())
	key := []byte("compressed-key")
	builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 7), bytes.Repeat([]byte("metadata-value-"), 32)))

	tableName := vfs.FileNameSSTable(dir, 10)
	tbl, err := Open(rt, tableName, builder)
	require.NoError(t, err)
	defer func() { _ = tbl.DecrRef() }()

	got, maxVs, err := tbl.Search(kv.InternalKey(kv.CFDefault, key, 7), 0)
	require.NoError(t, err)
	require.NotNil(t, got)
	defer got.DecrRef()
	require.Equal(t, uint64(7), maxVs)
	require.Equal(t, bytes.Repeat([]byte("metadata-value-"), 32), got.Value)
}

func TestTableReverseIterationMultiBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-multiblock")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opts := newTestOptions(dir)
	opts.BlockSize = 128 // force multiple blocks
	rt := newTestRuntime(opts)
	builder := NewBuilder(rt.Options())
	for i := range 20 {
		key := []byte{byte('a' + i)}
		builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 1), []byte("value-with-more-data")))
	}

	tableName := vfs.FileNameSSTable(dir, 2)
	tbl, err := Open(rt, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("reverse across multiple blocks", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Rewind()
		count := 0
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, splitTableUserKey(t, it.Item().Entry().Key)...)
			count++
		}
		require.Equal(t, 20, count)
		require.Equal(t, byte('t'), keys[0])
		require.Equal(t, byte('a'), keys[19])
		for i := range 19 {
			require.Greater(t, keys[i], keys[i+1], "keys should be in descending order")
		}
	})

	t.Run("forward across multiple blocks", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		it.Rewind()
		count := 0
		for ; it.Valid(); it.Next() {
			count++
		}
		require.Equal(t, 20, count)
	})
}

func TestTableSearchMaxVersionAcrossBlocks(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-search-maxver")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opts := newTestOptions(dir)
	opts.BlockSize = 128
	opts.BloomFalsePositive = 0.0
	rt := newTestRuntime(opts)
	builder := NewBuilder(rt.Options())
	const total = 500
	for i := range total {
		userKey := fmt.Appendf(nil, "k%06d", i)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
			[]byte("value"),
		))
	}

	tableName := vfs.FileNameSSTable(dir, 3)
	tbl, err := Open(rt, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	for i := range total {
		userKey := fmt.Appendf(nil, "k%06d", i)
		entry, maxVs, err := tbl.Search(kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion), 0)
		require.NoError(t, err)
		require.NotNil(t, entry)
		require.Equal(t, userKey, splitTableUserKey(t, entry.Key))
		require.Equal(t, uint64(i+1), maxVs)
		entry.DecrRef()
	}
}

func TestTableIteratorBoundsAcrossBlocks(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-bounds")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opts := newTestOptions(dir)
	opts.BlockSize = 128
	opts.BloomFalsePositive = 0.0
	rt := newTestRuntime(opts)
	builder := NewBuilder(rt.Options())
	for i := range 200 {
		key := fmt.Appendf(nil, "k%03d", i)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, key, 1),
			[]byte("value-with-more-data"),
		))
	}

	tableName := vfs.FileNameSSTable(dir, 4)
	tbl, err := Open(rt, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("forward bounded range", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{
			IsAsc:      true,
			LowerBound: []byte("k050"),
			UpperBound: []byte("k060"),
		})
		defer func() { _ = it.Close() }()

		var keys []string
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(splitTableUserKey(t, it.Item().Entry().Key)))
		}
		require.Len(t, keys, 10)
		require.Equal(t, "k050", keys[0])
		require.Equal(t, "k059", keys[len(keys)-1])
	})

	t.Run("reverse bounded range", func(t *testing.T) {
		it := tbl.NewIterator(&index.Options{
			IsAsc:      false,
			LowerBound: []byte("k050"),
			UpperBound: []byte("k060"),
		})
		defer func() { _ = it.Close() }()

		var keys []string
		for it.Rewind(); it.Valid(); it.Next() {
			keys = append(keys, string(splitTableUserKey(t, it.Item().Entry().Key)))
		}
		require.Len(t, keys, 10)
		require.Equal(t, "k059", keys[0])
		require.Equal(t, "k050", keys[len(keys)-1])
	})
}

func TestBlockRangeForBoundsUsesBaseKeyOrdering(t *testing.T) {
	idx := &storagepb.TableIndex{
		Offsets: []*storagepb.BlockOffset{
			{Key: kv.InternalKey(kv.CFDefault, []byte("z"), 5)},
			{Key: kv.InternalKey(kv.CFLock, []byte("a"), 5)},
			{Key: kv.InternalKey(kv.CFWrite, []byte("b"), 5)},
			{Key: kv.InternalKey(kv.CFWrite, []byte("f"), 5)},
		},
	}

	start, end := blockRangeForBounds(
		idx,
		kv.InternalKey(kv.CFWrite, []byte("a"), kv.MaxVersion),
		kv.InternalKey(kv.CFWrite, []byte("e"), kv.MaxVersion),
	)
	require.Equal(t, 1, start)
	require.Equal(t, 3, end)
}

func TestTableIteratorSingleKeyRespectsForwardBounds(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-single-bound")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opts := newTestOptions(dir)
	opts.BloomFalsePositive = 0.0
	rt := newTestRuntime(opts)
	builder := NewBuilder(rt.Options())
	builder.AddKey(kv.NewEntry(
		kv.InternalKey(kv.CFDefault, []byte("k00000010"), 1),
		[]byte("value"),
	))

	tableName := vfs.FileNameSSTable(dir, 40)
	tbl, err := Open(rt, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	it := tbl.NewIterator(&index.Options{
		IsAsc:      true,
		LowerBound: []byte("k00000010"),
		UpperBound: []byte("k00000011"),
	})
	defer func() { _ = it.Close() }()

	it.Rewind()
	require.True(t, it.Valid())
	require.Equal(t, []byte("k00000010"), splitTableUserKey(t, it.Item().Entry().Key))
	it.Next()
	require.False(t, it.Valid())
}

func TestTableDecrRefUnderflow(t *testing.T) {
	tbl := &Table{fid: 1}
	tbl.Init(2)
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, int32(1), tbl.Load())

	// Avoid the 1->0 path in this unit test (which requires a real table handle).
	tbl.Reset()
	require.Panics(t, func() {
		_ = tbl.DecrRef()
	})
}

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

type testRuntime struct {
	opts  Options
	cache *cachepkg.Cache
}

func newTestRuntime(opts Options) *testRuntime {
	c := cachepkg.New(cachepkg.Options{IndexBytes: 1 << 20, BlockBytes: 1 << 20})
	return &testRuntime{opts: opts, cache: c}
}

func (r *testRuntime) Cache() *cachepkg.Cache { return r.cache }
func (r *testRuntime) Options() Options       { return r.opts }
