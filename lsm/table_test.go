package lsm

import (
	"fmt"
	"os"
	"testing"

	"github.com/feichai0017/NoKV/kv"
	"github.com/feichai0017/NoKV/manifest"
	"github.com/feichai0017/NoKV/utils"
	"github.com/feichai0017/NoKV/wal"
	"github.com/stretchr/testify/require"
)

func buildTestLSM(t *testing.T, opt *Options) *LSM {
	c := make(chan map[manifest.ValueLogID]int64, 16)
	opt.DiscardStatsCh = &c
	wlog, err := wal.Open(wal.Config{Dir: opt.WorkDir})
	require.NoError(t, err)
	lsm, err := NewLSM(opt, wlog)
	require.NoError(t, err)
	return lsm
}

func splitTableUserKey(t *testing.T, internal []byte) []byte {
	t.Helper()
	_, userKey, _, ok := kv.SplitInternalKey(internal)
	require.True(t, ok)
	return userKey
}

// TestTableReverseIteration tests reverse iteration behavior on a single table.
func TestTableReverseIteration(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-test")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          4 << 10,
		BloomFalsePositive: 0.01,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	builder := newTableBuiler(opt)
	for i := range 10 {
		key := []byte{byte('a' + i)}
		builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 1), []byte("value")))
	}

	tableName := utils.FileNameSSTable(dir, 1)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("reverse iteration with Rewind", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Rewind()
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, splitTableUserKey(t, it.Item().Entry().Key)...)
		}
		require.Equal(t, "jihgfedcba", string(keys))
	})

	t.Run("reverse iteration with Seek", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
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
		it := tbl.NewIterator(&utils.Options{IsAsc: true})
		defer func() { _ = it.Close() }()

		it.Rewind()
		var keys []byte
		for ; it.Valid(); it.Next() {
			keys = append(keys, splitTableUserKey(t, it.Item().Entry().Key)...)
		}
		require.Equal(t, "abcdefghij", string(keys))
	})

	t.Run("reverse seek to first key", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.InternalKey(kv.CFDefault, []byte("a"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("a"), splitTableUserKey(t, it.Item().Entry().Key))
		it.Next()
		require.False(t, it.Valid())
	})

	t.Run("reverse seek to last key", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
		defer func() { _ = it.Close() }()

		it.Seek(kv.InternalKey(kv.CFDefault, []byte("j"), 1))
		require.True(t, it.Valid())
		require.Equal(t, []byte("j"), splitTableUserKey(t, it.Item().Entry().Key))
	})
}

// TestTableReverseIterationMultiBlock tests reverse iteration across multiple blocks.
func TestTableReverseIterationMultiBlock(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-table-multiblock")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          128, // Force multiple blocks.
		BloomFalsePositive: 0.01,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	builder := newTableBuiler(opt)
	for i := range 20 {
		key := []byte{byte('a' + i)}
		builder.AddKey(kv.NewEntry(kv.InternalKey(kv.CFDefault, key, 1), []byte("value-with-more-data")))
	}

	tableName := utils.FileNameSSTable(dir, 2)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("reverse across multiple blocks", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{IsAsc: false})
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
		it := tbl.NewIterator(&utils.Options{IsAsc: true})
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

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          128, // force many blocks; reproduces cross-block seek edge.
		BloomFalsePositive: 0.0,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	builder := newTableBuiler(opt)
	const total = 500
	for i := range total {
		userKey := fmt.Appendf(nil, "k%06d", i)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
			[]byte("value"),
		))
	}

	tableName := utils.FileNameSSTable(dir, 3)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	for i := range total {
		userKey := fmt.Appendf(nil, "k%06d", i)
		maxVs := uint64(0)
		entry, err := tbl.Search(kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion), &maxVs)
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

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          128,
		BloomFalsePositive: 0.0,
	}

	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	builder := newTableBuiler(opt)
	for i := range 200 {
		key := fmt.Appendf(nil, "k%03d", i)
		builder.AddKey(kv.NewEntry(
			kv.InternalKey(kv.CFDefault, key, 1),
			[]byte("value-with-more-data"),
		))
	}

	tableName := utils.FileNameSSTable(dir, 4)
	tbl, err := openTable(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	t.Run("forward bounded range", func(t *testing.T) {
		it := tbl.NewIterator(&utils.Options{
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
		it := tbl.NewIterator(&utils.Options{
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

func TestLevelGetHandlesOverlappingRanges(t *testing.T) {
	dir, err := os.MkdirTemp("", "nokv-level-overlap")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(dir)) }()

	opt := &Options{
		WorkDir:            dir,
		MemTableSize:       1 << 20,
		SSTableMaxSz:       1 << 20,
		BlockSize:          4 << 10,
		BloomFalsePositive: 0.0,
		MaxLevelNum:        utils.MaxLevelNum,
	}
	lsm := buildTestLSM(t, opt)
	defer func() { require.NoError(t, lsm.Close()) }()

	buildTable := func(fid uint64, keys ...string) *table {
		builder := newTableBuiler(opt)
		for _, k := range keys {
			builder.AddKey(kv.NewEntry(
				kv.InternalKey(kv.CFDefault, []byte(k), 1),
				[]byte("v-"+k),
			))
		}
		name := utils.FileNameSSTable(dir, fid)
		tbl, err := openTable(lsm.levels, name, builder)
		require.NoError(t, err)
		require.NotNil(t, tbl)
		return tbl
	}

	// Two tables overlap on user-key ranges:
	// t1 covers [a, c], t2 covers [b, d]. Key "b" only exists in t2.
	t1 := buildTable(11, "a", "c")
	t2 := buildTable(12, "b", "d")
	defer func() { _ = t1.DecrRef() }()
	defer func() { _ = t2.DecrRef() }()

	lh := lsm.levels.levels[6]
	lh.add(t1)
	lh.add(t2)
	lh.Sort()

	query := kv.InternalKey(kv.CFDefault, []byte("b"), kv.MaxVersion)
	entry, err := lh.Get(query)
	require.NoError(t, err)
	require.NotNil(t, entry)
	require.Equal(t, []byte("b"), splitTableUserKey(t, entry.Key))
	entry.DecrRef()
}

func TestTableDecrRefUnderflow(t *testing.T) {
	tbl := &table{fid: 1}
	tbl.ref.Store(2)
	require.NoError(t, tbl.DecrRef())
	require.Equal(t, int32(1), tbl.ref.Load())

	// Avoid the 1->0 path in this unit test (which requires a real table handle).
	tbl.ref.Store(0)
	require.PanicsWithError(t, fmt.Errorf("table refcount underflow: fid %d, current_ref %d", tbl.fid, int32(0)).Error(), func() {
		_ = tbl.DecrRef()
	})
}
