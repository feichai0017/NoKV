package lsm

import (
	"bytes"
	"testing"

	"github.com/feichai0017/NoKV/engine/index"
	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/lsm/table"
	"github.com/feichai0017/NoKV/engine/vfs"
	"github.com/stretchr/testify/require"
)

func splitIterUserKey(t *testing.T, internal []byte) []byte {
	t.Helper()
	_, userKey, _, ok := kv.SplitInternalKey(internal)
	require.True(t, ok)
	return userKey
}

func TestLSMNewIterators(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	entry := newRandomTestEntry()
	require.NoError(t, lsm.Set(entry))

	iters := lsm.NewIterators(&index.Options{IsAsc: true})
	require.NotEmpty(t, iters)

	levelIters := lsm.levels.iterators(&index.Options{IsAsc: true})
	for _, it := range levelIters {
		if it == nil {
			continue
		}
		it.Rewind()
		_ = it.Close()
	}

	for _, it := range iters {
		if it == nil {
			continue
		}
		it.Rewind()
		if it.Valid() {
			require.NotNil(t, it.Item())
			it.Next()
		}
		it.Seek(entry.Key)
		_ = it.Close()
	}
}

func TestBlockIteratorReverse(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builder := table.NewBuilder(tableOptionsFor(&builderOpt))

	for i := range 10 {
		key := kv.InternalKey(kv.CFDefault, []byte{byte('a' + i)}, 1)
		value := []byte{byte('v'), byte('0' + i)}
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := vfs.FileNameSSTable(lsm.option.WorkDir, 1)
	tbl, err := table.Open(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	forwardIter := tbl.NewIterator(&index.Options{IsAsc: true})
	defer func() { require.NoError(t, forwardIter.Close()) }()

	var forwardKeys []byte
	forwardIter.Rewind()
	for forwardIter.Valid() {
		e := forwardIter.Item().Entry()
		forwardKeys = append(forwardKeys, splitIterUserKey(t, e.Key)[0])
		forwardIter.Next()
	}
	require.Equal(t, "abcdefghij", string(forwardKeys))

	reverseIter := tbl.NewIterator(&index.Options{IsAsc: false})
	defer func() { require.NoError(t, reverseIter.Close()) }()

	var reverseKeys []byte
	reverseIter.Rewind()
	for reverseIter.Valid() {
		e := reverseIter.Item().Entry()
		reverseKeys = append(reverseKeys, splitIterUserKey(t, e.Key)[0])
		reverseIter.Next()
	}
	require.Equal(t, "jihgfedcba", string(reverseKeys))
}

func TestTableIteratorReverseSeek(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builder := table.NewBuilder(tableOptionsFor(&builderOpt))

	for i := range 20 {
		key := kv.InternalKey(kv.CFDefault, []byte{byte('a' + i)}, 1)
		value := bytes.Repeat([]byte{byte('v'), byte('0' + i%10)}, 24)
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := vfs.FileNameSSTable(lsm.option.WorkDir, 2)
	tbl, err := table.Open(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	forwardIter := tbl.NewIterator(&index.Options{IsAsc: true})
	defer func() { require.NoError(t, forwardIter.Close()) }()

	forwardIter.Seek(kv.InternalKey(kv.CFDefault, []byte{'e'}, 1))
	require.True(t, forwardIter.Valid())
	e := forwardIter.Item().Entry()
	require.Equal(t, byte('e'), splitIterUserKey(t, e.Key)[0])

	reverseIter := tbl.NewIterator(&index.Options{IsAsc: false})
	defer func() { require.NoError(t, reverseIter.Close()) }()

	reverseIter.Seek(kv.InternalKey(kv.CFDefault, []byte{'e'}, 1))
	require.True(t, reverseIter.Valid())
	e = reverseIter.Item().Entry()
	require.Equal(t, byte('e'), splitIterUserKey(t, e.Key)[0])

	var keys []byte
	for i := 0; i < 5 && reverseIter.Valid(); i++ {
		e := reverseIter.Item().Entry()
		keys = append(keys, splitIterUserKey(t, e.Key)[0])
		reverseIter.Next()
	}
	require.Equal(t, "edcba", string(keys))
}

func TestTableIteratorReverseMultiBlock(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	defer func() { _ = lsm.Close() }()

	builderOpt := *opt
	builderOpt.BlockSize = 64
	builder := table.NewBuilder(tableOptionsFor(&builderOpt))

	for i := range 30 {
		key := kv.InternalKey(kv.CFDefault, []byte{byte('a' + i)}, 1)
		value := bytes.Repeat([]byte{byte('v')}, 48)
		builder.AddKey(kv.NewEntry(key, value))
	}

	tableName := vfs.FileNameSSTable(lsm.option.WorkDir, 3)
	tbl, err := table.Open(lsm.levels, tableName, builder)
	require.NoError(t, err)
	require.NotNil(t, tbl)
	defer func() { _ = tbl.DecrRef() }()

	reverseIter := tbl.NewIterator(&index.Options{IsAsc: false})
	defer func() { require.NoError(t, reverseIter.Close()) }()

	reverseIter.Rewind()
	require.True(t, reverseIter.Valid())

	var keys []byte
	count := 0
	for reverseIter.Valid() && count < 30 {
		e := reverseIter.Item().Entry()
		keys = append(keys, splitIterUserKey(t, e.Key)[0])
		reverseIter.Next()
		count++
	}

	require.Equal(t, 30, len(keys))
	for i := range 30 {
		expected := byte('a' + 29 - i)
		require.Equal(t, expected, keys[i], "key at position %d should be %c, got %c", i, expected, keys[i])
	}
}
