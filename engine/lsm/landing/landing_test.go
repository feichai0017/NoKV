// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package landing

import (
	"testing"
	"time"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

// fakeTable implements Table using fixed-byte fields. It supports a stub
// Search that returns the stored entry when key matches userKey, otherwise
// utils.ErrKeyNotFound.
type fakeTable struct {
	id        uint64
	min       []byte
	max       []byte
	size      int64
	valueSize uint64
	maxVer    uint64
	createdAt time.Time
	userKey   []byte
	value     []byte
	version   uint64
	released  bool
}

func (t *fakeTable) MinKey() []byte        { return t.min }
func (t *fakeTable) MaxKey() []byte        { return t.max }
func (t *fakeTable) Size() int64           { return t.size }
func (t *fakeTable) ValueSize() uint64     { return t.valueSize }
func (t *fakeTable) MaxVersionVal() uint64 { return t.maxVer }
func (t *fakeTable) FID() uint64           { return t.id }
func (t *fakeTable) CreatedAt() time.Time  { return t.createdAt }
func (t *fakeTable) DecrRef() error        { t.released = true; return nil }

func (t *fakeTable) Search(key []byte, maxVs uint64) (*kv.Entry, uint64, error) {
	_, userKey, _, ok := kv.SplitInternalKey(key)
	if !ok {
		return nil, maxVs, utils.ErrKeyNotFound
	}
	if string(userKey) != string(t.userKey) {
		return nil, maxVs, utils.ErrKeyNotFound
	}
	if t.version <= maxVs {
		return nil, maxVs, utils.ErrKeyNotFound
	}
	entry := kv.NewEntry(kv.SafeCopy(nil, key), kv.SafeCopy(nil, t.value))
	entry.Version = t.version
	return entry, t.version, nil
}

func ikey(s string, ts uint64) []byte {
	return kv.InternalKey(kv.CFDefault, []byte(s), ts)
}

func newTable(id uint64, key string, version uint64, value string) *fakeTable {
	return &fakeTable{
		id:        id,
		min:       ikey(key, version),
		max:       ikey(key, version),
		size:      100,
		valueSize: uint64(len(value)),
		maxVer:    version,
		createdAt: time.Now().Add(-time.Minute),
		userKey:   []byte(key),
		value:     []byte(value),
		version:   version,
	}
}

func TestShardIndexPartitionsByFirstByte(t *testing.T) {
	require.Equal(t, 0, ShardIndex(ikey("\x00aa", 1)))
	require.Equal(t, 3, ShardIndex(ikey("\xffzz", 1)))

	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic on non-internal key")
		}
	}()
	ShardIndex([]byte("not-an-internal-key"))
}

func TestBufferAddAndQueries(t *testing.T) {
	var buf Buffer[*fakeTable]
	t1 := newTable(1, "apple", 5, "v1")
	t2 := newTable(2, "banana", 6, "v2")
	t3 := newTable(3, "cherry", 7, "v3")
	buf.Add(t1)
	buf.AddBatch([]*fakeTable{t2, t3})

	require.Equal(t, 3, buf.TableCount())
	require.Equal(t, int64(300), buf.TotalSize())
	require.Equal(t, int64(6), buf.TotalValueSize())
	require.Greater(t, buf.MaxAgeSeconds(), 0.0)

	all := buf.AllTables()
	require.Len(t, all, 3)

	views := buf.ShardViews()
	require.NotEmpty(t, views)
	for _, v := range views {
		require.Greater(t, v.SizeBytes, int64(0))
	}
}

func TestBufferRemove(t *testing.T) {
	var buf Buffer[*fakeTable]
	t1 := newTable(1, "apple", 5, "v1")
	t2 := newTable(2, "banana", 6, "v2")
	buf.AddBatch([]*fakeTable{t1, t2})

	require.Equal(t, 2, buf.TableCount())
	buf.Remove(map[uint64]struct{}{1: {}})
	require.Equal(t, 1, buf.TableCount())
	require.Equal(t, int64(100), buf.TotalSize())
}

func TestBufferShardTablesByIndex(t *testing.T) {
	var buf Buffer[*fakeTable]
	t1 := newTable(1, "\x00aa", 5, "v1") // shard 0
	t2 := newTable(2, "\xffaa", 6, "v2") // shard 3
	buf.AddBatch([]*fakeTable{t1, t2})

	shard0 := buf.ShardTablesByIndex(0)
	require.Len(t, shard0, 1)
	require.Equal(t, uint64(1), shard0[0].FID())

	shard3 := buf.ShardTablesByIndex(3)
	require.Len(t, shard3, 1)
	require.Equal(t, uint64(2), shard3[0].FID())

	require.Nil(t, buf.ShardTablesByIndex(-1))
	require.Nil(t, buf.ShardTablesByIndex(99))
}

func TestBufferSortShardsAndSearch(t *testing.T) {
	var buf Buffer[*fakeTable]
	tA := newTable(1, "apple", 5, "v-apple")
	tB := newTable(2, "banana", 6, "v-banana")
	tC := newTable(3, "cherry", 7, "v-cherry")
	// Insert out of order.
	buf.AddBatch([]*fakeTable{tC, tA, tB})
	buf.SortShards()

	entry, _, err := buf.Search(ikey("banana", 0), 0)
	require.NoError(t, err)
	require.Equal(t, []byte("v-banana"), entry.Value)
	entry.DecrRef()

	_, _, err = buf.Search(ikey("nonexistent", 0), 0)
	require.Equal(t, utils.ErrKeyNotFound, err)
}

func TestBufferTablesWithinBounds(t *testing.T) {
	var buf Buffer[*fakeTable]
	tA := newTable(1, "apple", 5, "va")
	tB := newTable(2, "banana", 6, "vb")
	tC := newTable(3, "cherry", 7, "vc")
	buf.AddBatch([]*fakeTable{tA, tB, tC})
	buf.SortShards()

	hits := buf.TablesWithinBounds(ikey("b", 1), ikey("d", 1))
	require.Len(t, hits, 2)
}

func TestBufferEnsureInitIdempotent(t *testing.T) {
	var buf Buffer[*fakeTable]
	buf.EnsureInit()
	count1 := buf.TableCount()
	buf.EnsureInit()
	require.Equal(t, count1, buf.TableCount())
}

func TestEmptyBufferQueriesReturnZero(t *testing.T) {
	var buf Buffer[*fakeTable]
	require.Equal(t, 0, buf.TableCount())
	require.Equal(t, int64(0), buf.TotalSize())
	require.Equal(t, int64(0), buf.TotalValueSize())
	require.Equal(t, 0.0, buf.MaxAgeSeconds())
	require.Nil(t, buf.AllTables())
	require.Empty(t, buf.ShardViews())

	_, _, err := buf.Search(ikey("k", 0), 0)
	require.Equal(t, utils.ErrKeyNotFound, err)
}
