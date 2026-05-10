package cache

import (
	"testing"

	storagepb "github.com/feichai0017/NoKV/pb/storage"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

type fakeTable struct{}

func (fakeTable) IncrRef()         {}
func (fakeTable) DecrRef() error   { return nil }

func TestCacheHotColdMetrics(t *testing.T) {
	c := New(Options{BlockBytes: 256})
	require.NotNil(t, c)

	tbl := fakeTable{}
	c.AddBlock(0, tbl, 1, Block{DiskData: []byte("d1")})
	c.Wait()
	c.GetBlock(0, 1)
	c.GetBlock(0, 2)

	c.AddBlock(1, tbl, 42, Block{DiskData: []byte("d2")})
	c.Wait()
	c.GetBlock(1, 42)
	c.GetBlock(1, 43)

	stats := c.MetricsSnapshot()
	require.NotZero(t, stats.L0Misses)
	require.NotZero(t, stats.L1Misses)
}

func TestCacheIndex(t *testing.T) {
	c := New(Options{IndexBytes: 1 << 20})
	idx := &storagepb.TableIndex{KeyCount: 10}
	c.AddIndex(1, idx)
	got, ok := c.GetIndex(1)
	require.True(t, ok)
	require.Equal(t, uint32(10), got.GetKeyCount())

	c.DelIndex(1)
	_, _ = c.GetIndex(1)

	_ = c.MetricsSnapshot()
	require.NoError(t, c.Close())
}

func TestBlockCacheOperations(t *testing.T) {
	bc := newBlockCache(1 << 20)

	bc.add(2, nil, 1, Block{DiskData: []byte("data")})
	bc.add(0, nil, 1, Block{DiskData: []byte("data")})
	for _, sh := range bc.shards {
		if sh.rc != nil {
			sh.rc.Wait()
		}
	}
	_, _ = bc.get(1)

	bc.close()
}

func TestBlockCacheShardsKeys(t *testing.T) {
	bc := newBlockCache(1 << 20)
	require.NotNil(t, bc)
	require.GreaterOrEqual(t, len(bc.shards), 1)

	bc.add(0, nil, 1, Block{DiskData: []byte("one")})
	bc.add(0, nil, 2, Block{DiskData: []byte("two")})
	for _, sh := range bc.shards {
		if sh.rc != nil {
			sh.rc.Wait()
		}
	}
	_, ok1 := bc.get(1)
	_, ok2 := bc.get(2)
	require.True(t, ok1)
	require.True(t, ok2)
	bc.close()
}

func TestBlockCacheStoresCompressedPayload(t *testing.T) {
	bc := newBlockCache(1 << 20)
	require.NotNil(t, bc)
	bc.add(0, nil, 7, Block{
		DiskData:    []byte("zip"),
		Compression: 1,
		RawLen:      len("decoded-block-payload"),
	})
	for _, sh := range bc.shards {
		if sh.rc != nil {
			sh.rc.Wait()
		}
	}
	entry, ok := bc.get(7)
	require.True(t, ok)
	require.Equal(t, []byte("zip"), entry.DiskData)
	require.Equal(t, int64(3), entry.cost)
	require.Nil(t, entry.Tbl)
	bc.close()
}

func TestIndexCacheEvictionByBudget(t *testing.T) {
	idx1 := &storagepb.TableIndex{KeyCount: 1, BloomFilter: make([]byte, 64)}
	idx2 := &storagepb.TableIndex{KeyCount: 2, BloomFilter: make([]byte, 64)}
	budget := int64(proto.Size(idx1))
	ic := newIndexCache(budget)
	require.NotNil(t, ic)

	ic.Set(1, idx1)
	ic.Set(2, idx2)

	_, ok := ic.Get(1)
	require.False(t, ok)
	val, ok := ic.Get(2)
	require.True(t, ok)
	got := val.(*storagepb.TableIndex)
	require.Equal(t, uint32(2), got.GetKeyCount())
}

func TestEntryReleaseNilAndNoTable(t *testing.T) {
	var nilEntry *Entry
	nilEntry.release()

	entryWithoutTable := &Entry{}
	entryWithoutTable.release()
}
