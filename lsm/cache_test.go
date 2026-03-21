package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestCacheHotColdMetrics(t *testing.T) {
	opt := &Options{
		BlockCacheBytes: 256,
	}
	cache := newCache(opt)
	if cache == nil {
		t.Fatalf("expected cache to initialize")
		return
	}
	if cache.blocks == nil || cache.blocks.rc == nil {
		t.Fatalf("expected block cache to initialize")
		return
	}

	tbl := &table{}
	blk := &block{tbl: tbl}
	cache.addBlock(0, tbl, 1, blk)
	cache.blocks.rc.Wait()
	cache.getBlock(0, 1)
	// Miss on different key.
	cache.getBlock(0, 2)

	cache.addBlock(1, tbl, 42, &block{tbl: tbl})
	cache.blocks.rc.Wait()
	cache.getBlock(1, 42)
	cache.getBlock(1, 43)

	metrics := cache.metricsSnapshot()
	if metrics.L0Misses == 0 {
		t.Fatalf("unexpected L0 metrics: %+v", metrics)
	}
	if metrics.L1Misses == 0 {
		t.Fatalf("unexpected L1 metrics: %+v", metrics)
	}
}

func TestCacheIndex(t *testing.T) {
	opt := &Options{
		IndexCacheBytes: 1 << 20,
	}
	c := newCache(opt)
	idx := &pb.TableIndex{KeyCount: 10}
	c.addIndex(1, idx)
	got, ok := c.getIndex(1)
	require.True(t, ok)
	require.Equal(t, uint32(10), got.GetKeyCount())

	c.delIndex(1)
	_, _ = c.getIndex(1)

	_ = c.metricsSnapshot()
	require.NoError(t, c.close())
}

func TestBlockCacheOperations(t *testing.T) {
	bc := newBlockCache(1 << 20)
	blk := &block{data: []byte("data")}

	bc.add(2, nil, 1, blk)
	bc.add(0, nil, 1, blk)
	bc.rc.Wait()
	_, _ = bc.get(1)

	bc.close()
}

func TestIndexCacheEvictionByBudget(t *testing.T) {
	idx1 := &pb.TableIndex{KeyCount: 1, BloomFilter: make([]byte, 64)}
	idx2 := &pb.TableIndex{KeyCount: 2, BloomFilter: make([]byte, 64)}
	budget := int64(proto.Size(idx1))
	ic := newIndexCache(budget)
	require.NotNil(t, ic)

	ic.Set(1, idx1)
	ic.Set(2, idx2)

	_, ok := ic.Get(1)
	require.False(t, ok)
	val, ok := ic.Get(2)
	require.True(t, ok)
	got := val.(*pb.TableIndex)
	require.Equal(t, uint32(2), got.GetKeyCount())
}

func TestBlockEntryReleaseNilAndNoTable(t *testing.T) {
	var nilEntry *blockEntry
	nilEntry.release()

	entryWithoutTable := &blockEntry{}
	entryWithoutTable.release()
}
