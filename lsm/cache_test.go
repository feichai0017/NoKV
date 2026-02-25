package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/utils"
	"github.com/stretchr/testify/require"
)

func TestCacheHotColdMetrics(t *testing.T) {
	opt := &Options{
		BlockCacheSize: 4,
		BloomCacheSize: 4,
	}
	cache := newCache(opt)
	if cache == nil {
		t.Fatalf("expected cache to initialize")
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

	filter := utils.NewFilter([]uint32{utils.Hash([]byte("foo"))}, 10)
	cache.addBloom(7, filter)
	if _, ok := cache.getBloom(7); !ok {
		t.Fatalf("expected bloom hit")
	}
	cache.getBloom(8) // miss

	metrics := cache.metricsSnapshot()
	if metrics.L0Misses == 0 {
		t.Fatalf("unexpected L0 metrics: %+v", metrics)
	}
	if metrics.L1Misses == 0 {
		t.Fatalf("unexpected L1 metrics: %+v", metrics)
	}
	if metrics.BloomHits != 1 || metrics.BloomMisses != 1 {
		t.Fatalf("unexpected bloom metrics: %+v", metrics)
	}
}

func TestCacheIndexAndBloom(t *testing.T) {
	opt := &Options{
		BlockCacheSize: 0,
		BloomCacheSize: 2,
	}
	c := newCache(opt)
	idx := &pb.TableIndex{KeyCount: 10}
	c.addIndex(1, idx)
	got, ok := c.getIndex(1)
	require.True(t, ok)
	require.Equal(t, uint32(10), got.GetKeyCount())

	c.delIndex(1)
	_, _ = c.getIndex(1)

	filter := utils.Filter{0x01, 0x02}
	c.addBloom(1, filter)
	gotFilter, ok := c.getBloom(1)
	require.True(t, ok)
	require.Equal(t, filter, gotFilter)

	_ = c.metricsSnapshot()
	require.NoError(t, c.close())
}

func TestBlockCacheOperations(t *testing.T) {
	bc := newBlockCache(16)
	blk := &block{data: []byte("data")}

	bc.add(2, nil, 1, blk)
	bc.add(0, nil, 1, blk)
	bc.rc.Wait()
	_, _ = bc.get(1)

	bc.close()
}

func TestBloomCacheEviction(t *testing.T) {
	bc := newBloomCache(1)
	filter1 := utils.Filter{0x01}
	filter2 := utils.Filter{0x02}
	bc.add(1, filter1)
	bc.add(2, filter2)

	_, ok := bc.get(1)
	require.False(t, ok)
	got, ok := bc.get(2)
	require.True(t, ok)
	require.Equal(t, filter2, got)

	bc.close()
}

func TestBlockEntryReleaseNilAndNoTable(t *testing.T) {
	var nilEntry *blockEntry
	nilEntry.release()

	entryWithoutTable := &blockEntry{}
	entryWithoutTable.release()
}
