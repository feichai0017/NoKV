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
	cache.addBlock(0, tbl, 1, blk, true)
	cache.blocks.rc.Wait()
	if v, ok := cache.getBlock(0, 1, true); !ok || v == nil {
		t.Fatalf("expected hot block hit")
	}
	// Miss on different key.
	cache.getBlock(0, 2, false)

	cache.addBlock(1, tbl, 42, &block{tbl: tbl}, true)
	if v, ok := cache.getBlock(1, 42, true); !ok || v == nil {
		t.Fatalf("expected L1 block hit")
	}

	filter := utils.NewFilter([]uint32{utils.Hash([]byte("foo"))}, 10)
	cache.addBloom(7, filter)
	if _, ok := cache.getBloom(7); !ok {
		t.Fatalf("expected bloom hit")
	}
	cache.getBloom(8) // miss

	metrics := cache.metricsSnapshot()
	if metrics.L0Hits != 1 || metrics.L0Misses != 1 {
		t.Fatalf("unexpected L0 metrics: %+v", metrics)
	}
	if metrics.L1Hits != 1 {
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

	bc.addWithTier(2, nil, 1, blk, true)
	bc.addWithTier(0, nil, 1, blk, true)
	bc.rc.Wait()

	got, ok := bc.get(0, 1, true)
	require.True(t, ok)
	require.Equal(t, blk, got)

	entry := &blockEntry{key: 2, tbl: nil, blk: blk}
	bc.promoteHot(entry)
	bc.removeHotEntry(entry)
	entry.release()

	bc.close()
}

func TestBloomCacheEviction(t *testing.T) {
	bc := newBloomCache(1)
	bc.hot = nil
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
