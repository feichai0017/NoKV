package lsm

import (
	"testing"

	"github.com/feichai0017/NoKV/utils"
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
