// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package cache

import (
	"testing"

	storagepb "github.com/feichai0017/NoKV/pb/storage"
)

// BenchmarkAddBlock measures the cost of inserting a block into the cache.
// AddBlock runs on every cold-block read, so its overhead shows up under
// scan-heavy workloads.
func BenchmarkAddBlock(b *testing.B) {
	c := New(Options{BlockBytes: 64 << 20})
	tbl := fakeTable{}
	payload := make([]byte, 4<<10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AddBlock(0, tbl, uint64(i), Block{DiskData: payload})
	}
}

// BenchmarkGetBlockHit measures the cost of a cache hit on the block path.
func BenchmarkGetBlockHit(b *testing.B) {
	c := New(Options{BlockBytes: 1 << 20})
	tbl := fakeTable{}
	c.AddBlock(0, tbl, 1, Block{DiskData: []byte("hot")})
	c.Wait()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetBlock(0, 1)
	}
}

// BenchmarkGetBlockMiss measures the cost of a cache miss.
func BenchmarkGetBlockMiss(b *testing.B) {
	c := New(Options{BlockBytes: 1 << 20})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetBlock(0, uint64(i+1))
	}
}

// BenchmarkAddIndex measures index-cache writes; called once per opened SST.
func BenchmarkAddIndex(b *testing.B) {
	c := New(Options{IndexBytes: 64 << 20})
	idx := &storagepb.TableIndex{KeyCount: 1024}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.AddIndex(uint64(i), idx)
	}
}

// BenchmarkGetIndexHit measures the index-cache hot path used on every read.
func BenchmarkGetIndexHit(b *testing.B) {
	c := New(Options{IndexBytes: 1 << 20})
	idx := &storagepb.TableIndex{KeyCount: 1024}
	c.AddIndex(1, idx)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = c.GetIndex(1)
	}
}
