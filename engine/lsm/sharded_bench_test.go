package lsm

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
	"github.com/feichai0017/NoKV/engine/wal"
)

// openShardedBenchLSM mirrors openShardHintTestLSM but takes *testing.B and
// uses a larger memtable so the bench loops do not trigger flush.
func openShardedBenchLSM(b *testing.B, shardCount int) (*LSM, []*wal.Manager) {
	b.Helper()
	dir := b.TempDir()
	opts := newTestLSMOptions(dir, nil)
	opts.MemTableSize = 64 << 20 // keep everything in memtable for a clean micro
	wals := make([]*wal.Manager, shardCount)
	for i := range wals {
		mgr, err := wal.Open(wal.Config{Dir: filepath.Join(dir, fmt.Sprintf("wal-%02d", i))})
		if err != nil {
			b.Fatalf("open wal %d: %v", i, err)
		}
		wals[i] = mgr
	}
	lsm, err := NewLSM(opts, wals)
	if err != nil {
		b.Fatalf("new lsm: %v", err)
	}
	b.Cleanup(func() {
		_ = lsm.Close()
		for _, mgr := range wals {
			_ = mgr.Close()
		}
	})
	return lsm, wals
}

// makeShardedBenchEntries fabricates internal-key entries with monotonically
// increasing versions so MVCC tiebreak is well-defined.
func makeShardedBenchEntries(count, valueSize int) []*kv.Entry {
	entries := make([]*kv.Entry, count)
	value := make([]byte, valueSize)
	for i := range entries {
		userKey := make([]byte, 16)
		copy(userKey, "shardbench")
		binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
		entries[i] = &kv.Entry{
			Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
			Value:   value,
			CF:      kv.CFDefault,
			Version: uint64(i + 1),
		}
	}
	return entries
}

// BenchmarkShardedSetBatchByShardCount measures writeSome / SetBatchGroup
// throughput as the data plane shard count varies. The driver keeps
// per-iteration work fixed (one batch of K entries) and pins to shardID=0
// so we observe per-shard hot path cost without dispatcher / channel
// overhead. Going from N=1 to N>1 also exercises the per-shard lock + WAL
// Manager regardless of dispatch.
func BenchmarkShardedSetBatchByShardCount(b *testing.B) {
	const batchSize = 64
	const valueSize = 128
	for _, shardCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("shards_%d", shardCount), func(b *testing.B) {
			lsm, _ := openShardedBenchLSM(b, shardCount)
			entries := makeShardedBenchEntries(batchSize, valueSize)
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * valueSize))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := lsm.SetBatchGroup(0, [][]*kv.Entry{entries}); err != nil {
					b.Fatalf("set batch group: %v", err)
				}
			}
		})
	}
}

// BenchmarkShardedGetWithHint compares cross-shard memtable Get with and
// without the hint cache. The "warm_hint" sub-benchmark seeds the hint
// table with the right shardID before timing; "cold_hint" forces a full
// N-way memtable walk. The delta is the headline number for PR #160.
func BenchmarkShardedGetWithHint(b *testing.B) {
	const valueSize = 128
	keys := func(n int) [][]byte {
		out := make([][]byte, n)
		for i := range out {
			userKey := make([]byte, 16)
			copy(userKey, "hintbench")
			binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
			out[i] = kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
		}
		return out
	}

	for _, shardCount := range []int{1, 4, 8} {
		for _, mode := range []string{"warm_hint", "cold_hint"} {
			b.Run(fmt.Sprintf("shards_%d/%s", shardCount, mode), func(b *testing.B) {
				lsm, _ := openShardedBenchLSM(b, shardCount)
				const seedKeys = 4096
				lookups := keys(seedKeys)
				value := make([]byte, valueSize)

				// Distribute writes across shards via per-key affinity.
				for i, k := range lookups {
					_, userKey, _, _ := kv.SplitInternalKey(k)
					targetShard := int(rand.Uint32()) & (shardCount - 1)
					entry := &kv.Entry{
						Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
						Value:   value,
						CF:      kv.CFDefault,
						Version: uint64(i + 1),
					}
					if _, err := lsm.SetBatchGroup(targetShard, [][]*kv.Entry{{entry}}); err != nil {
						b.Fatalf("seed set: %v", err)
					}
				}

				if mode == "cold_hint" && lsm.shardHints != nil {
					// Wipe hints so every Get walks all shards.
					lsm.shardHints = newShardHintTable()
				}

				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					entry, err := lsm.Get(lookups[i%seedKeys])
					if err != nil || entry == nil {
						b.Fatalf("get: err=%v entry=%v", err, entry)
					}
					entry.DecrRef()
				}
			})
		}
	}
}

// BenchmarkShardedCrossShardMVCCMerge measures the cost of the cross-shard
// memtable walk + max-version selection without any hint. We seed the same
// userKey on every shard with strictly increasing versions, then read with
// kv.MaxVersion and verify Get picks the latest. Without the cross-shard
// merge the wrong entry would be returned; with it, the cost is N
// memtable lookups + N-way version compare.
func BenchmarkShardedCrossShardMVCCMerge(b *testing.B) {
	for _, shardCount := range []int{1, 2, 4, 8} {
		b.Run(fmt.Sprintf("shards_%d", shardCount), func(b *testing.B) {
			lsm, _ := openShardedBenchLSM(b, shardCount)
			userKey := []byte("mvcc-merge-bench")
			value := []byte("v")

			// Write the same userKey on every shard. Versions are
			// strictly increasing so the highest-version shard
			// holds the visible entry. Wipe the hint table afterwards
			// so the read path takes the full N-way walk.
			for shardID := 0; shardID < shardCount; shardID++ {
				entry := &kv.Entry{
					Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(shardID+1)),
					Value:   value,
					CF:      kv.CFDefault,
					Version: uint64(shardID + 1),
				}
				if _, err := lsm.SetBatchGroup(shardID, [][]*kv.Entry{{entry}}); err != nil {
					b.Fatalf("seed shard %d: %v", shardID, err)
				}
			}
			if lsm.shardHints != nil {
				lsm.shardHints = newShardHintTable()
			}

			query := kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				entry, err := lsm.Get(query)
				if err != nil || entry == nil {
					b.Fatalf("get: err=%v entry=%v", err, entry)
				}
				if entry.Version != uint64(shardCount) {
					b.Fatalf("expected version=%d got %d", shardCount, entry.Version)
				}
				entry.DecrRef()
			}
		})
	}
}

// BenchmarkShardedNegativeCache measures the effect of the negative cache
// on a high-miss read workload. "warm_neg" lets the cache populate before
// timing; "cold_neg" wipes the cache every iteration so the hot path
// always pays the full miss probe.
func BenchmarkShardedNegativeCache(b *testing.B) {
	const seedHits = 256
	for _, mode := range []string{"warm_neg", "cold_neg"} {
		b.Run(mode, func(b *testing.B) {
			lsm, _ := openShardedBenchLSM(b, 4)
			value := make([]byte, 64)

			// Seed a small set of present keys so reads aren't all-miss
			// (otherwise levels.Get fast path may dominate).
			for i := 0; i < seedHits; i++ {
				userKey := make([]byte, 16)
				copy(userKey, "negbench-hit")
				binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
				entry := &kv.Entry{
					Key:     kv.InternalKey(kv.CFDefault, userKey, uint64(i+1)),
					Value:   value,
					CF:      kv.CFDefault,
					Version: uint64(i + 1),
				}
				if _, err := lsm.SetBatchGroup(int(rand.Uint32())&3, [][]*kv.Entry{{entry}}); err != nil {
					b.Fatalf("seed: %v", err)
				}
			}

			missQuery := func(i int) []byte {
				userKey := make([]byte, 16)
				copy(userKey, "negbench-miss")
				binary.LittleEndian.PutUint64(userKey[8:], uint64(i))
				return kv.InternalKey(kv.CFDefault, userKey, kv.MaxVersion)
			}

			if mode == "warm_neg" {
				// First pass populates the negative cache so the timed
				// loop pays only the cache hit.
				for i := 0; i < 1024; i++ {
					_, _ = lsm.Get(missQuery(i % 1024))
				}
			}

			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if mode == "cold_neg" && lsm.negatives != nil {
					lsm.negatives.clear()
				}
				_, _ = lsm.Get(missQuery(i & 1023))
			}
		})
	}
}
