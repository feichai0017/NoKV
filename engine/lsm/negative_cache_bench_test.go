package lsm

import (
	"encoding/binary"
	"math/rand"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
)

// BenchmarkShardedNegativeCache measures the effect of the negative cache
// on a high-miss read workload. "warm_neg" lets the cache populate before
// timing; "cold_neg" wipes the cache every iteration so the hot path
// always pays the full miss probe through every shard's memtable + the
// L0..LN levels.
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
