package lsm

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"testing"

	"github.com/feichai0017/NoKV/engine/kv"
)

// BenchmarkShardedGetWithHint compares cross-shard memtable Get with and
// without the hint cache. The "warm_hint" sub-benchmark seeds the hint
// table with the right shardID before timing; "cold_hint" forces a full
// N-way memtable walk. The delta is the headline number for shard_hint.go.
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
