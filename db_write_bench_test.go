package NoKV

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

func BenchmarkDBBatchSet(b *testing.B) {
	db := newBenchDB(b, func(opt *Options) {
		opt.WriteBatchMaxCount = 128
	})
	value := make([]byte, 256)
	batchSize := 64
	entries := make([]*kv.Entry, batchSize)
	b.ReportAllocs()
	b.SetBytes(int64(batchSize * len(value)))
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		for j := range batchSize {
			key := makeBenchKey(i*batchSize + j)
			entry := kv.NewEntryWithCF(kv.CFDefault, key, value)
			entry.Key = kv.InternalKey(entry.CF, entry.Key, nonTxnMaxVersion)
			entries[j] = entry
		}
		if err := db.batchSet(entries); err != nil {
			b.Fatalf("batchSet: %v", err)
		}
	}
}
