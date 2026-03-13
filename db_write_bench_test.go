package NoKV

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

// BenchmarkDBBatchSet compares write throughput under three sync modes:
//
//	Async          – SyncWrites=false (no fsync, baseline)
//	SyncInline     – SyncWrites=true, SyncPipeline=false (commit worker fsync inline)
//	SyncPipeline   – SyncWrites=true, SyncPipeline=true  (dedicated sync worker)
//
// Each mode is tested with batch sizes 1, 8, 64.
func BenchmarkDBBatchSet(b *testing.B) {
	type syncMode struct {
		name     string
		sync     bool
		pipeline bool
	}
	modes := []syncMode{
		{"NoSync", false, false},
		{"SyncInline", true, false},
		{"SyncPipeline", true, true},
	}

	value := make([]byte, 256)
	batchSize := 64

	for _, mode := range modes {
		mode := mode
		b.Run(mode.name, func(b *testing.B) {
			db := newBenchDB(b, func(opt *Options) {
				opt.WriteBatchMaxCount = 128
				opt.SyncWrites = mode.sync
				opt.SyncPipeline = mode.pipeline
			})
			entries := make([]*kv.Entry, batchSize)
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * len(value)))
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				for j := range batchSize {
					key := makeBenchKey(i*batchSize + j)
					entries[j] = kv.NewInternalEntry(kv.CFDefault, key, nonTxnMaxVersion, value, 0, 0)
				}
				if _, err := db.sendToWriteCh(entries, true); err != nil {
					b.Fatalf("batchSet: %v", err)
				}
			}
		})

	}
}
