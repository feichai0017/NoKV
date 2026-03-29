package NoKV

import (
	"testing"

	"github.com/feichai0017/NoKV/kv"
)

// BenchmarkDBBatchSet compares end-to-end batch write throughput under three sync modes:
//
//	Async          – SyncWrites=false (no fsync, baseline)
//	SyncInline     – SyncWrites=true, SyncPipeline=false (commit worker fsync inline)
//	SyncPipeline   – SyncWrites=true, SyncPipeline=true  (dedicated sync worker)
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
		b.Run(mode.name, func(b *testing.B) {
			db := newBenchDB(b, func(opt *Options) {
				opt.WriteBatchMaxCount = 128
				opt.MaxBatchCount = 128
				opt.SyncWrites = mode.sync
				opt.SyncPipeline = mode.pipeline
			})
			b.ReportAllocs()
			b.SetBytes(int64(batchSize * len(value)))
			b.ResetTimer()
			for i := 0; b.Loop(); i++ {
				entries := make([]*kv.Entry, batchSize)
				for j := range batchSize {
					key := makeBenchKey(i*batchSize + j)
					entries[j] = kv.NewInternalEntry(kv.CFDefault, key, nonTxnMaxVersion, value, 0, 0)
				}
				req, err := db.sendToWriteCh(entries, true)
				if err != nil {
					b.Fatalf("batchSet: %v", err)
				}
				if err := req.Wait(); err != nil {
					b.Fatalf("wait batchSet: %v", err)
				}
			}
		})

	}
}
