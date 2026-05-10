package percolator

import (
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/txn/latch"
)

// BenchmarkApplyAtomicMutateBatchDisjoint measures the per-batch
// wall-time of ApplyAtomicMutateBatch on a batch of N independent
// requests. Below parallelValidationThreshold the planner runs
// serially; at and above it validations fan out to goroutines. Each
// iteration uses fresh keys so back-to-back batches do not see each
// other's writes.
//
// To keep the benchmark dominated by the apply path itself rather
// than fixture cost, the DB and latch manager are reused across
// iterations. b.SetBytes is set to the per-batch entry count so
// `-benchtime=Ns` runs are still well-defined.
func BenchmarkApplyAtomicMutateBatchDisjoint(b *testing.B) {
	for _, batch := range []int{1, 2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("n=%d", batch), func(b *testing.B) {
			opt := testOptionsForDir(b.TempDir())
			opt.LSMShardCount = 1
			// Big memtable so the bench measures the apply pipeline, not
			// memtable rotations / compaction triggered by tiny defaults.
			opt.MemTableSize = 64 << 20
			opt.SSTableMaxSz = 256 << 20
			db, err := local.Open(opt)
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			b.Cleanup(func() { _ = db.Close() })
			store := newCountingAtomicStore(db)
			latches := latch.NewManager(64)
			var keyCounter atomic.Uint64
			var ts atomic.Uint64
			ts.Store(2)

			b.ReportAllocs()
			b.SetBytes(int64(batch))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				reqs := make([]*kvrpcpb.TryAtomicMutateRequest, 0, batch)
				for k := 0; k < batch; k++ {
					id := keyCounter.Add(1)
					start := ts.Add(2) - 2
					reqs = append(reqs, atomicPutRequest(
						start, start+1,
						[]byte(fmt.Sprintf("bench-disjoint-%010d", id)),
						[]byte("v"),
					))
				}

				results := ApplyAtomicMutateBatch(store, latches, reqs)
				if len(results) != batch {
					b.Fatalf("expected %d results, got %d", batch, len(results))
				}
				for j, r := range results {
					if r.Error != nil {
						b.Fatalf("result %d errored: %v", j, r.Error)
					}
				}
			}
		})
	}
}

// BenchmarkApplyAtomicMutateBatchAllConflict measures the worst case:
// every request in the batch conflicts with the previous one, forcing
// the DAG into a single chain (one item per level). Validation cannot
// run in parallel; the only difference from the legacy path is the
// constant overhead of building and walking the DAG.
//
// Useful as a guard: if this regresses materially relative to the
// pre-Falcon serial implementation, we are paying too much for
// scheduling we never use.
func BenchmarkApplyAtomicMutateBatchAllConflict(b *testing.B) {
	for _, batch := range []int{2, 4, 8, 16, 32} {
		b.Run(fmt.Sprintf("n=%d", batch), func(b *testing.B) {
			opt := testOptionsForDir(b.TempDir())
			opt.LSMShardCount = 1
			// Big memtable so the bench measures the apply pipeline, not
			// memtable rotations / compaction triggered by tiny defaults.
			opt.MemTableSize = 64 << 20
			opt.SSTableMaxSz = 256 << 20
			db, err := local.Open(opt)
			if err != nil {
				b.Fatalf("open db: %v", err)
			}
			b.Cleanup(func() { _ = db.Close() })
			store := newCountingAtomicStore(db)
			latches := latch.NewManager(64)
			var ts atomic.Uint64
			ts.Store(2)
			var keyCounter atomic.Uint64

			b.ReportAllocs()
			b.SetBytes(int64(batch))
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				// All operations target the same single key, so the DAG
				// degenerates into a chain. Only the first succeeds; the
				// remainder fail with AlreadyExists. Each iteration uses a
				// fresh key so the chain reproduces.
				key := []byte(fmt.Sprintf("bench-conflict-%010d", keyCounter.Add(1)))
				reqs := make([]*kvrpcpb.TryAtomicMutateRequest, 0, batch)
				for k := 0; k < batch; k++ {
					start := ts.Add(2) - 2
					reqs = append(reqs, atomicPutRequest(start, start+1, key, []byte("v")))
				}

				results := ApplyAtomicMutateBatch(store, latches, reqs)
				if len(results) != batch {
					b.Fatalf("expected %d results, got %d", batch, len(results))
				}
				if results[0].Error != nil {
					b.Fatalf("first result errored: %v", results[0].Error)
				}
				// The rest are expected to fail with AlreadyExists; that's
				// fine — the bench measures the apply pipeline, not the
				// outcome semantics.
			}
		})
	}
}
