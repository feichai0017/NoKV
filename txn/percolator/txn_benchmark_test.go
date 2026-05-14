// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package percolator

import (
	"fmt"
	"path/filepath"
	"testing"

	local "github.com/feichai0017/NoKV/local"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/feichai0017/NoKV/txn/latch"
)

func openBenchmarkDB(b *testing.B) *local.DB {
	b.Helper()
	db, err := local.Open(testOptionsForDir(filepath.Join(b.TempDir(), "db")))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = db.Close() })
	return db
}

func BenchmarkPercolatorBatchApplyFullTransaction(b *testing.B) {
	for _, keyCount := range []int{2, 3, 5} {
		b.Run(fmt.Sprintf("keys_%d", keyCount), func(b *testing.B) {
			db := openBenchmarkDB(b)
			store := newCountingStore(db)
			latches := latch.NewManager(128)
			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				startTs := uint64(i*10 + 1)
				commitTs := startTs + 5
				mutations := make([]*kvrpcpb.Mutation, 0, keyCount)
				keys := make([][]byte, 0, keyCount)
				for keyIdx := range keyCount {
					key := fmt.Appendf(nil, "bench-%d-%d", i, keyIdx)
					keys = append(keys, key)
					mutations = append(mutations, &kvrpcpb.Mutation{
						Op:    kvrpcpb.Mutation_Put,
						Key:   key,
						Value: []byte("value"),
					})
				}
				if errs := Prewrite(store, latches, &kvrpcpb.PrewriteRequest{
					Mutations:    mutations,
					PrimaryLock:  keys[0],
					StartVersion: startTs,
					LockTtl:      3000,
				}); len(errs) > 0 {
					b.Fatalf("prewrite: %v", errs)
				}
				if err := Commit(store, latches, &kvrpcpb.CommitRequest{
					Keys:          keys,
					StartVersion:  startTs,
					CommitVersion: commitTs,
				}); err != nil {
					b.Fatalf("commit: %v", err)
				}
			}

			b.StopTimer()
			b.ReportMetric(float64(store.applyCalls)/float64(b.N), "apply/op")
		})
	}
}
