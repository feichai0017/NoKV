// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package integration

import (
	"context"
	"fmt"
	"testing"
	"time"

	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/stretchr/testify/require"
)

func TestRaftstoreRuntimeFSMetaConcurrentHistoryOnSplitCluster(t *testing.T) {
	seeds := envInt("NOKV_RAFTSTORE_HISTORY_SEEDS", 1)
	steps := envInt("NOKV_RAFTSTORE_HISTORY_STEPS", 24)
	batchSize := envInt("NOKV_RAFTSTORE_HISTORY_BATCH", 3)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			model := fsmetacontract.NewModel("vol")
			executor := openSplitRealClusterExecutorWithOptions(t, ctx, fsmetaexec.WithClock(func() time.Time {
				return time.Unix(0, model.NowUnixNs)
			}))
			ops := fsmetacontract.GenerateScript(seed, steps)

			// The real raftstore path has a finite transaction-contention retry
			// budget. If that budget is exhausted, the API returns a retryable
			// transaction error rather than a namespace-semantic result; keep
			// both legal model outcomes so this test checks serialization without
			// depending on a particular retry budget.
			err := fsmetacontract.RunConcurrentBatches(ctx, executor, model, ops, batchSize, fsmetacontract.HistoryOptions{
				AllowIndeterminateErrors: true,
			})
			require.NoError(t, err, "seed=%d steps=%d batch=%d", seed, steps, batchSize)
		})
	}
}
