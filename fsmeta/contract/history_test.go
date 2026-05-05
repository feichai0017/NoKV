package contract

import (
	"context"
	"fmt"
	"testing"
	"time"

	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/stretchr/testify/require"
)

func TestFSMetaExecutorConcurrentHistoryContract(t *testing.T) {
	seeds := envInt("NOKV_CONTRACT_HISTORY_SEEDS", 8)
	steps := envInt("NOKV_CONTRACT_HISTORY_STEPS", 48)
	batchSize := envInt("NOKV_CONTRACT_HISTORY_BATCH", 3)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			model := NewModel("vol")
			runner := newVersionedRunner()
			executor, err := fsmetaexec.New(runner, fsmetaexec.WithClock(func() time.Time {
				return time.Unix(0, model.NowUnixNs)
			}))
			require.NoError(t, err)

			ops := GenerateScript(seed, steps)
			err = RunConcurrentBatches(context.Background(), executor, model, ops, batchSize, HistoryOptions{})
			require.NoError(t, err, "seed=%d steps=%d batch=%d", seed, steps, batchSize)
		})
	}
}
