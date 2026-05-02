package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	fsmetacontract "github.com/feichai0017/NoKV/fsmeta/contract"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	"github.com/stretchr/testify/require"
)

func TestRaftstoreRunnerFSMetaContractOnSplitCluster(t *testing.T) {
	seeds := envInt("NOKV_RAFTSTORE_CONTRACT_SEEDS", 2)
	steps := envInt("NOKV_RAFTSTORE_CONTRACT_STEPS", 40)
	for seed := int64(1); seed <= int64(seeds); seed++ {
		t.Run(fmt.Sprintf("seed_%03d", seed), func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
			defer cancel()

			model := fsmetacontract.NewModel("vol")
			executor := openSplitRealClusterExecutorWithOptions(t, ctx, fsmetaexec.WithClock(func() time.Time {
				return time.Unix(0, model.NowUnixNs)
			}))
			ops := fsmetacontract.GenerateScript(seed, steps)

			err := fsmetacontract.Run(ctx, executor, model, ops)
			require.NoError(t, err, "seed=%d steps=%d", seed, steps)
		})
	}
}

func envInt(name string, fallback int) int {
	raw := os.Getenv(name)
	if raw == "" {
		return fallback
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value <= 0 {
		return fallback
	}
	return value
}
