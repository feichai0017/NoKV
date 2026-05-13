package exec

import (
	"context"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestExecutorGetQuotaUsage(t *testing.T) {
	runner := newFakeRunner()
	key, err := fsmeta.EncodeUsageKey(testMountIdentity, 7)
	require.NoError(t, err)
	value, err := fsmeta.EncodeUsageValue(fsmeta.UsageRecord{Bytes: 4096, Inodes: 2})
	require.NoError(t, err)
	runner.data[string(key)] = value
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol", Scope: 7})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, usage)
}

func TestExecutorGetQuotaUsageReturnsZeroForMissingCounter(t *testing.T) {
	runner := newFakeRunner()
	executor, err := newTestExecutor(runner)
	require.NoError(t, err)

	usage, err := executor.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{}, usage)
}
