package exec

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

type fakeQuotaLookup struct {
	fences map[quotaSubject]*coordpb.QuotaFenceInfo
	calls  int
}

func (f *fakeQuotaLookup) GetQuotaFence(_ context.Context, req *coordpb.GetQuotaFenceRequest) (*coordpb.GetQuotaFenceResponse, error) {
	f.calls++
	subject := req.GetSubject()
	key := quotaSubject{mount: fsmeta.MountID(subject.GetMountId()), scope: fsmeta.InodeID(subject.GetSubtreeRoot())}
	if fence := f.fences[key]; fence != nil {
		return &coordpb.GetQuotaFenceResponse{Fence: fence}, nil
	}
	return &coordpb.GetQuotaFenceResponse{NotFound: true}, nil
}

func TestQuotaReserveWritesUsageCountersInTransaction(t *testing.T) {
	runner := newFakeRunner()
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}:          {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 8192, LimitInodes: 10, Era: 1},
		{mount: "vol", scope: 7}: {Subject: &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 7}, LimitBytes: 4096, LimitInodes: 2, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	mutations, err := cache.ReserveQuota(context.Background(), runner, []QuotaChange{{Mount: "vol", Scope: 7, Bytes: 1024, Inodes: 1}}, 1)
	require.NoError(t, err)
	require.Len(t, mutations, 2)

	for _, mut := range mutations {
		require.Equal(t, kvrpcpb.Mutation_Put, mut.GetOp())
		usage, err := fsmeta.DecodeUsageValue(mut.GetValue())
		require.NoError(t, err)
		require.Equal(t, fsmeta.UsageRecord{Bytes: 1024, Inodes: 1}, usage)
	}
}

func TestQuotaReserveRejectsClusterWideLimit(t *testing.T) {
	runner := newFakeRunner()
	key, err := fsmeta.EncodeUsageKey("vol", 0)
	require.NoError(t, err)
	value, err := fsmeta.EncodeUsageValue(fsmeta.UsageRecord{Bytes: 900, Inodes: 1})
	require.NoError(t, err)
	runner.data[string(key)] = value
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}: {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	_, err = cache.ReserveQuota(context.Background(), runner, []QuotaChange{{Mount: "vol", Scope: 7, Bytes: 200, Inodes: 1}}, 1)
	require.ErrorIs(t, err, fsmeta.ErrQuotaExceeded)
}

func TestQuotaReserveCoalescesRenameTransfer(t *testing.T) {
	runner := newFakeRunner()
	lookup := &fakeQuotaLookup{fences: map[quotaSubject]*coordpb.QuotaFenceInfo{
		{mount: "vol"}:          {Subject: &coordpb.QuotaSubject{MountId: "vol"}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
		{mount: "vol", scope: 7}: {Subject: &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 7}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
		{mount: "vol", scope: 8}: {Subject: &coordpb.QuotaSubject{MountId: "vol", SubtreeRoot: 8}, LimitBytes: 1000, LimitInodes: 10, Era: 1},
	}}
	cache := &quotaCache{coord: lookup, ttl: time.Minute}

	mutations, err := cache.ReserveQuota(context.Background(), runner, []QuotaChange{
		{Mount: "vol", Scope: 7, Bytes: -100, Inodes: -1},
		{Mount: "vol", Scope: 8, Bytes: 100, Inodes: 1},
	}, 1)
	require.NoError(t, err)
	require.Len(t, mutations, 2, "mount-wide zero delta should be elided")
}
