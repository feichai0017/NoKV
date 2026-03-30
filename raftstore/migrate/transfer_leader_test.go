package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

func TestTransferLeaderWaitsForTargetLeadership(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*pb.RegionStatusResponse{
			{Known: true, LeaderPeerId: 11, Region: &pb.RegionMeta{Id: 8}},
			{Known: true, LeaderPeerId: 22, Region: &pb.RegionMeta{Id: 8}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionStatusResponse{
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1},
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, Leader: true},
		},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		switch addr {
		case "leader":
			return leader, func() error { return nil }, nil
		case "target":
			return target, func() error { return nil }, nil
		default:
			t.Fatalf("unexpected addr %q", addr)
			return nil, nil, nil
		}
	}

	result, err := TransferLeader(context.Background(), TransferLeaderConfig{
		Addr:         "leader",
		TargetAddr:   "target",
		RegionID:     8,
		PeerID:       22,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
	})
	require.NoError(t, err)
	require.True(t, result.TargetLeader)
	require.Equal(t, uint64(22), result.LeaderPeerID)
}
