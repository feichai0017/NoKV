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
		statuses: []*pb.RegionRuntimeStatusResponse{
			{Known: true, LeaderPeerId: 11, Region: &pb.RegionMeta{Id: 8}},
			{Known: true, LeaderPeerId: 22, Region: &pb.RegionMeta{Id: 8}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{
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
		Addr:            "leader",
		TargetAdminAddr: "target",
		RegionID:        8,
		PeerID:          22,
		WaitTimeout:     time.Second,
		PollInterval:    time.Millisecond,
		Dial:            dial,
	})
	require.NoError(t, err)
	require.True(t, result.TargetLeader)
	require.Equal(t, uint64(22), result.LeaderPeerID)
}

func TestTransferLeaderTimesOutWhenLeaderDoesNotMove(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, LeaderPeerId: 11, Region: &pb.RegionMeta{Id: 8}}},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	_, err := TransferLeader(context.Background(), TransferLeaderConfig{
		Addr:         "leader",
		RegionID:     8,
		PeerID:       22,
		WaitTimeout:  5 * time.Millisecond,
		PollInterval: time.Millisecond,
		Dial:         dial,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for region 8 to elect peer 22 as leader")
}

func TestTransferLeaderTimesOutWhenTargetNeverBecomesLeader(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, LeaderPeerId: 22, Region: &pb.RegionMeta{Id: 8}}},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, Leader: false}},
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

	_, err := TransferLeader(context.Background(), TransferLeaderConfig{
		Addr:            "leader",
		TargetAdminAddr: "target",
		RegionID:        8,
		PeerID:          22,
		WaitTimeout:     5 * time.Millisecond,
		PollInterval:    time.Millisecond,
		Dial:            dial,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for target peer 22 to become leader")
}
