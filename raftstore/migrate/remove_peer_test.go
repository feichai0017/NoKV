package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

func TestRemovePeerWaitsForTargetDrop(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{
			{Known: true, Region: &pb.RegionMeta{Id: 8, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
			{Known: true, Region: &pb.RegionMeta{Id: 8}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1},
			{},
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

	result, err := RemovePeer(context.Background(), RemovePeerConfig{
		Addr:            "leader",
		TargetAdminAddr: "target",
		RegionID:        8,
		PeerID:          22,
		WaitTimeout:     time.Second,
		PollInterval:    time.Millisecond,
		Dial:            dial,
	})
	require.NoError(t, err)
	require.False(t, result.TargetHosted)
}

func TestRemovePeerTimesOutWhenLeaderStillPublishesPeer(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, Region: &pb.RegionMeta{Id: 8, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}}},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	_, err := RemovePeer(context.Background(), RemovePeerConfig{
		Addr:         "leader",
		RegionID:     8,
		PeerID:       22,
		WaitTimeout:  5 * time.Millisecond,
		PollInterval: time.Millisecond,
		Dial:         dial,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for leader region 8 to remove peer 22")
}

func TestRemovePeerTimesOutWhenTargetStillHosted(t *testing.T) {
	leader := &fakeAdminClient{statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, Region: &pb.RegionMeta{Id: 8}}}}
	target := &fakeAdminClient{statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1}}}
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

	_, err := RemovePeer(context.Background(), RemovePeerConfig{
		Addr:            "leader",
		TargetAdminAddr: "target",
		RegionID:        8,
		PeerID:          22,
		WaitTimeout:     5 * time.Millisecond,
		PollInterval:    time.Millisecond,
		Dial:            dial,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for target store to drop peer 22")
}
