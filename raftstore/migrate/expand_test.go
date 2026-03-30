package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

type fakeAdminClient struct {
	addResp *pb.AddPeerResponse
	addErr  error

	statuses []*pb.RegionStatusResponse
	calls    int
}

func (f *fakeAdminClient) AddPeer(context.Context, *pb.AddPeerRequest) (*pb.AddPeerResponse, error) {
	if f.addErr != nil {
		return nil, f.addErr
	}
	if f.addResp == nil {
		return &pb.AddPeerResponse{}, nil
	}
	return f.addResp, nil
}

func (f *fakeAdminClient) RegionStatus(context.Context, *pb.RegionStatusRequest) (*pb.RegionStatusResponse, error) {
	if len(f.statuses) == 0 {
		return &pb.RegionStatusResponse{}, nil
	}
	if f.calls >= len(f.statuses) {
		return f.statuses[len(f.statuses)-1], nil
	}
	resp := f.statuses[f.calls]
	f.calls++
	return resp, nil
}

func TestExpandWaitsForTargetHosted(t *testing.T) {
	leader := &fakeAdminClient{
		addResp: &pb.AddPeerResponse{
			Region: &pb.RegionMeta{Id: 8},
		},
		statuses: []*pb.RegionStatusResponse{
			{Known: true, Region: &pb.RegionMeta{Id: 8}},
			{Known: true, Region: &pb.RegionMeta{Id: 8, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionStatusResponse{
			{Known: false},
			{Known: true, Hosted: true, LocalPeerId: 22},
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

	result, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		TargetAddr:   "target",
		RegionID:     8,
		StoreID:      2,
		PeerID:       22,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
	})
	require.NoError(t, err)
	require.True(t, result.LeaderKnown)
	require.True(t, result.TargetKnown)
	require.True(t, result.TargetHosted)
	require.Equal(t, uint64(22), result.TargetLocalPeerID)
}

func TestExpandWithoutWaitReturnsAfterAddPeer(t *testing.T) {
	leader := &fakeAdminClient{
		addResp: &pb.AddPeerResponse{
			Region: &pb.RegionMeta{Id: 9, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 33}}},
		},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	result, err := Expand(context.Background(), ExpandConfig{
		Addr:        "leader",
		RegionID:    9,
		StoreID:     2,
		PeerID:      33,
		WaitTimeout: 0,
		Dial:        dial,
	})
	require.NoError(t, err)
	require.True(t, result.LeaderKnown)
	require.False(t, result.Waited)
	require.Zero(t, leader.calls)
}
