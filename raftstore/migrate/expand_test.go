package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

type fakeAdminClient struct {
	addResp     *pb.AddPeerResponse
	addErr      error
	removeErr   error
	transferErr error

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

func (f *fakeAdminClient) RemovePeer(context.Context, *pb.RemovePeerRequest) (*pb.RemovePeerResponse, error) {
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	return &pb.RemovePeerResponse{}, nil
}

func (f *fakeAdminClient) TransferLeader(context.Context, *pb.TransferLeaderRequest) (*pb.TransferLeaderResponse, error) {
	if f.transferErr != nil {
		return nil, f.transferErr
	}
	return &pb.TransferLeaderResponse{}, nil
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

func TestExpandManyWaitsForTargetHosted(t *testing.T) {
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
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, AppliedTerm: 1},
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

	result, err := ExpandMany(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     8,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 22, TargetAddr: "target"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.True(t, result.Results[0].LeaderKnown)
	require.True(t, result.Results[0].TargetKnown)
	require.True(t, result.Results[0].TargetHosted)
	require.Equal(t, uint64(22), result.Results[0].TargetLocalPeerID)
	require.Equal(t, uint64(1), result.Results[0].TargetAppliedIdx)
}

func TestExpandManyWithoutWaitReturnsAfterAddPeer(t *testing.T) {
	leader := &fakeAdminClient{
		addResp: &pb.AddPeerResponse{
			Region: &pb.RegionMeta{Id: 9, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 33}}},
		},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	result, err := ExpandMany(context.Background(), ExpandConfig{
		Addr:        "leader",
		RegionID:    9,
		WaitTimeout: 0,
		Dial:        dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 33},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.True(t, result.Results[0].LeaderKnown)
	require.False(t, result.Results[0].Waited)
	require.Zero(t, leader.calls)
}

func TestExpandManyRollsTargetsSequentially(t *testing.T) {
	leader := &fakeAdminClient{
		addResp: &pb.AddPeerResponse{Region: &pb.RegionMeta{Id: 11}},
		statuses: []*pb.RegionStatusResponse{
			{Known: true, Region: &pb.RegionMeta{Id: 11, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
			{Known: true, Region: &pb.RegionMeta{Id: 11, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}, {StoreId: 3, PeerId: 33}}}},
		},
	}
	target2 := &fakeAdminClient{statuses: []*pb.RegionStatusResponse{{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, AppliedTerm: 1}}}
	target3 := &fakeAdminClient{statuses: []*pb.RegionStatusResponse{{Known: true, Hosted: true, LocalPeerId: 33, AppliedIndex: 1, AppliedTerm: 1}}}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		switch addr {
		case "leader":
			return leader, func() error { return nil }, nil
		case "target2":
			return target2, func() error { return nil }, nil
		case "target3":
			return target3, func() error { return nil }, nil
		default:
			t.Fatalf("unexpected addr %q", addr)
			return nil, nil, nil
		}
	}

	result, err := ExpandMany(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     11,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 22, TargetAddr: "target2"},
			{StoreID: 3, PeerID: 33, TargetAddr: "target3"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 2)
	require.Equal(t, uint64(22), result.Results[0].PeerID)
	require.Equal(t, uint64(33), result.Results[1].PeerID)
	require.True(t, result.Results[1].TargetHosted)
}
