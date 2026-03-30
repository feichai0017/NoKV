package migrate

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/stretchr/testify/require"
)

type fakeAdminClient struct {
	addResp             *pb.AddPeerResponse
	addErr              error
	removeErr           error
	transferErr         error
	exportSnapshotResp  *pb.ExportRegionSnapshotResponse
	exportSnapshotErr   error
	installSnapshotErr  error
	installSnapshotReqs []*pb.InstallRegionSnapshotRequest

	statuses []*pb.RegionRuntimeStatusResponse
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

func (f *fakeAdminClient) ExportRegionSnapshot(context.Context, *pb.ExportRegionSnapshotRequest) (*pb.ExportRegionSnapshotResponse, error) {
	if f.exportSnapshotErr != nil {
		return nil, f.exportSnapshotErr
	}
	if f.exportSnapshotResp == nil {
		return &pb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot")}, nil
	}
	return f.exportSnapshotResp, nil
}

func (f *fakeAdminClient) InstallRegionSnapshot(_ context.Context, req *pb.InstallRegionSnapshotRequest) (*pb.InstallRegionSnapshotResponse, error) {
	f.installSnapshotReqs = append(f.installSnapshotReqs, req)
	if f.installSnapshotErr != nil {
		return nil, f.installSnapshotErr
	}
	return &pb.InstallRegionSnapshotResponse{}, nil
}

func (f *fakeAdminClient) RegionRuntimeStatus(context.Context, *pb.RegionRuntimeStatusRequest) (*pb.RegionRuntimeStatusResponse, error) {
	if len(f.statuses) == 0 {
		return &pb.RegionRuntimeStatusResponse{}, nil
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
		exportSnapshotResp: &pb.ExportRegionSnapshotResponse{
			Snapshot: []byte("snapshot-8"),
			Region:   &pb.RegionMeta{Id: 8, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}},
		},
		statuses: []*pb.RegionRuntimeStatusResponse{
			{Known: true, Region: &pb.RegionMeta{Id: 8}},
			{Known: true, Region: &pb.RegionMeta{Id: 8, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*pb.RegionRuntimeStatusResponse{
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

	result, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     8,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 22, TargetAdminAddr: "target"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.True(t, result.Results[0].LeaderKnown)
	require.True(t, result.Results[0].TargetKnown)
	require.True(t, result.Results[0].TargetHosted)
	require.Equal(t, uint64(22), result.Results[0].TargetLocalPeerID)
	require.Equal(t, uint64(1), result.Results[0].TargetAppliedIdx)
	require.Len(t, target.installSnapshotReqs, 1)
	require.Equal(t, []byte("snapshot-8"), target.installSnapshotReqs[0].GetSnapshot())
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

func TestExpandRollsTargetsSequentially(t *testing.T) {
	leader := &fakeAdminClient{
		addResp:            &pb.AddPeerResponse{Region: &pb.RegionMeta{Id: 11}},
		exportSnapshotResp: &pb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot-11")},
		statuses: []*pb.RegionRuntimeStatusResponse{
			{Known: true, Region: &pb.RegionMeta{Id: 11, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
			{Known: true, Region: &pb.RegionMeta{Id: 11, Peers: []*pb.RegionPeer{{StoreId: 2, PeerId: 22}, {StoreId: 3, PeerId: 33}}}},
		},
	}
	target2 := &fakeAdminClient{statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, AppliedTerm: 1}}}
	target3 := &fakeAdminClient{statuses: []*pb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 33, AppliedIndex: 1, AppliedTerm: 1}}}
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

	result, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     11,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 22, TargetAdminAddr: "target2"},
			{StoreID: 3, PeerID: 33, TargetAdminAddr: "target3"},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 2)
	require.Equal(t, uint64(22), result.Results[0].PeerID)
	require.Equal(t, uint64(33), result.Results[1].PeerID)
	require.True(t, result.Results[1].TargetHosted)
}
