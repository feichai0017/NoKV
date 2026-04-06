package migrate

import (
	"bytes"
	"context"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"io"
	"testing"
	"time"

	adminclient "github.com/feichai0017/NoKV/raftstore/admin"
	"github.com/stretchr/testify/require"
)

type fakeAdminClient struct {
	addResp             *adminpb.AddPeerResponse
	addErr              error
	removeErr           error
	transferErr         error
	exportSnapshotResp  *adminpb.ExportRegionSnapshotResponse
	exportSnapshotErr   error
	exportSnapshotReqs  []*adminpb.ExportRegionSnapshotRequest
	importSnapshotErr   error
	importSnapshotHdrs  [][]byte
	importSnapshotRegs  []*metapb.RegionDescriptor
	importSnapshotBytes [][]byte

	statuses []*adminpb.RegionRuntimeStatusResponse
	calls    int
}

func (f *fakeAdminClient) AddPeer(context.Context, *adminpb.AddPeerRequest) (*adminpb.AddPeerResponse, error) {
	if f.addErr != nil {
		return nil, f.addErr
	}
	if f.addResp == nil {
		return &adminpb.AddPeerResponse{}, nil
	}
	return f.addResp, nil
}

func (f *fakeAdminClient) RemovePeer(context.Context, *adminpb.RemovePeerRequest) (*adminpb.RemovePeerResponse, error) {
	if f.removeErr != nil {
		return nil, f.removeErr
	}
	return &adminpb.RemovePeerResponse{}, nil
}

func (f *fakeAdminClient) TransferLeader(context.Context, *adminpb.TransferLeaderRequest) (*adminpb.TransferLeaderResponse, error) {
	if f.transferErr != nil {
		return nil, f.transferErr
	}
	return &adminpb.TransferLeaderResponse{}, nil
}

func (f *fakeAdminClient) ExportRegionSnapshotStream(_ context.Context, req *adminpb.ExportRegionSnapshotStreamRequest) (*adminclient.SnapshotExportStream, error) {
	f.exportSnapshotReqs = append(f.exportSnapshotReqs, &adminpb.ExportRegionSnapshotRequest{RegionId: req.GetRegionId()})
	if f.exportSnapshotErr != nil {
		return nil, f.exportSnapshotErr
	}
	resp := f.exportSnapshotResp
	if resp == nil {
		resp = &adminpb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot")}
	}
	return &adminclient.SnapshotExportStream{
		Header: []byte("header"),
		Region: resp.GetRegion(),
		Reader: io.NopCloser(bytes.NewReader(resp.GetSnapshot())),
	}, nil
}

func (f *fakeAdminClient) ImportRegionSnapshotStream(_ context.Context, header []byte, region *metapb.RegionDescriptor, r io.Reader) (*adminpb.ImportRegionSnapshotResponse, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}
	f.importSnapshotHdrs = append(f.importSnapshotHdrs, append([]byte(nil), header...))
	f.importSnapshotRegs = append(f.importSnapshotRegs, region)
	f.importSnapshotBytes = append(f.importSnapshotBytes, data)
	if f.importSnapshotErr != nil {
		return nil, f.importSnapshotErr
	}
	return &adminpb.ImportRegionSnapshotResponse{}, nil
}

func (f *fakeAdminClient) RegionRuntimeStatus(context.Context, *adminpb.RegionRuntimeStatusRequest) (*adminpb.RegionRuntimeStatusResponse, error) {
	if len(f.statuses) == 0 {
		return &adminpb.RegionRuntimeStatusResponse{}, nil
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
		addResp: &adminpb.AddPeerResponse{
			Region: &metapb.RegionDescriptor{RegionId: 8},
		},
		exportSnapshotResp: &adminpb.ExportRegionSnapshotResponse{
			Snapshot: []byte("snapshot-8"),
			Region:   &metapb.RegionDescriptor{RegionId: 8, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}},
		},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 8}},
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 8, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: false},
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, AppliedTerm: 1},
		},
	}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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
	require.Len(t, leader.exportSnapshotReqs, 1)
	require.Equal(t, uint64(8), leader.exportSnapshotReqs[0].GetRegionId())
	require.Len(t, target.importSnapshotBytes, 1)
	require.Equal(t, []byte("snapshot-8"), target.importSnapshotBytes[0])
}

func TestExpandWithoutWaitReturnsAfterAddPeer(t *testing.T) {
	leader := &fakeAdminClient{
		addResp: &adminpb.AddPeerResponse{
			Region: &metapb.RegionDescriptor{RegionId: 9, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 33}}},
		},
	}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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
		addResp:            &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 11}},
		exportSnapshotResp: &adminpb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot-11")},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 11, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 11, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}, {StoreId: 3, PeerId: 33}}}},
		},
	}
	target2 := &fakeAdminClient{statuses: []*adminpb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, AppliedTerm: 1}}}
	target3 := &fakeAdminClient{statuses: []*adminpb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 33, AppliedIndex: 1, AppliedTerm: 1}}}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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

func TestExpandWritesWorkdirCheckpoint(t *testing.T) {
	workDir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: workDir, StoreID: 1, RegionID: 15, PeerID: 115})
	require.NoError(t, err)

	leader := &fakeAdminClient{
		addResp:            &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 15}},
		exportSnapshotResp: &adminpb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot-15")},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 15, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1, AppliedTerm: 1},
		},
	}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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

	_, err = Expand(context.Background(), ExpandConfig{
		WorkDir:      workDir,
		Addr:         "leader",
		RegionID:     15,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 22, TargetAdminAddr: "target"},
		},
	})
	require.NoError(t, err)

	status, err := ReadStatus(workDir)
	require.NoError(t, err)
	require.NotNil(t, status.Checkpoint)
	require.Equal(t, CheckpointExpandHosted, status.Checkpoint.Stage)
	require.Equal(t, uint64(2), status.Checkpoint.TargetStoreID)
	require.Equal(t, uint64(22), status.Checkpoint.TargetPeerID)
	require.Equal(t, 1, status.Checkpoint.CompletedTargets)
	require.Equal(t, 1, status.Checkpoint.TotalTargets)
	require.Contains(t, status.ResumeHint, "completed 1/1 target")
}

func TestExpandRequestsRegionSnapshot(t *testing.T) {
	leader := &fakeAdminClient{
		addResp:            &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 18}},
		exportSnapshotResp: &adminpb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot-18")},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 18, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 28}}}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Hosted: true, LocalPeerId: 28, AppliedIndex: 1, AppliedTerm: 1},
		},
	}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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

	_, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     18,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets: []PeerTarget{
			{StoreID: 2, PeerID: 28, TargetAdminAddr: "target"},
		},
	})
	require.NoError(t, err)
	require.Len(t, leader.exportSnapshotReqs, 1)
	require.Equal(t, uint64(18), leader.exportSnapshotReqs[0].GetRegionId())
	require.Len(t, target.importSnapshotBytes, 1)
	require.Equal(t, []byte("snapshot-18"), target.importSnapshotBytes[0])
}

func TestExpandFailsWhenLeaderSnapshotExportFails(t *testing.T) {
	leader := &fakeAdminClient{
		addResp:           &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 12}},
		exportSnapshotErr: context.DeadlineExceeded,
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 12, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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

	_, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     12,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets:      []PeerTarget{{StoreID: 2, PeerID: 22, TargetAdminAddr: "target"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "export region 12 snapshot")
	require.Empty(t, target.importSnapshotBytes)
}

func TestExpandFailsWhenTargetSnapshotInstallFails(t *testing.T) {
	leader := &fakeAdminClient{
		addResp:            &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 13}},
		exportSnapshotResp: &adminpb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot-13")},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 13, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{importSnapshotErr: context.DeadlineExceeded}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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

	_, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     13,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets:      []PeerTarget{{StoreID: 2, PeerID: 22, TargetAdminAddr: "target"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "import region 13 snapshot")
	require.Len(t, target.importSnapshotBytes, 1)
}

func TestExpandTimesOutWhenLeaderNeverPublishesPeer(t *testing.T) {
	leader := &fakeAdminClient{
		addResp: &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 14}},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 14}},
		},
	}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	_, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     14,
		WaitTimeout:  5 * time.Millisecond,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets:      []PeerTarget{{StoreID: 2, PeerID: 22}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for leader region 14 to publish peer 22")
}

func TestExpandTimesOutWhenTargetNeverHostsPeer(t *testing.T) {
	leader := &fakeAdminClient{
		addResp:            &adminpb.AddPeerResponse{Region: &metapb.RegionDescriptor{RegionId: 15}},
		exportSnapshotResp: &adminpb.ExportRegionSnapshotResponse{Snapshot: []byte("snapshot-15")},
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionDescriptor{RegionId: 15, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
		},
	}
	target := &fakeAdminClient{statuses: []*adminpb.RegionRuntimeStatusResponse{{Known: true, Hosted: false}}}
	dial := func(ctx context.Context, addr string) (adminclient.Client, func() error, error) {
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

	_, err := Expand(context.Background(), ExpandConfig{
		Addr:         "leader",
		RegionID:     15,
		WaitTimeout:  5 * time.Millisecond,
		PollInterval: time.Millisecond,
		Dial:         dial,
		Targets:      []PeerTarget{{StoreID: 2, PeerID: 22, TargetAdminAddr: "target"}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "waiting for target store to host peer 22")
}
