package migrate

import (
	"context"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	metapb "github.com/feichai0017/NoKV/pb/legacy"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/stretchr/testify/require"
)

func TestRemovePeerWaitsForTargetDrop(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionMeta{Id: 8, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}},
			{Known: true, Region: &metapb.RegionMeta{Id: 8}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
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
		statuses: []*adminpb.RegionRuntimeStatusResponse{{Known: true, Region: &metapb.RegionMeta{Id: 8, Peers: []*metapb.RegionPeer{{StoreId: 2, PeerId: 22}}}}},
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

func TestRemovePeerTreatsLeaderDehostAsSuccess(t *testing.T) {
	leader := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionMeta{Id: 8, Peers: []*metapb.RegionPeer{{StoreId: 1, PeerId: 11}, {StoreId: 2, PeerId: 22}}}},
			{Known: false},
		},
	}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	result, err := RemovePeer(context.Background(), RemovePeerConfig{
		Addr:         "leader",
		RegionID:     8,
		PeerID:       11,
		WaitTimeout:  time.Second,
		PollInterval: time.Millisecond,
		Dial:         dial,
	})
	require.NoError(t, err)
	require.False(t, result.LeaderKnown)
	require.Nil(t, result.LeaderRegion)
}

func TestRemovePeerTimesOutWhenTargetStillHosted(t *testing.T) {
	leader := &fakeAdminClient{statuses: []*adminpb.RegionRuntimeStatusResponse{{Known: true, Region: &metapb.RegionMeta{Id: 8}}}}
	target := &fakeAdminClient{statuses: []*adminpb.RegionRuntimeStatusResponse{{Known: true, Hosted: true, LocalPeerId: 22, AppliedIndex: 1}}}
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

func TestRemovePeerWritesWorkdirCheckpoint(t *testing.T) {
	workDir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: workDir, StoreID: 1, RegionID: 8, PeerID: 11})
	require.NoError(t, err)
	require.NoError(t, mode.Write(workDir, mode.State{Mode: mode.ModeCluster, StoreID: 1, RegionID: 8, PeerID: 11}))

	leader := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
			{Known: true, Region: &metapb.RegionMeta{Id: 8}},
		},
	}
	target := &fakeAdminClient{
		statuses: []*adminpb.RegionRuntimeStatusResponse{
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

	_, err = RemovePeer(context.Background(), RemovePeerConfig{
		WorkDir:         workDir,
		Addr:            "leader",
		TargetAdminAddr: "target",
		RegionID:        8,
		PeerID:          22,
		WaitTimeout:     time.Second,
		PollInterval:    time.Millisecond,
		Dial:            dial,
	})
	require.NoError(t, err)

	status, err := ReadStatus(workDir)
	require.NoError(t, err)
	require.NotNil(t, status.Checkpoint)
	require.Equal(t, CheckpointRemoveFinished, status.Checkpoint.Stage)
	require.Equal(t, uint64(22), status.Checkpoint.TargetPeerID)
	require.Contains(t, status.ResumeHint, "peer removal for peer=22 completed")
}

func TestRemovePeerNoWaitDoesNotLeaveStartedCheckpoint(t *testing.T) {
	workDir := prepareStandaloneWorkdir(t)
	_, err := Init(InitConfig{WorkDir: workDir, StoreID: 1, RegionID: 8, PeerID: 11})
	require.NoError(t, err)
	require.NoError(t, mode.Write(workDir, mode.State{Mode: mode.ModeCluster, StoreID: 1, RegionID: 8, PeerID: 11}))

	leader := &fakeAdminClient{}
	dial := func(ctx context.Context, addr string) (AdminClient, func() error, error) {
		require.Equal(t, "leader", addr)
		return leader, func() error { return nil }, nil
	}

	_, err = RemovePeer(context.Background(), RemovePeerConfig{
		WorkDir:     workDir,
		Addr:        "leader",
		RegionID:    8,
		PeerID:      22,
		WaitTimeout: 0,
		Dial:        dial,
	})
	require.NoError(t, err)

	status, err := ReadStatus(workDir)
	require.NoError(t, err)
	require.NotNil(t, status.Checkpoint)
	require.NotEqual(t, CheckpointRemoveStarted, status.Checkpoint.Stage)
}
