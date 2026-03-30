package integration

import (
	"context"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/pb"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func regionHasPeer(meta *pb.RegionMeta, peerID uint64) bool {
	if meta == nil {
		return false
	}
	for _, peer := range meta.GetPeers() {
		if peer.GetPeerId() == peerID {
			return true
		}
	}
	return false
}

func TestPartitionedFollowerCatchesUpAfterRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 81, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target2 := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	target3 := testcluster.StartNode(t, 3, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target2.Close(t)
	defer target3.Close(t)

	seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
	target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
	target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 81, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     81,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.Addr()},
			{StoreID: 3, PeerID: 301, TargetAdminAddr: target3.Addr()},
		},
	})
	require.NoError(t, err)

	leaderStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 81)
	cli, err := client.New(client.Config{
		Stores: []client.StoreEndpoint{
			{StoreID: 1, Addr: seed.Addr()},
			{StoreID: 2, Addr: target2.Addr()},
			{StoreID: 3, Addr: target3.Addr()},
		},
		RegionResolver: &staticResolver{regions: []*pb.RegionMeta{leaderStatus.GetRegion()}},
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	target3.BlockPeer(101)
	target3.BlockPeer(201)
	seed.BlockPeer(301)
	target2.BlockPeer(301)

	key := []byte("partitioned-follower-key")
	value := []byte("partitioned-follower-value")
	require.NoError(t, cli.Put(ctx, key, value, 10, 11, 3000))

	_, err = target3.DB.Get(key)
	require.Error(t, err)

	target3.Restart(t, nil, true)
	seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
	target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
	target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})
	target3.BlockPeer(101)
	target3.BlockPeer(201)
	seed.BlockPeer(301)
	target2.BlockPeer(301)

	target3.UnblockPeer(101)
	target3.UnblockPeer(201)
	seed.UnblockPeer(301)
	target2.UnblockPeer(301)

	require.Eventually(t, func() bool {
		entry, err := target3.DB.Get(key)
		return err == nil && string(entry.Value) == string(value)
	}, 5*time.Second, 50*time.Millisecond)
}

func TestTransferLeaderRecoversAfterPartitionedTargetReturns(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 82, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target2 := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	target3 := testcluster.StartNode(t, 3, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target2.Close(t)
	defer target3.Close(t)

	seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
	target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
	target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 82, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     82,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.Addr()},
			{StoreID: 3, PeerID: 301, TargetAdminAddr: target3.Addr()},
		},
	})
	require.NoError(t, err)

	seed.BlockPeer(201)
	target2.BlockPeer(101)

	_, err = migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            seed.Addr(),
		TargetAdminAddr: target2.Addr(),
		RegionID:        82,
		PeerID:          201,
		WaitTimeout:     500 * time.Millisecond,
		PollInterval:    20 * time.Millisecond,
	})
	require.Error(t, err)

	seed.UnblockPeer(201)
	target2.UnblockPeer(101)

	currentLeader, currentStatus := testcluster.FindLeader(t, ctx, 82, seed, target2, target3)
	if currentStatus.GetLeaderPeerId() == 201 {
		require.Equal(t, target2.Addr(), currentLeader.Addr())
		require.True(t, currentStatus.GetLeader())
		return
	}

	result, err := migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            currentLeader.Addr(),
		TargetAdminAddr: target2.Addr(),
		RegionID:        82,
		PeerID:          201,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, result.TargetLeader)
	require.Equal(t, uint64(201), result.LeaderPeerID)
}

func TestRepeatedLinkFlapConvergesDuringMembershipChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Set([]byte("flap-key"), []byte("flap-value")))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 83, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target2 := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	target3 := testcluster.StartNode(t, 3, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target2.Close(t)
	defer target3.Close(t)

	wireAll := func() {
		seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
		target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
		target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})
	}
	wireAll()
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 83, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     83,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{
			{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.Addr()},
			{StoreID: 3, PeerID: 301, TargetAdminAddr: target3.Addr()},
		},
	})
	require.NoError(t, err)

	stopFlap := make(chan struct{})
	doneFlap := make(chan struct{})
	go func() {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		defer close(doneFlap)
		blocked := false
		for {
			select {
			case <-stopFlap:
				if blocked {
					seed.UnblockPeer(301)
					target3.UnblockPeer(101)
				}
				return
			case <-ticker.C:
				if blocked {
					seed.UnblockPeer(301)
					target3.UnblockPeer(101)
				} else {
					seed.BlockPeer(301)
					target3.BlockPeer(101)
				}
				blocked = !blocked
			}
		}
	}()

	transferResult, err := migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            seed.Addr(),
		TargetAdminAddr: target2.Addr(),
		RegionID:        83,
		PeerID:          201,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, transferResult.TargetLeader)

	close(stopFlap)
	<-doneFlap

	stopFlap = make(chan struct{})
	doneFlap = make(chan struct{})
	go func() {
		ticker := time.NewTicker(40 * time.Millisecond)
		defer ticker.Stop()
		defer close(doneFlap)
		blocked := false
		for {
			select {
			case <-stopFlap:
				if blocked {
					target2.UnblockPeer(301)
					target3.UnblockPeer(201)
				}
				return
			case <-ticker.C:
				if blocked {
					target2.UnblockPeer(301)
					target3.UnblockPeer(201)
				} else {
					target2.BlockPeer(301)
					target3.BlockPeer(201)
				}
				blocked = !blocked
			}
		}
	}()

	removeResult, err := migrate.RemovePeer(ctx, migrate.RemovePeerConfig{
		Addr:         target2.Addr(),
		RegionID:     83,
		PeerID:       301,
		WaitTimeout:  8 * time.Second,
		PollInterval: 20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.False(t, regionHasPeer(removeResult.LeaderRegion, 301))

	close(stopFlap)
	<-doneFlap

	require.Eventually(t, func() bool {
		status := testcluster.FetchRuntimeStatus(t, ctx, target3.Addr(), 83)
		return !status.GetKnown() || !status.GetHosted() || status.GetLocalPeerId() != 301
	}, 5*time.Second, 50*time.Millisecond)

	status := testcluster.FetchRuntimeStatus(t, ctx, target2.Addr(), 83)
	require.True(t, status.GetKnown())
	require.Len(t, status.GetRegion().GetPeers(), 2)
	testcluster.AssertValue(t, seed.DB, []byte("flap-key"), []byte("flap-value"))
	testcluster.AssertValue(t, target2.DB, []byte("flap-key"), []byte("flap-value"))
}
