package integration

import (
	"context"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	adminpb "github.com/feichai0017/NoKV/pb/admin"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
)

const restartRecoveryTimeout = 40 * time.Second

func waitForStableLeader(tb testing.TB, ctx context.Context, regionID uint64, nodes ...*testcluster.Node) (*testcluster.Node, *adminpb.RegionRuntimeStatusResponse) {
	tb.Helper()
	deadline := time.Now().Add(10 * time.Second)
	var stableLeader uint64
	var stableCount int
	for time.Now().Before(deadline) {
		var leaderNode *testcluster.Node
		var leaderStatus *adminpb.RegionRuntimeStatusResponse
		var leaderPeerID uint64
		consistent := true
		for _, node := range nodes {
			status := testcluster.FetchRuntimeStatus(tb, ctx, node.Addr(), regionID)
			if !status.GetKnown() || !status.GetHosted() || status.GetLeaderPeerId() == 0 {
				consistent = false
				break
			}
			if leaderPeerID == 0 {
				leaderPeerID = status.GetLeaderPeerId()
			} else if leaderPeerID != status.GetLeaderPeerId() {
				consistent = false
				break
			}
			if status.GetLeader() {
				if leaderNode != nil {
					consistent = false
					break
				}
				leaderNode = node
				leaderStatus = status
			}
		}
		if consistent && leaderNode != nil {
			if stableLeader == leaderPeerID {
				stableCount++
			} else {
				stableLeader = leaderPeerID
				stableCount = 1
			}
			if stableCount >= 10 {
				return leaderNode, leaderStatus
			}
		} else {
			stableLeader = 0
			stableCount = 0
		}
		time.Sleep(50 * time.Millisecond)
	}
	tb.Fatalf("timed out waiting for stable leader on region %d: %s", regionID, testcluster.DumpStatus(tb, ctx, regionID, nodes...))
	return nil, nil
}

func TestExpandedPeerRestartPreservesRegionAndData(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), restartRecoveryTimeout)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 8
	})
	key := []byte("restart-key")
	value := make([]byte, 2048)
	for i := range value {
		value[i] = byte('k' + (i % 7))
	}
	require.NoError(t, standalone.Set(key, value))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 9, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	targetDir := t.TempDir()
	target := testcluster.StartNode(t, 2, targetDir, nil, false)
	defer seed.Close(t)
	defer target.Close(t)

	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 9, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     9,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.NoError(t, err)
	testcluster.AssertValue(t, target.DB, key, value)

	target.Restart(t, nil, true)
	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForHostedPeer(t, ctx, target.Addr(), 9, 201)
	testcluster.AssertValue(t, target.DB, key, value)
}

func TestExpandedPeerRestartPreservesRegionAndDataWithSST(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), restartRecoveryTimeout)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 8
	})
	key := []byte("restart-sst-key")
	value := make([]byte, 2048)
	for i := range value {
		value[i] = byte('q' + (i % 7))
	}
	require.NoError(t, standalone.Set(key, value))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 19, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	targetDir := t.TempDir()
	target := testcluster.StartNode(t, 2, targetDir, nil, false)
	defer seed.Close(t)
	defer target.Close(t)

	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 19, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     19,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.NoError(t, err)
	testcluster.AssertValue(t, target.DB, key, value)

	target.Restart(t, nil, true)
	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForHostedPeer(t, ctx, target.Addr(), 19, 201)
	testcluster.AssertValue(t, target.DB, key, value)
}

func TestRemovedPeerRestartDoesNotRehost(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), restartRecoveryTimeout)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	key := []byte("removed-peer-key")
	value := []byte("removed-peer-value")
	require.NoError(t, standalone.Set(key, value))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 21, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target.Close(t)

	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 21, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     21,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.NoError(t, err)

	_, err = migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            seed.Addr(),
		TargetAdminAddr: target.Addr(),
		RegionID:        21,
		PeerID:          201,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)

	opCtx, opCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer opCancel()
	_, err = migrate.RemovePeer(opCtx, migrate.RemovePeerConfig{
		Addr:            target.Addr(),
		TargetAdminAddr: seed.Addr(),
		RegionID:        21,
		PeerID:          101,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	status := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 21)
	require.False(t, status.GetHosted())

	seed.Restart(t, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForNotHosted(t, ctx, seed.Addr(), 21)
	testcluster.AssertValue(t, target.DB, key, value)
}

func TestLeaderRestartStillAllowsMembershipChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	key := []byte("leader-restart-key")
	value := []byte("leader-restart-value")
	require.NoError(t, standalone.Set(key, value))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 31, PeerID: 101})
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
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 31, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     31,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.Addr()}, {StoreID: 3, PeerID: 301, TargetAdminAddr: target3.Addr()}},
	})
	require.NoError(t, err)

	_, err = migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            seed.Addr(),
		TargetAdminAddr: target2.Addr(),
		RegionID:        31,
		PeerID:          201,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)

	target2.Restart(t, nil, true)
	seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
	target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
	target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})

	testcluster.WaitForHostedPeer(t, ctx, target2.Addr(), 31, 201)
	leaderNode, leaderStatus := waitForStableLeader(t, ctx, 31, seed, target2, target3)
	removeTarget := seed
	removePeerID := uint64(101)
	if leaderStatus.GetLeaderPeerId() == 101 {
		removeTarget = target3
		removePeerID = 301
	}

	opCtx, opCancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer opCancel()
	_, err = migrate.RemovePeer(opCtx, migrate.RemovePeerConfig{
		Addr:            leaderNode.Addr(),
		TargetAdminAddr: removeTarget.Addr(),
		RegionID:        31,
		PeerID:          removePeerID,
		WaitTimeout:     20 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err, testcluster.DumpStatus(t, ctx, 31, seed, target2, target3))

	testcluster.WaitForNotHosted(t, ctx, removeTarget.Addr(), 31)
	remainingA := target2
	remainingB := seed
	if removeTarget.StoreID == 1 {
		remainingB = target3
	}
	testcluster.AssertValue(t, remainingA.DB, key, value)
	testcluster.AssertValue(t, remainingB.DB, key, value)
}
