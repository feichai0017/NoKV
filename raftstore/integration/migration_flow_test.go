package integration

import (
	"context"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
)

func TestMigrationFlowEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 16
	})
	smallKey := []byte("small-key")
	smallValue := []byte("small-value")
	largeKey := []byte("large-key")
	largeValue := make([]byte, 4096)
	for i := range largeValue {
		largeValue[i] = byte('a' + (i % 23))
	}
	require.NoError(t, standalone.Set(smallKey, smallValue))
	require.NoError(t, standalone.Set(largeKey, largeValue))
	require.NoError(t, standalone.Close())

	plan, err := migrate.BuildPlan(seedDir)
	require.NoError(t, err)
	require.True(t, plan.Eligible)
	_, err = migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 1, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target2 := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	target3 := testcluster.StartNode(t, 3, t.TempDir(), nil, false)
	defer target3.Close(t)
	defer target2.Close(t)
	defer seed.Close(t)

	seed.WirePeers(map[uint64]string{201: target2.Addr(), 301: target3.Addr()})
	target2.WirePeers(map[uint64]string{101: seed.Addr(), 301: target3.Addr()})
	target3.WirePeers(map[uint64]string{101: seed.Addr(), 201: target2.Addr()})

	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 1, 101)

	expandResult, err := migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     1,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets: []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target2.Addr()}, {StoreID: 3, PeerID: 301, TargetAdminAddr: target3.Addr()}},
	})
	require.NoError(t, err)
	require.Len(t, expandResult.Results, 2)
	for _, step := range expandResult.Results {
		require.True(t, step.TargetHosted)
		require.Greater(t, step.TargetAppliedIdx, uint64(0))
	}

	testcluster.AssertValue(t, target2.DB, smallKey, smallValue)
	testcluster.AssertValue(t, target2.DB, largeKey, largeValue)
	testcluster.AssertValue(t, target3.DB, smallKey, smallValue)
	testcluster.AssertValue(t, target3.DB, largeKey, largeValue)

	transferResult, err := migrate.TransferLeader(ctx, migrate.TransferLeaderConfig{
		Addr:            seed.Addr(),
		TargetAdminAddr: target2.Addr(),
		RegionID:        1,
		PeerID:          201,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.True(t, transferResult.TargetLeader)
	require.Equal(t, uint64(201), transferResult.LeaderPeerID)

	removeResult, err := migrate.RemovePeer(ctx, migrate.RemovePeerConfig{
		Addr:            target2.Addr(),
		TargetAdminAddr: seed.Addr(),
		RegionID:        1,
		PeerID:          101,
		WaitTimeout:     5 * time.Second,
		PollInterval:    20 * time.Millisecond,
	})
	require.NoError(t, err)
	require.False(t, removeResult.TargetHosted)

	leaderStatus := testcluster.FetchRuntimeStatus(t, ctx, target2.Addr(), 1)
	require.True(t, leaderStatus.GetKnown())
	require.Len(t, leaderStatus.GetRegion().GetPeers(), 2)
	for _, p := range leaderStatus.GetRegion().GetPeers() {
		require.NotEqual(t, uint64(101), p.GetPeerId())
	}

	seedStatus := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 1)
	require.False(t, seedStatus.GetHosted())
	testcluster.AssertValue(t, target2.DB, largeKey, largeValue)
}
