package integration

import (
	"context"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	"testing"
	"time"

	localmeta "github.com/feichai0017/NoKV/raftstore/localmeta"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
)

func TestSplitMergeRestartSafetyAcrossStores(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Set([]byte("bravo"), []byte("v1")))
	require.NoError(t, standalone.Set([]byte("tango"), []byte("v2")))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 71, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target := testcluster.StartNode(t, 2, t.TempDir(), nil, false)
	defer seed.Close(t)
	defer target.Close(t)

	wireAll := func() {
		seed.WirePeers(map[uint64]string{201: target.Addr(), 202: target.Addr()})
		target.WirePeers(map[uint64]string{101: seed.Addr(), 102: seed.Addr()})
	}
	wireAll()
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 71, 101)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     71,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.NoError(t, err)

	parentLeader, _ := testcluster.FindLeader(t, ctx, 71, seed, target)
	childMeta := localmeta.RegionMeta{
		ID:       72,
		StartKey: []byte("m"),
		EndKey:   nil,
		Epoch: metaregion.Epoch{
			Version:     1,
			ConfVersion: 1,
		},
		Peers: []metaregion.Peer{
			{StoreID: 1, PeerID: 102},
			{StoreID: 2, PeerID: 202},
		},
	}
	require.NoError(t, parentLeader.Server.Store().ProposeSplit(71, childMeta, childMeta.StartKey))
	require.Eventually(t, func() bool {
		a := testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 72)
		b := testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 72)
		return a.GetKnown() && a.GetHosted() && b.GetKnown() && b.GetHosted()
	}, 5*time.Second, 20*time.Millisecond, testcluster.DumpStatus(t, ctx, 72, seed, target))
	_, _ = testcluster.FindLeader(t, ctx, 72, seed, target)

	seed.Restart(t, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target.Restart(t, nil, true)
	wireAll()
	require.Eventually(t, func() bool {
		return testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 71).GetKnown() &&
			testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 71).GetKnown() &&
			testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 72).GetKnown() &&
			testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 72).GetKnown()
	}, 5*time.Second, 20*time.Millisecond)

	parentLeader, _ = testcluster.FindLeader(t, ctx, 71, seed, target)
	require.NoError(t, parentLeader.Server.Store().ProposeMerge(71, 72))
	require.Eventually(t, func() bool {
		return !testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 72).GetKnown() &&
			!testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 72).GetKnown()
	}, 5*time.Second, 20*time.Millisecond)

	seed.Restart(t, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	target.Restart(t, nil, true)
	wireAll()
	require.Eventually(t, func() bool {
		return testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 71).GetKnown() &&
			testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 71).GetKnown() &&
			!testcluster.FetchRuntimeStatus(t, ctx, seed.Addr(), 72).GetKnown() &&
			!testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 72).GetKnown()
	}, 5*time.Second, 20*time.Millisecond)

	testcluster.AssertValue(t, seed.DB, []byte("bravo"), []byte("v1"))
	testcluster.AssertValue(t, target.DB, []byte("tango"), []byte("v2"))
}
