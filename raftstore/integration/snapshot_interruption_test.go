package integration

import (
	"context"
	"testing"
	"time"

	NoKV "github.com/feichai0017/NoKV"
	"github.com/feichai0017/NoKV/raftstore/failpoints"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
)

func TestExpandSnapshotInstallInterruptedBeforePublish(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, func(opt *NoKV.Options) {
		opt.ValueThreshold = 8
	})
	key := []byte("snapshot-interrupt-key")
	value := make([]byte, 2048)
	for i := range value {
		value[i] = byte('a' + (i % 17))
	}
	require.NoError(t, standalone.Set(key, value))
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 51, PeerID: 101})
	require.NoError(t, err)

	seed := testcluster.StartNode(t, 1, seedDir, []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster}, true)
	targetDir := t.TempDir()
	target := testcluster.StartNode(t, 2, targetDir, nil, false)
	defer seed.Close(t)
	defer target.Close(t)

	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 51, 101)

	failpoints.Set(failpoints.AfterSnapshotApplyBeforePublish)
	defer failpoints.Set(failpoints.None)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     51,
		WaitTimeout:  2 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "after snapshot apply before publish")

	status := testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 51)
	require.False(t, status.GetHosted())
	require.False(t, status.GetKnown())

	target.Restart(t, nil, true)
	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	status = testcluster.FetchRuntimeStatus(t, ctx, target.Addr(), 51)
	require.False(t, status.GetHosted())
	require.False(t, status.GetKnown())

	failpoints.Set(failpoints.None)
	result, err := migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     51,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.True(t, result.Results[0].TargetHosted)
	testcluster.AssertValue(t, target.DB, key, value)
}
