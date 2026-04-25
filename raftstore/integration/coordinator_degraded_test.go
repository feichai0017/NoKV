package integration

import (
	"context"
	"testing"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestClusterSurvivesCoordinatorUnavailableAfterStartup(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	coord := testcluster.StartCoordinator(t)
	defer coord.Close(t)

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	_, err := migrate.Init(migrate.InitConfig{WorkDir: seedDir, StoreID: 1, RegionID: 61, PeerID: 101})
	require.NoError(t, err)

	coord.JoinStore(t, 1)
	coord.JoinStore(t, 2)
	seed := testcluster.StartNodeWithConfig(t, 1, seedDir, testcluster.NodeConfig{
		AllowedModes:      []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster},
		StartPeers:        true,
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	target := testcluster.StartNodeWithConfig(t, 2, t.TempDir(), testcluster.NodeConfig{
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	defer seed.Close(t)
	defer target.Close(t)

	seed.WirePeers(map[uint64]string{201: target.Addr()})
	target.WirePeers(map[uint64]string{101: seed.Addr()})
	testcluster.WaitForLeaderPeer(t, ctx, seed.Addr(), 61, 101)
	testcluster.WaitForSchedulerMode(t, seed, storepkg.SchedulerModeHealthy, false)
	testcluster.WaitForSchedulerMode(t, target, storepkg.SchedulerModeHealthy, false)

	_, err = migrate.Expand(ctx, migrate.ExpandConfig{
		Addr:         seed.Addr(),
		RegionID:     61,
		WaitTimeout:  5 * time.Second,
		PollInterval: 20 * time.Millisecond,
		Targets:      []migrate.PeerTarget{{StoreID: 2, PeerID: 201, TargetAdminAddr: target.Addr()}},
	})
	require.NoError(t, err)

	resolver, err := coordclient.NewGRPCClient(ctx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	cli, err := client.New(client.Config{
		StoreResolver:  resolver,
		RegionResolver: resolver,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	defer func() { _ = cli.Close() }()

	key := []byte("coordinator-outage-key")
	value := []byte("coordinator-outage-value")
	require.NoError(t, cli.Put(ctx, key, value, 10, 11, 3000))
	getResp, err := cli.Get(ctx, key, 12)
	require.NoError(t, err)
	require.Equal(t, value, getResp.GetValue())

	coord.Close(t)
	testcluster.WaitForSchedulerMode(t, seed, storepkg.SchedulerModeUnavailable, true)
	testcluster.WaitForSchedulerMode(t, target, storepkg.SchedulerModeUnavailable, true)

	updated := []byte("coordinator-outage-updated")
	require.NoError(t, cli.Put(ctx, key, updated, 20, 21, 3000))
	require.Eventually(t, func() bool {
		entry, err := target.DB.Get(key)
		return err == nil && string(entry.Value) == string(updated)
	}, 5*time.Second, 20*time.Millisecond)

	getResp, err = cli.Get(ctx, key, 30)
	require.NoError(t, err)
	require.Equal(t, updated, getResp.GetValue())

	dialCtx, dialCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer dialCancel()
	newResolver, err := coordclient.NewGRPCClient(dialCtx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.Error(t, err)
	require.Nil(t, newResolver)

	staleResolver := &unavailableResolver{}
	coldCli, err := client.New(client.Config{
		StoreResolver:  staleResolver,
		RegionResolver: staleResolver,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
		Retry: client.RetryPolicy{
			MaxAttempts:             1,
			RouteUnavailableBackoff: 0,
		},
	})
	require.NoError(t, err)
	defer func() { _ = coldCli.Close() }()

	_, err = coldCli.Get(ctx, key, 30)
	require.Error(t, err)
	require.True(t, client.IsRouteUnavailable(err))
}

type unavailableResolver struct{}

func (u *unavailableResolver) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	return nil, context.DeadlineExceeded
}

func (u *unavailableResolver) GetStore(context.Context, *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	return nil, context.DeadlineExceeded
}

func (u *unavailableResolver) Close() error { return nil }
