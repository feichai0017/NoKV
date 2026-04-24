package exec

import (
	"context"
	"errors"
	"testing"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestRaftstoreRunnerExecutorContractOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint-0001",
		Inode:  42,
	}
	err := executor.Create(ctx, req, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Size:      4096,
		LinkCount: 1,
		Mode:      0o644,
	})
	require.NoError(t, err)

	record, err := executor.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	entries, err := executor.ReadDir(ctx, fsmeta.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryRecord{{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  req.Inode,
		Type:   fsmeta.InodeTypeFile,
	}}, entries)

	err = executor.Create(ctx, req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.True(t, errors.Is(err, fsmeta.ErrExists), "duplicate create error = %v", err)
}

func openRealClusterExecutor(t *testing.T, ctx context.Context) *Executor {
	t.Helper()

	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID  = uint64(1)
		regionID = uint64(121)
		peerID   = uint64(101)
	)
	_, err := migrate.Init(migrate.InitConfig{
		WorkDir:  seedDir,
		StoreID:  storeID,
		RegionID: regionID,
		PeerID:   peerID,
	})
	require.NoError(t, err)

	node := testcluster.StartNodeWithConfig(t, storeID, seedDir, testcluster.NodeConfig{
		AllowedModes:      []raftmode.Mode{raftmode.ModeSeeded, raftmode.ModeCluster},
		StartPeers:        true,
		Scheduler:         testcluster.NewScheduler(t, coord.Addr(), 100*time.Millisecond),
		HeartbeatInterval: 50 * time.Millisecond,
	})
	t.Cleanup(func() { node.Close(t) })

	testcluster.WaitForLeaderPeer(t, ctx, node.Addr(), regionID, peerID)
	testcluster.WaitForSchedulerMode(t, node, storepkg.SchedulerModeHealthy, false)

	coordRPC, err := coordclient.NewGRPCClient(ctx, coord.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	kv, err := client.New(client.Config{
		Stores: []client.StoreEndpoint{
			{StoreID: storeID, Addr: node.Addr()},
		},
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := NewRaftstoreRunner(kv, coordRPC)
	require.NoError(t, err)
	executor, err := New(runner)
	require.NoError(t, err)
	return executor
}
