package integration

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	coordclient "github.com/feichai0017/NoKV/coordinator/client"
	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetaexec "github.com/feichai0017/NoKV/fsmeta/exec"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	"github.com/feichai0017/NoKV/raftstore/client"
	"github.com/feichai0017/NoKV/raftstore/migrate"
	raftmode "github.com/feichai0017/NoKV/raftstore/mode"
	storepkg "github.com/feichai0017/NoKV/raftstore/store"
	"github.com/feichai0017/NoKV/raftstore/testcluster"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestFSMetadataClientServerOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, executor)
	defer cleanup()

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint-0001",
		Inode:  42,
	}
	require.NoError(t, cli.Create(ctx, req, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	}))

	record, err := cli.Lookup(ctx, fsmeta.LookupRequest{
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

	pairs, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{
			Parent: req.Parent,
			Name:   req.Name,
			Inode:  req.Inode,
			Type:   fsmeta.InodeTypeFile,
		},
		Inode: fsmeta.InodeRecord{
			Inode:     req.Inode,
			Type:      fsmeta.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}}, pairs)

	err = cli.Create(ctx, req, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.True(t, errors.Is(err, fsmeta.ErrExists), "duplicate create error = %v", err)

	require.NoError(t, cli.Rename(ctx, fsmeta.RenameRequest{
		Mount:      req.Mount,
		FromParent: req.Parent,
		FromName:   req.Name,
		ToParent:   req.Parent,
		ToName:     "checkpoint-0002",
	}))
	_, err = cli.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	require.NoError(t, cli.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   "checkpoint-0002",
	}))
	_, err = cli.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   "checkpoint-0002",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func openFSMetadataClient(t *testing.T, ctx context.Context, executor fsmetaserver.Executor) (*fsmetaclient.GRPCClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	fsmetaserver.Register(grpcServer, executor)
	go func() {
		_ = grpcServer.Serve(ln)
	}()
	cli, err := fsmetaclient.NewGRPCClient(ctx, ln.Addr().String())
	require.NoError(t, err)
	return cli, func() {
		_ = cli.Close()
		grpcServer.Stop()
		_ = ln.Close()
	}
}

func openRealClusterExecutor(t *testing.T, ctx context.Context) *fsmetaexec.Executor {
	t.Helper()

	coord := testcluster.StartCoordinator(t)
	t.Cleanup(func() { coord.Close(t) })

	seedDir := t.TempDir()
	standalone := testcluster.OpenStandaloneDB(t, seedDir, nil)
	require.NoError(t, standalone.Close())

	const (
		storeID  = uint64(1)
		regionID = uint64(171)
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
	t.Cleanup(func() { _ = coordRPC.Close() })
	kv, err := client.New(client.Config{
		Stores: []client.StoreEndpoint{
			{StoreID: storeID, Addr: node.Addr()},
		},
		RegionResolver: coordRPC,
		DialOptions:    []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())},
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = kv.Close() })

	runner, err := fsmetaexec.NewRaftstoreRunner(kv, coordRPC)
	require.NoError(t, err)
	executor, err := fsmetaexec.New(runner)
	require.NoError(t, err)
	return executor
}
