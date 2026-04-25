package integration

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
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
