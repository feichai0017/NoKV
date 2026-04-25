package integration

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaclient "github.com/feichai0017/NoKV/fsmeta/client"
	fswatch "github.com/feichai0017/NoKV/fsmeta/exec/watch"
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

	require.NoError(t, cli.RenameSubtree(ctx, fsmeta.RenameSubtreeRequest{
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

func TestFSMetadataHardLinkLifecycleOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, executor)
	defer cleanup()

	mount := fsmeta.MountID("vol")
	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "original",
		Inode:  901,
	}, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Size:      8192,
		Mode:      0o644,
		LinkCount: 1,
	}))

	require.NoError(t, cli.Link(ctx, fsmeta.LinkRequest{
		Mount:      mount,
		FromParent: fsmeta.RootInode,
		FromName:   "original",
		ToParent:   fsmeta.RootInode,
		ToName:     "alias",
	}))

	linked, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"alias", "original"}, dentryNames(linked))
	require.Equal(t, uint32(2), inodeLinkCount(t, linked, "alias"))
	require.Equal(t, uint32(2), inodeLinkCount(t, linked, "original"))

	require.NoError(t, cli.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "original",
	}))
	afterFirstUnlink, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"alias"}, dentryNames(afterFirstUnlink))
	require.Equal(t, uint32(1), inodeLinkCount(t, afterFirstUnlink, "alias"))

	require.NoError(t, cli.Unlink(ctx, fsmeta.UnlinkRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "alias",
	}))
	afterLastUnlink, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Empty(t, afterLastUnlink)
}

func TestFSMetadataWatchSubtreeOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runtime := openRealClusterRuntime(t, ctx)
	router := fswatch.NewRouter()
	reg, err := runtime.node.Server.Store().RegisterApplyObserver(router, 256)
	require.NoError(t, err)
	defer reg.Close()

	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor, fsmetaserver.WithWatcher(router))
	defer cleanup()

	prefix, err := fsmeta.EncodeDentryPrefix("vol", fsmeta.RootInode)
	require.NoError(t, err)
	stream, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: 8,
	})
	require.NoError(t, err)
	defer func() { _ = stream.Close() }()

	req := fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "watched-checkpoint",
		Inode:  77,
	}
	require.NoError(t, cli.Create(ctx, req, fsmeta.InodeRecord{
		Type:      fsmeta.InodeTypeFile,
		Size:      1,
		Mode:      0o644,
		LinkCount: 1,
	}))
	wantKey, err := fsmeta.EncodeDentryKey(req.Mount, req.Parent, req.Name)
	require.NoError(t, err)

	var got fsmeta.WatchEvent
	require.Eventually(t, func() bool {
		evt, err := stream.Recv()
		if err != nil {
			return false
		}
		got = evt
		return string(evt.Key) == string(wantKey)
	}, 5*time.Second, 20*time.Millisecond)
	require.Equal(t, fsmeta.WatchEventSourceCommit, got.Source)
	require.NotZero(t, got.CommitVersion)
	require.NotZero(t, got.Cursor.Index)
	require.NoError(t, stream.Ack(got.Cursor))
}

func TestFSMetadataWatchSubtreeReplaysAfterResumeCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runtime := openRealClusterRuntime(t, ctx)
	router := fswatch.NewRouter()
	reg, err := runtime.node.Server.Store().RegisterApplyObserver(router, 256)
	require.NoError(t, err)
	defer reg.Close()

	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor, fsmetaserver.WithWatcher(router))
	defer cleanup()

	prefix, err := fsmeta.EncodeDentryPrefix("vol", fsmeta.RootInode)
	require.NoError(t, err)
	stream, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: 8,
	})
	require.NoError(t, err)

	first := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "catchup-0001", Inode: 801}
	require.NoError(t, cli.Create(ctx, first, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}))
	firstKey, err := fsmeta.EncodeDentryKey(first.Mount, first.Parent, first.Name)
	require.NoError(t, err)
	firstEvent := recvWatchKey(t, stream, firstKey)
	require.NoError(t, stream.Ack(firstEvent.Cursor))
	require.NoError(t, stream.Close())

	second := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "catchup-0002", Inode: 802}
	third := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "catchup-0003", Inode: 803}
	require.NoError(t, cli.Create(ctx, second, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}))
	require.NoError(t, cli.Create(ctx, third, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}))
	secondKey, err := fsmeta.EncodeDentryKey(second.Mount, second.Parent, second.Name)
	require.NoError(t, err)
	thirdKey, err := fsmeta.EncodeDentryKey(third.Mount, third.Parent, third.Name)
	require.NoError(t, err)

	resumed, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		ResumeCursor:       firstEvent.Cursor,
		BackPressureWindow: 8,
	})
	require.NoError(t, err)
	defer func() { _ = resumed.Close() }()
	require.NotZero(t, resumed.ReadyCursor().Index)

	got := map[string]bool{}
	require.Eventually(t, func() bool {
		evt, err := resumed.Recv()
		if err != nil {
			return false
		}
		got[string(evt.Key)] = true
		_ = resumed.Ack(evt.Cursor)
		return got[string(secondKey)] && got[string(thirdKey)]
	}, 5*time.Second, 20*time.Millisecond)
}

func TestFSMetadataSnapshotSubtreeOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)
	publisher := &snapshotRecorder{}
	cli, cleanup := openFSMetadataClient(t, ctx, executor, fsmetaserver.WithSnapshotPublisher(publisher))
	defer cleanup()

	mount := fsmeta.MountID("vol")
	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "a",
		Inode:  501,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}))

	token, err := cli.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{
		Mount:     mount,
		RootInode: fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.Equal(t, mount, token.Mount)
	require.Equal(t, fsmeta.RootInode, token.RootInode)
	require.NotZero(t, token.ReadVersion)
	require.Equal(t, token, publisher.token)

	require.NoError(t, cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "b",
		Inode:  502,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile, LinkCount: 1}))

	snapshotPage, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:           mount,
		Parent:          fsmeta.RootInode,
		Limit:           8,
		SnapshotVersion: token.ReadVersion,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a"}, dentryNames(snapshotPage))

	latestPage, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []string{"a", "b"}, dentryNames(latestPage))

	require.NoError(t, cli.RetireSnapshotSubtree(ctx, token))
	require.Equal(t, token, publisher.retired)
}

type snapshotRecorder struct {
	token   fsmeta.SnapshotSubtreeToken
	retired fsmeta.SnapshotSubtreeToken
}

func (r *snapshotRecorder) PublishSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	r.token = token
	return nil
}

func (r *snapshotRecorder) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	r.retired = token
	return nil
}

func recvWatchKey(t *testing.T, stream fsmetaclient.WatchSubscription, key []byte) fsmeta.WatchEvent {
	t.Helper()
	var got fsmeta.WatchEvent
	require.Eventually(t, func() bool {
		evt, err := stream.Recv()
		if err != nil {
			return false
		}
		got = evt
		return string(evt.Key) == string(key)
	}, 5*time.Second, 20*time.Millisecond)
	return got
}

func dentryNames(entries []fsmeta.DentryAttrPair) []string {
	out := make([]string, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.Dentry.Name)
	}
	return out
}

func inodeLinkCount(t *testing.T, entries []fsmeta.DentryAttrPair, name string) uint32 {
	t.Helper()
	for _, entry := range entries {
		if entry.Dentry.Name == name {
			return entry.Inode.LinkCount
		}
	}
	t.Fatalf("dentry %q not found in %v", name, dentryNames(entries))
	return 0
}

func openFSMetadataClient(t *testing.T, ctx context.Context, executor fsmetaserver.Executor, opts ...fsmetaserver.Option) (*fsmetaclient.GRPCClient, func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	grpcServer := grpc.NewServer()
	fsmetaserver.Register(grpcServer, executor, opts...)
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
