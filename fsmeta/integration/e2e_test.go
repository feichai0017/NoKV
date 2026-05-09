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
	fsmetaraftstore "github.com/feichai0017/NoKV/fsmeta/runtime/raftstore"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type staticMountWatcher struct {
	router   *fswatch.Router
	identity fsmeta.MountIdentity
}

func (w staticMountWatcher) Subscribe(ctx context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	if req.Mount != "" {
		prefix, err := fsmeta.WatchPrefixForMount(req, w.identity)
		if err != nil {
			return nil, err
		}
		req.KeyPrefix = prefix
	}
	return w.router.Subscribe(ctx, req)
}

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
		Attrs: fsmeta.CreateAttrs{
			Type:        fsmeta.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint-0001","sha256":"abc"}`),
		},
	}
	created, err := cli.Create(ctx, req)
	require.NoError(t, err)

	record, err := cli.Lookup(ctx, fsmeta.LookupRequest{
		Mount:  req.Mount,
		Parent: req.Parent,
		Name:   req.Name,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: req.Parent,
		Name:   req.Name,
		Inode:  created.Inode.Inode,
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
			Inode:  created.Inode.Inode,
			Type:   fsmeta.InodeTypeFile,
		},
		Inode: fsmeta.InodeRecord{
			Inode:       created.Inode.Inode,
			Type:        fsmeta.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			LinkCount:   1,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint-0001","sha256":"abc"}`),
		},
	}}, pairs)

	_, err = cli.Create(ctx, req)
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

func TestFSMetadataHardLinkLifecycleOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, executor)
	defer cleanup()

	mount := fsmeta.MountID("vol")
	_, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "original",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 8192, Mode: 0o644},
	})
	require.NoError(t, err)

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
	reg, err := runtime.node.Server.Store().RegisterApplyObserver(fsmetaraftstore.NewApplyObserver(router), 256)
	require.NoError(t, err)
	defer reg.Close()

	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor, fsmetaserver.WithWatcher(staticMountWatcher{router: router, identity: runtime.mountIdentity}))
	defer cleanup()

	prefix, err := fsmeta.EncodeDentryPrefix(runtime.mountIdentity, fsmeta.RootInode)
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
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 1, Mode: 0o644},
	}
	_, err = cli.Create(ctx, req)
	require.NoError(t, err)
	wantKey, err := fsmeta.EncodeDentryKey(runtime.mountIdentity, req.Parent, req.Name)
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
	reg, err := runtime.node.Server.Store().RegisterApplyObserver(fsmetaraftstore.NewApplyObserver(router), 256)
	require.NoError(t, err)
	defer reg.Close()

	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor, fsmetaserver.WithWatcher(staticMountWatcher{router: router, identity: runtime.mountIdentity}))
	defer cleanup()

	prefix, err := fsmeta.EncodeDentryPrefix(runtime.mountIdentity, fsmeta.RootInode)
	require.NoError(t, err)
	stream, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		KeyPrefix:          prefix,
		BackPressureWindow: 8,
	})
	require.NoError(t, err)

	first := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "catchup-0001", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = cli.Create(ctx, first)
	require.NoError(t, err)
	firstKey, err := fsmeta.EncodeDentryKey(runtime.mountIdentity, first.Parent, first.Name)
	require.NoError(t, err)
	firstEvent := recvWatchKey(t, stream, firstKey)
	require.NoError(t, stream.Ack(firstEvent.Cursor))
	require.NoError(t, stream.Close())

	second := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "catchup-0002", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	third := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "catchup-0003", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = cli.Create(ctx, second)
	require.NoError(t, err)
	_, err = cli.Create(ctx, third)
	require.NoError(t, err)
	secondKey, err := fsmeta.EncodeDentryKey(runtime.mountIdentity, second.Parent, second.Name)
	require.NoError(t, err)
	thirdKey, err := fsmeta.EncodeDentryKey(runtime.mountIdentity, third.Parent, third.Name)
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

func TestFSMetadataWatchSubtreeReconcilesAfterExpiredCursor(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	runtime := openRealClusterRuntime(t, ctx)
	router := fswatch.NewRouter()
	reg, err := runtime.node.Server.Store().RegisterApplyObserver(fsmetaraftstore.NewApplyObserver(router), 256)
	require.NoError(t, err)
	defer reg.Close()

	cli, cleanup := openFSMetadataClient(t, ctx, runtime.executor, fsmetaserver.WithWatcher(staticMountWatcher{router: router, identity: runtime.mountIdentity}))
	defer cleanup()

	warmup, err := cli.WatchSubtree(ctx, fsmeta.WatchRequest{
		Mount:              "vol",
		RootInode:          fsmeta.RootInode,
		BackPressureWindow: 8,
	})
	require.NoError(t, err)

	baseline := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "baseline-artifact", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = cli.Create(ctx, baseline)
	require.NoError(t, err)
	baselineKey, err := fsmeta.EncodeDentryKey(runtime.mountIdentity, baseline.Parent, baseline.Name)
	require.NoError(t, err)
	baselineEvent := recvWatchKey(t, warmup, baselineKey)
	require.NotZero(t, baselineEvent.Cursor.RegionID)
	require.NotZero(t, baselineEvent.Cursor.Index)
	require.NoError(t, warmup.Ack(baselineEvent.Cursor))
	require.NoError(t, warmup.Close())

	expired := baselineEvent.Cursor
	expired.Index = 0

	result, err := fsmetaclient.WatchDirectoryWithReconcile(ctx, cli,
		fsmeta.WatchRequest{
			Mount:              "vol",
			RootInode:          fsmeta.RootInode,
			ResumeCursor:       expired,
			BackPressureWindow: 8,
		},
		fsmeta.ReadDirRequest{Mount: "vol", Parent: fsmeta.RootInode, Limit: 8},
	)
	require.NoError(t, err)
	defer func() { _ = result.Subscription.Close() }()
	require.True(t, result.Reconciled)
	require.Equal(t, []string{"baseline-artifact"}, dentryNames(result.Snapshot))

	live := fsmeta.CreateRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: "live-after-reconcile", Attrs: fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile}}
	_, err = cli.Create(ctx, live)
	require.NoError(t, err)
	liveKey, err := fsmeta.EncodeDentryKey(runtime.mountIdentity, live.Parent, live.Name)
	require.NoError(t, err)
	got := recvWatchKey(t, result.Subscription, liveKey)
	require.NoError(t, result.Subscription.Ack(got.Cursor))
}

func TestFSMetadataSnapshotSubtreeOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)
	publisher := &snapshotRecorder{}
	cli, cleanup := openFSMetadataClient(t, ctx, executor, fsmetaserver.WithSnapshotPublisher(publisher))
	defer cleanup()

	mount := fsmeta.MountID("vol")
	_, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "a",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

	token, err := cli.SnapshotSubtree(ctx, fsmeta.SnapshotSubtreeRequest{
		Mount:     mount,
		RootInode: fsmeta.RootInode,
	})
	require.NoError(t, err)
	require.Equal(t, mount, token.Mount)
	require.Equal(t, fsmeta.RootInode, token.RootInode)
	require.NotZero(t, token.ReadVersion)
	require.Equal(t, token.Mount, publisher.token.Mount)
	require.Equal(t, fsmeta.MountKeyID(1), publisher.token.MountKeyID)
	require.Equal(t, token.RootInode, publisher.token.RootInode)
	require.Equal(t, token.ReadVersion, publisher.token.ReadVersion)

	_, err = cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   "b",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.NoError(t, err)

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
	require.Equal(t, token.Mount, publisher.retired.Mount)
	require.Equal(t, fsmeta.MountKeyID(1), publisher.retired.MountKeyID)
	require.Equal(t, token.RootInode, publisher.retired.RootInode)
	require.Equal(t, token.ReadVersion, publisher.retired.ReadVersion)
}

func TestFSMetadataStageCommitArtifactPublishOnRealCluster(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	executor := openRealClusterExecutor(t, ctx)
	cli, cleanup := openFSMetadataClient(t, ctx, executor)
	defer cleanup()

	mount := fsmeta.MountID("vol")
	staging, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: fsmeta.RootInode,
		Name:   ".staging",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)
	stagingInode := staging.Inode.Inode
	run, err := cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: stagingInode,
		Name:   "run-tmp",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeDirectory},
	})
	require.NoError(t, err)
	runInode := run.Inode.Inode
	_, err = cli.Create(ctx, fsmeta.CreateRequest{
		Mount:  mount,
		Parent: runInode,
		Name:   "manifest.json",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Size: 128},
	})
	require.NoError(t, err)

	_, err = cli.Lookup(ctx, fsmeta.LookupRequest{Mount: mount, Parent: fsmeta.RootInode, Name: "run-001"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	staged, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{Mount: mount, Parent: runInode, Limit: 8})
	require.NoError(t, err)
	require.Equal(t, []string{"manifest.json"}, dentryNames(staged))

	require.NoError(t, cli.Rename(ctx, fsmeta.RenameRequest{
		Mount:      mount,
		FromParent: stagingInode,
		FromName:   "run-tmp",
		ToParent:   fsmeta.RootInode,
		ToName:     "run-001",
	}))

	_, err = cli.Lookup(ctx, fsmeta.LookupRequest{Mount: mount, Parent: stagingInode, Name: "run-tmp"})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
	published, err := cli.Lookup(ctx, fsmeta.LookupRequest{Mount: mount, Parent: fsmeta.RootInode, Name: "run-001"})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "run-001",
		Inode:  runInode,
		Type:   fsmeta.InodeTypeDirectory,
	}, published)
	children, err := cli.ReadDirPlus(ctx, fsmeta.ReadDirRequest{Mount: mount, Parent: runInode, Limit: 8})
	require.NoError(t, err)
	require.Equal(t, []string{"manifest.json"}, dentryNames(children))
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
