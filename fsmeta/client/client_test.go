package client

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetaserver "github.com/feichai0017/NoKV/fsmeta/server"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeExecutor struct {
	err error
}

func (e *fakeExecutor) Create(context.Context, fsmeta.CreateRequest, fsmeta.InodeRecord) error {
	return e.err
}

func (e *fakeExecutor) Lookup(context.Context, fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	if e.err != nil {
		return fsmeta.DentryRecord{}, e.err
	}
	return fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "checkpoint", Inode: 42, Type: fsmeta.InodeTypeFile}, nil
}

func (e *fakeExecutor) ReadDir(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	if e.err != nil {
		return nil, e.err
	}
	return []fsmeta.DentryRecord{{Parent: fsmeta.RootInode, Name: "checkpoint", Inode: 42, Type: fsmeta.InodeTypeFile}}, nil
}

func (e *fakeExecutor) ReadDirPlus(context.Context, fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	if e.err != nil {
		return nil, e.err
	}
	return []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "checkpoint", Inode: 42, Type: fsmeta.InodeTypeFile},
		Inode:  fsmeta.InodeRecord{Inode: 42, Type: fsmeta.InodeTypeFile, Size: 4096, Mode: 0o644, LinkCount: 1},
	}}, nil
}

func (e *fakeExecutor) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	if e.err != nil {
		return fsmeta.SnapshotSubtreeToken{}, e.err
	}
	return fsmeta.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: 5678}, nil
}

func (e *fakeExecutor) GetQuotaUsage(context.Context, fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	if e.err != nil {
		return fsmeta.UsageRecord{}, e.err
	}
	return fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, nil
}

func (e *fakeExecutor) RenameSubtree(context.Context, fsmeta.RenameSubtreeRequest) error {
	return e.err
}

func (e *fakeExecutor) Link(context.Context, fsmeta.LinkRequest) error {
	return e.err
}

func (e *fakeExecutor) Unlink(context.Context, fsmeta.UnlinkRequest) error {
	return e.err
}

func TestTypedClientRoundTrip(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	record, err := cli.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   fsmeta.InodeTypeFile,
	}, record)

	pairs, err := cli.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "checkpoint", Inode: 42, Type: fsmeta.InodeTypeFile},
		Inode:  fsmeta.InodeRecord{Inode: 42, Type: fsmeta.InodeTypeFile, Size: 4096, Mode: 0o644, LinkCount: 1},
	}}, pairs)
}

func TestTypedClientErrorTranslation(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrExists})
	defer cleanup()

	err := cli.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Inode:  42,
	}, fsmeta.InodeRecord{Type: fsmeta.InodeTypeFile})
	require.ErrorIs(t, err, fsmeta.ErrExists)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrNotFound})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)
}

func TestTypedClientPreservesUnknownStatus(t *testing.T) {
	errClient := New(fsmetapb.NewFSMetadataClient(&failingConn{}))
	_, err := errClient.Lookup(context.Background(), fsmeta.LookupRequest{})
	require.Error(t, err)
	require.False(t, errors.Is(err, fsmeta.ErrNotFound))
	require.Equal(t, codes.Internal, status.Code(err))
}

func TestTypedClientWatchSubtree(t *testing.T) {
	watcher := &fakeWatcher{sub: newFakeWatchSub(1)}
	cli, cleanup := openBufconnClient(t, &fakeExecutor{}, fsmetaserver.WithWatcher(watcher))
	defer cleanup()

	stream, err := cli.WatchSubtree(context.Background(), fsmeta.WatchRequest{
		KeyPrefix:          []byte("fsm/"),
		BackPressureWindow: 4,
	})
	require.NoError(t, err)
	require.Equal(t, watcher.sub.ready, stream.ReadyCursor())

	require.Eventually(t, func() bool {
		return string(watcher.req.KeyPrefix) == "fsm/" && watcher.req.BackPressureWindow == 4
	}, time.Second, 10*time.Millisecond)
	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 8, Term: 1, Index: 2},
		CommitVersion: 90,
		Source:        fsmeta.WatchEventSourceResolveLock,
		Key:           []byte("fsm/checkpoint"),
	}
	watcher.sub.events <- evt
	got, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, evt, got)
	require.NoError(t, stream.Ack(got.Cursor))
	require.Eventually(t, func() bool {
		return len(watcher.sub.acks) == 1 && watcher.sub.acks[0] == evt.Cursor
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, stream.Close())
}

func TestTypedClientSnapshotSubtree(t *testing.T) {
	publisher := &fakeSnapshotPublisher{}
	cli, cleanup := openBufconnClient(t, &fakeExecutor{}, fsmetaserver.WithSnapshotPublisher(publisher))
	defer cleanup()

	token, err := cli.SnapshotSubtree(context.Background(), fsmeta.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", RootInode: 42, ReadVersion: 5678}, token)
	require.NoError(t, cli.RetireSnapshotSubtree(context.Background(), token))
	require.Equal(t, token, publisher.retired)
}

func TestTypedClientGetQuotaUsage(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	usage, err := cli.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol", Scope: 7})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, usage)
}

type fakeSnapshotPublisher struct {
	retired fsmeta.SnapshotSubtreeToken
}

func (p *fakeSnapshotPublisher) PublishSnapshotSubtree(context.Context, fsmeta.SnapshotSubtreeToken) error {
	return nil
}

func (p *fakeSnapshotPublisher) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	p.retired = token
	return nil
}

type fakeWatcher struct {
	req fsmeta.WatchRequest
	sub *fakeWatchSub
}

func (w *fakeWatcher) Subscribe(_ context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	w.req = req
	return w.sub, nil
}

type fakeWatchSub struct {
	events chan fsmeta.WatchEvent
	acks   []fsmeta.WatchCursor
	ready  fsmeta.WatchCursor
}

func newFakeWatchSub(buffer int) *fakeWatchSub {
	return &fakeWatchSub{
		events: make(chan fsmeta.WatchEvent, buffer),
		ready:  fsmeta.WatchCursor{RegionID: 8, Term: 1, Index: 1},
	}
}

func (s *fakeWatchSub) Events() <-chan fsmeta.WatchEvent {
	return s.events
}

func (s *fakeWatchSub) ReadyCursor() fsmeta.WatchCursor {
	return s.ready
}

func (s *fakeWatchSub) Ack(cursor fsmeta.WatchCursor) {
	s.acks = append(s.acks, cursor)
}

func (s *fakeWatchSub) Close() {
	close(s.events)
}

func (s *fakeWatchSub) Err() error {
	return nil
}

func openBufconnClient(t *testing.T, executor fsmetaserver.Executor, opts ...fsmetaserver.Option) (*GRPCClient, func()) {
	t.Helper()
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	fsmetaserver.Register(grpcServer, executor, opts...)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	ctx := context.Background()
	cli, err := NewGRPCClient(ctx, "passthrough:///fsmeta-bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	return cli, func() {
		_ = cli.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}

type failingConn struct{}

func (f *failingConn) Invoke(context.Context, string, any, any, ...grpc.CallOption) error {
	return status.Error(codes.Internal, "boom")
}

func (f *failingConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Error(codes.Internal, "boom")
}
