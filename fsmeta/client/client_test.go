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

func (e *fakeExecutor) Create(_ context.Context, req fsmeta.CreateRequest) (fsmeta.CreateResult, error) {
	if e.err != nil {
		return fsmeta.CreateResult{}, e.err
	}
	inode := req.Attrs.InodeRecord(42)
	return fsmeta.CreateResult{
		Dentry: fsmeta.DentryRecord{Parent: req.Parent, Name: req.Name, Inode: inode.Inode, Type: inode.Type},
		Inode:  inode,
	}, nil
}

func (e *fakeExecutor) UpdateInode(_ context.Context, req fsmeta.UpdateInodeRequest) (fsmeta.InodeRecord, error) {
	if e.err != nil {
		return fsmeta.InodeRecord{}, e.err
	}
	return fsmeta.InodeRecord{
		Inode:         req.Inode,
		Type:          fsmeta.InodeTypeFile,
		Size:          req.Size,
		Mode:          req.Mode,
		LinkCount:     1,
		UpdatedUnixNs: req.UpdatedUnixNs,
		OpaqueAttrs:   append([]byte(nil), req.OpaqueAttrs...),
	}, nil
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
		Inode: fsmeta.InodeRecord{
			Inode:       42,
			Type:        fsmeta.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			LinkCount:   1,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	}}, nil
}

func (e *fakeExecutor) GetReadVersion(context.Context, fsmeta.ReadVersionRequest) (uint64, error) {
	if e.err != nil {
		return 0, e.err
	}
	return 5678, nil
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

func (e *fakeExecutor) Rename(context.Context, fsmeta.RenameRequest) error {
	return e.err
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

func (e *fakeExecutor) OpenWriteSession(_ context.Context, req fsmeta.OpenWriteSessionRequest) (fsmeta.SessionRecord, error) {
	if e.err != nil {
		return fsmeta.SessionRecord{}, e.err
	}
	return fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) HeartbeatWriteSession(_ context.Context, req fsmeta.HeartbeatWriteSessionRequest) (fsmeta.SessionRecord, error) {
	if e.err != nil {
		return fsmeta.SessionRecord{}, e.err
	}
	return fsmeta.SessionRecord{Session: req.Session, Inode: req.Inode, ExpiresUnixNs: int64(req.TTL)}, nil
}

func (e *fakeExecutor) CloseWriteSession(context.Context, fsmeta.CloseWriteSessionRequest) error {
	return e.err
}

func (e *fakeExecutor) ExpireWriteSessions(context.Context, fsmeta.ExpireWriteSessionsRequest) (fsmeta.ExpireWriteSessionsResult, error) {
	if e.err != nil {
		return fsmeta.ExpireWriteSessionsResult{}, e.err
	}
	return fsmeta.ExpireWriteSessionsResult{Expired: 2}, nil
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

	records, err := cli.ReadDir(context.Background(), fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     fsmeta.RootInode,
		StartAfter: "batch-0001",
		Limit:      16,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryRecord{{
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   fsmeta.InodeTypeFile,
	}}, records)

	pairs, err := cli.ReadDirPlus(context.Background(), fsmeta.ReadDirRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Limit:  8,
	})
	require.NoError(t, err)
	require.Equal(t, []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{Parent: fsmeta.RootInode, Name: "checkpoint", Inode: 42, Type: fsmeta.InodeTypeFile},
		Inode: fsmeta.InodeRecord{
			Inode:       42,
			Type:        fsmeta.InodeTypeFile,
			Size:        4096,
			Mode:        0o644,
			LinkCount:   1,
			OpaqueAttrs: []byte(`{"body_ref":"cas://checkpoint"}`),
		},
	}}, pairs)
}

func TestTypedClientMutationRPCs(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	created, err := cli.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "created",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile, Mode: 0o644},
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeID(42), created.Inode.Inode)
	require.Equal(t, "created", created.Dentry.Name)

	require.NoError(t, cli.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "old",
		ToParent:   2,
		ToName:     "new",
	}))
	require.NoError(t, cli.Link(context.Background(), fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "file",
		ToParent:   2,
		ToName:     "alias",
	}))
	require.NoError(t, cli.Unlink(context.Background(), fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}))

	updated, err := cli.UpdateInode(context.Background(), fsmeta.UpdateInodeRequest{
		Mount:            "vol",
		Parent:           2,
		Inode:            42,
		Name:             "alias",
		SetSize:          true,
		Size:             8192,
		SetMode:          true,
		Mode:             0o600,
		SetUpdatedUnixNs: true,
		UpdatedUnixNs:    99,
		SetOpaqueAttrs:   true,
		OpaqueAttrs:      []byte("body=cas://2"),
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.InodeRecord{
		Inode:         42,
		Type:          fsmeta.InodeTypeFile,
		Size:          8192,
		Mode:          0o600,
		LinkCount:     1,
		UpdatedUnixNs: 99,
		OpaqueAttrs:   []byte("body=cas://2"),
	}, updated)

	session, err := cli.OpenWriteSession(context.Background(), fsmeta.OpenWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TTL:     time.Microsecond,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SessionRecord{Session: "writer-1", Inode: 42, ExpiresUnixNs: 1000}, session)

	session, err = cli.HeartbeatWriteSession(context.Background(), fsmeta.HeartbeatWriteSessionRequest{
		Mount:   "vol",
		Inode:   42,
		Session: "writer-1",
		TTL:     2 * time.Microsecond,
	})
	require.NoError(t, err)
	require.Equal(t, int64(2000), session.ExpiresUnixNs)
	require.NoError(t, cli.CloseWriteSession(context.Background(), fsmeta.CloseWriteSessionRequest{Mount: "vol", Session: "writer-1"}))
	expired, err := cli.ExpireWriteSessions(context.Background(), fsmeta.ExpireWriteSessionsRequest{Mount: "vol", Limit: 64})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ExpireWriteSessionsResult{Expired: 2}, expired)
}

func TestTypedClientErrorTranslation(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrExists})
	defer cleanup()

	_, err := cli.Create(context.Background(), fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Attrs:  fsmeta.CreateAttrs{Type: fsmeta.InodeTypeFile},
	})
	require.ErrorIs(t, err, fsmeta.ErrExists)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrNotFound})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), fsmeta.LookupRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "missing",
	})
	require.ErrorIs(t, err, fsmeta.ErrNotFound)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrMountNotRegistered})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "missing", Parent: fsmeta.RootInode, Name: "x"})
	require.ErrorIs(t, err, fsmeta.ErrMountNotRegistered)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrMountRetired})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "retired", Parent: fsmeta.RootInode, Name: "x"})
	require.ErrorIs(t, err, fsmeta.ErrMountRetired)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrCrossAuthorityRename})
	defer cleanup()
	err = cli.Rename(context.Background(), fsmeta.RenameRequest{Mount: "vol", FromParent: 1, FromName: "a", ToParent: 2, ToName: "b"})
	require.ErrorIs(t, err, fsmeta.ErrCrossAuthorityRename)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrInvalidRequest})
	defer cleanup()
	err = cli.RenameSubtree(context.Background(), fsmeta.RenameSubtreeRequest{Mount: "vol", FromParent: 1, FromName: "a", ToParent: 1, ToName: "a"})
	require.ErrorIs(t, err, fsmeta.ErrInvalidRequest)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrInvalidName})
	defer cleanup()
	_, err = cli.Lookup(context.Background(), fsmeta.LookupRequest{Mount: "vol", Parent: fsmeta.RootInode, Name: ""})
	require.ErrorIs(t, err, fsmeta.ErrInvalidName)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrQuotaExceeded})
	defer cleanup()
	_, err = cli.GetQuotaUsage(context.Background(), fsmeta.QuotaUsageRequest{Mount: "vol"})
	require.ErrorIs(t, err, fsmeta.ErrQuotaExceeded)

	cli, cleanup = openBufconnClient(t, &fakeExecutor{err: fsmeta.ErrWatchOverflow})
	defer cleanup()
	_, err = cli.ReadDir(context.Background(), fsmeta.ReadDirRequest{Mount: "vol", Parent: fsmeta.RootInode})
	require.ErrorIs(t, err, fsmeta.ErrWatchOverflow)
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
	watchStream, ok := stream.(*WatchStream)
	require.True(t, ok)
	require.NoError(t, watchStream.AckEvent(got))
	require.NoError(t, stream.Close())
}

func TestWatchSessionHelpers(t *testing.T) {
	sub := &stubWatchSubscription{
		ready: fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		events: []fsmeta.WatchEvent{{
			Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 4},
			CommitVersion: 99,
			Source:        fsmeta.WatchEventSourceCommit,
			Key:           []byte("fsm/key"),
		}},
	}
	session := NewWatchSession(sub)

	require.Equal(t, sub.ready, session.ReadyCursor())
	want := sub.events[0]
	evt, err := session.Recv()
	require.NoError(t, err)
	require.Equal(t, want, evt)
	require.NoError(t, session.Ack(evt))
	require.Equal(t, []fsmeta.WatchCursor{evt.Cursor}, sub.acks)
	require.NoError(t, session.Close())
	require.True(t, sub.closed)

	var nilSession *WatchSession
	_, err = nilSession.Recv()
	require.Error(t, err)
	require.Error(t, nilSession.Ack(evt))
	require.Equal(t, fsmeta.WatchCursor{}, nilSession.ReadyCursor())
	require.NoError(t, nilSession.Close())
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

func TestTypedClientGetReadVersion(t *testing.T) {
	cli, cleanup := openBufconnClient(t, &fakeExecutor{})
	defer cleanup()

	version, err := cli.GetReadVersion(context.Background(), fsmeta.ReadVersionRequest{Mount: "vol"})
	require.NoError(t, err)
	require.Equal(t, uint64(5678), version)
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

type stubWatchSubscription struct {
	events []fsmeta.WatchEvent
	acks   []fsmeta.WatchCursor
	ready  fsmeta.WatchCursor
	closed bool
}

func (s *stubWatchSubscription) Recv() (fsmeta.WatchEvent, error) {
	if len(s.events) == 0 {
		return fsmeta.WatchEvent{}, errors.New("empty")
	}
	evt := s.events[0]
	s.events = s.events[1:]
	return evt, nil
}

func (s *stubWatchSubscription) ReadyCursor() fsmeta.WatchCursor {
	return s.ready
}

func (s *stubWatchSubscription) Ack(cursor fsmeta.WatchCursor) error {
	s.acks = append(s.acks, cursor)
	return nil
}

func (s *stubWatchSubscription) Close() error {
	s.closed = true
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
