package server

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

type fakeExecutor struct {
	createReq   fsmeta.CreateRequest
	createInode fsmeta.InodeRecord
	readDirReq  fsmeta.ReadDirRequest
	snapshotReq fsmeta.SnapshotSubtreeRequest
	quotaReq    fsmeta.QuotaUsageRequest
	renameReq   fsmeta.RenameSubtreeRequest
	linkReq     fsmeta.LinkRequest
	unlinkReq   fsmeta.UnlinkRequest
	err         error
}

func (e *fakeExecutor) Create(_ context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error {
	e.createReq = req
	e.createInode = inode
	return e.err
}

func (e *fakeExecutor) Lookup(context.Context, fsmeta.LookupRequest) (fsmeta.DentryRecord, error) {
	if e.err != nil {
		return fsmeta.DentryRecord{}, e.err
	}
	return fsmeta.DentryRecord{
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Inode:  42,
		Type:   fsmeta.InodeTypeFile,
	}, nil
}

func (e *fakeExecutor) ReadDir(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error) {
	e.readDirReq = req
	if e.err != nil {
		return nil, e.err
	}
	return []fsmeta.DentryRecord{{
		Parent: req.Parent,
		Name:   "checkpoint",
		Inode:  42,
		Type:   fsmeta.InodeTypeFile,
	}}, nil
}

func (e *fakeExecutor) ReadDirPlus(_ context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error) {
	e.readDirReq = req
	if e.err != nil {
		return nil, e.err
	}
	return []fsmeta.DentryAttrPair{{
		Dentry: fsmeta.DentryRecord{
			Parent: req.Parent,
			Name:   "checkpoint",
			Inode:  42,
			Type:   fsmeta.InodeTypeFile,
		},
		Inode: fsmeta.InodeRecord{
			Inode:     42,
			Type:      fsmeta.InodeTypeFile,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	}}, nil
}

func (e *fakeExecutor) SnapshotSubtree(_ context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error) {
	e.snapshotReq = req
	if e.err != nil {
		return fsmeta.SnapshotSubtreeToken{}, e.err
	}
	return fsmeta.SnapshotSubtreeToken{Mount: req.Mount, RootInode: req.RootInode, ReadVersion: 1234}, nil
}

func (e *fakeExecutor) GetQuotaUsage(_ context.Context, req fsmeta.QuotaUsageRequest) (fsmeta.UsageRecord, error) {
	e.quotaReq = req
	if e.err != nil {
		return fsmeta.UsageRecord{}, e.err
	}
	return fsmeta.UsageRecord{Bytes: 4096, Inodes: 2}, nil
}

func (e *fakeExecutor) RenameSubtree(_ context.Context, req fsmeta.RenameSubtreeRequest) error {
	e.renameReq = req
	return e.err
}

func (e *fakeExecutor) Link(_ context.Context, req fsmeta.LinkRequest) error {
	e.linkReq = req
	return e.err
}

func (e *fakeExecutor) Unlink(_ context.Context, req fsmeta.UnlinkRequest) error {
	e.unlinkReq = req
	return e.err
}

func TestGRPCServiceCreateAndReadDirPlus(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	_, err := client.Create(context.Background(), &fsmetapb.CreateRequest{
		Mount:  "vol",
		Parent: uint64(fsmeta.RootInode),
		Name:   "checkpoint",
		Inode: &fsmetapb.InodeRecord{
			Inode:     42,
			Type:      fsmetapb.InodeType_INODE_TYPE_FILE,
			Size:      4096,
			Mode:      0o644,
			LinkCount: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.CreateRequest{
		Mount:  "vol",
		Parent: fsmeta.RootInode,
		Name:   "checkpoint",
		Inode:  42,
	}, executor.createReq)
	require.Equal(t, fsmeta.InodeRecord{
		Inode:     42,
		Type:      fsmeta.InodeTypeFile,
		Size:      4096,
		Mode:      0o644,
		LinkCount: 1,
	}, executor.createInode)

	resp, err := client.ReadDirPlus(context.Background(), &fsmetapb.ReadDirRequest{
		Mount:      "vol",
		Parent:     uint64(fsmeta.RootInode),
		StartAfter: "batch-0001",
		Limit:      64,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     fsmeta.RootInode,
		StartAfter: "batch-0001",
		Limit:      64,
	}, executor.readDirReq)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, "checkpoint", resp.GetEntries()[0].GetDentry().GetName())
	require.Equal(t, uint64(4096), resp.GetEntries()[0].GetInode().GetSize())
}

func TestGRPCServiceReadDirAndMutationRPCs(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	readDirResp, err := client.ReadDir(context.Background(), &fsmetapb.ReadDirRequest{
		Mount:      "vol",
		Parent:     uint64(fsmeta.RootInode),
		StartAfter: "a",
		Limit:      32,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.ReadDirRequest{
		Mount:      "vol",
		Parent:     fsmeta.RootInode,
		StartAfter: "a",
		Limit:      32,
	}, executor.readDirReq)
	require.Len(t, readDirResp.GetEntries(), 1)
	require.Equal(t, "checkpoint", readDirResp.GetEntries()[0].GetName())

	_, err = client.RenameSubtree(context.Background(), &fsmetapb.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "old",
		ToParent:   2,
		ToName:     "new",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.RenameSubtreeRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "old",
		ToParent:   2,
		ToName:     "new",
	}, executor.renameReq)

	_, err = client.Link(context.Background(), &fsmetapb.LinkRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "file",
		ToParent:   2,
		ToName:     "alias",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.LinkRequest{
		Mount:      "vol",
		FromParent: 1,
		FromName:   "file",
		ToParent:   2,
		ToName:     "alias",
	}, executor.linkReq)

	_, err = client.Unlink(context.Background(), &fsmetapb.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.UnlinkRequest{
		Mount:  "vol",
		Parent: 2,
		Name:   "alias",
	}, executor.unlinkReq)
}

func TestGRPCServiceErrorMapping(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{name: "exists", err: fsmeta.ErrExists, code: codes.AlreadyExists},
		{name: "not found", err: fsmeta.ErrNotFound, code: codes.NotFound},
		{name: "invalid", err: fsmeta.ErrInvalidName, code: codes.InvalidArgument},
		{name: "internal", err: errors.New("boom"), code: codes.Internal},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, cleanup := openBufconnClient(t, &fakeExecutor{err: tt.err})
			defer cleanup()
			_, err := client.Lookup(context.Background(), &fsmetapb.LookupRequest{
				Mount:  "vol",
				Parent: uint64(fsmeta.RootInode),
				Name:   "checkpoint",
			})
			require.Error(t, err)
			require.Equal(t, tt.code, status.Code(err))
		})
	}
}

func TestGRPCServiceWatchSubtree(t *testing.T) {
	watcher := &fakeWatcher{sub: newFakeWatchSub(1)}
	client, cleanup := openBufconnClient(t, &fakeExecutor{}, WithWatcher(watcher))
	defer cleanup()

	stream, err := client.WatchSubtree(context.Background())
	require.NoError(t, err)
	require.NoError(t, stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Subscribe{Subscribe: &fsmetapb.WatchSubtreeRequest{
			KeyPrefix:          []byte("fsm/"),
			BackPressureWindow: 8,
		}},
	}))
	require.Eventually(t, func() bool {
		return string(watcher.req.KeyPrefix) == "fsm/" && watcher.req.BackPressureWindow == 8
	}, time.Second, 10*time.Millisecond)
	ready, err := stream.Recv()
	require.NoError(t, err)
	require.NotNil(t, ready.GetReady())
	require.Equal(t, uint64(3), ready.GetReady().GetCursor().GetIndex())

	evt := fsmeta.WatchEvent{
		Cursor:        fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
		CommitVersion: 44,
		Source:        fsmeta.WatchEventSourceCommit,
		Key:           []byte("fsm/a"),
	}
	watcher.sub.events <- evt
	resp, err := stream.Recv()
	require.NoError(t, err)
	require.Equal(t, uint64(44), resp.GetEvent().GetCommitVersion())
	require.Equal(t, []byte("fsm/a"), resp.GetEvent().GetKey())

	require.NoError(t, stream.Send(&fsmetapb.WatchAckOrSubscribe{
		Body: &fsmetapb.WatchAckOrSubscribe_Ack{Ack: &fsmetapb.WatchAck{Cursor: resp.GetEvent().GetRaftCursor()}},
	}))
	require.Eventually(t, func() bool {
		return len(watcher.sub.acks) == 1 && watcher.sub.acks[0] == evt.Cursor
	}, time.Second, 10*time.Millisecond)
	require.NoError(t, stream.CloseSend())
}

func TestGRPCServiceSnapshotSubtreePublishesToken(t *testing.T) {
	executor := &fakeExecutor{}
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	resp, err := client.SnapshotSubtree(context.Background(), &fsmetapb.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeRequest{Mount: "vol", RootInode: 42}, executor.snapshotReq)
	require.Equal(t, uint64(1234), resp.GetReadVersion())
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", RootInode: 42, ReadVersion: 1234}, publisher.token)
}

func TestGRPCServiceSnapshotSubtreeRetiresTokenAfterPublishFailure(t *testing.T) {
	executor := &fakeExecutor{}
	publisher := &fakeSnapshotPublisher{err: errors.New("publish failed")}
	client, cleanup := openBufconnClient(t, executor, WithSnapshotPublisher(publisher))
	defer cleanup()

	_, err := client.SnapshotSubtree(context.Background(), &fsmetapb.SnapshotSubtreeRequest{
		Mount:     "vol",
		RootInode: 42,
	})
	require.Error(t, err)
	want := fsmeta.SnapshotSubtreeToken{Mount: "vol", RootInode: 42, ReadVersion: 1234}
	require.Equal(t, want, publisher.token)
	require.Equal(t, want, publisher.retired)
}

func TestGRPCServiceRetireSnapshotSubtree(t *testing.T) {
	publisher := &fakeSnapshotPublisher{}
	client, cleanup := openBufconnClient(t, &fakeExecutor{}, WithSnapshotPublisher(publisher))
	defer cleanup()

	_, err := client.RetireSnapshotSubtree(context.Background(), &fsmetapb.RetireSnapshotSubtreeRequest{
		Mount:       "vol",
		RootInode:   42,
		ReadVersion: 1234,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.SnapshotSubtreeToken{Mount: "vol", RootInode: 42, ReadVersion: 1234}, publisher.retired)
}

func TestGRPCServiceGetQuotaUsage(t *testing.T) {
	executor := &fakeExecutor{}
	client, cleanup := openBufconnClient(t, executor)
	defer cleanup()

	resp, err := client.GetQuotaUsage(context.Background(), &fsmetapb.QuotaUsageRequest{
		Mount: "vol",
		Scope: 7,
	})
	require.NoError(t, err)
	require.Equal(t, fsmeta.QuotaUsageRequest{Mount: "vol", Scope: 7}, executor.quotaReq)
	require.Equal(t, uint64(4096), resp.GetBytes())
	require.Equal(t, uint64(2), resp.GetInodes())
}

type fakeSnapshotPublisher struct {
	token       fsmeta.SnapshotSubtreeToken
	retired     fsmeta.SnapshotSubtreeToken
	err         error
	retireError error
}

func (p *fakeSnapshotPublisher) PublishSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	p.token = token
	return p.err
}

func (p *fakeSnapshotPublisher) RetireSnapshotSubtree(_ context.Context, token fsmeta.SnapshotSubtreeToken) error {
	p.retired = token
	return p.retireError
}

type fakeWatcher struct {
	req fsmeta.WatchRequest
	sub *fakeWatchSub
	err error
}

func (w *fakeWatcher) Subscribe(_ context.Context, req fsmeta.WatchRequest) (fsmeta.WatchSubscription, error) {
	w.req = req
	if w.err != nil {
		return nil, w.err
	}
	return w.sub, nil
}

type fakeWatchSub struct {
	events chan fsmeta.WatchEvent
	acks   []fsmeta.WatchCursor
	err    error
	ready  fsmeta.WatchCursor
}

func newFakeWatchSub(buffer int) *fakeWatchSub {
	return &fakeWatchSub{
		events: make(chan fsmeta.WatchEvent, buffer),
		ready:  fsmeta.WatchCursor{RegionID: 1, Term: 2, Index: 3},
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
	return s.err
}

func openBufconnClient(t *testing.T, executor Executor, opts ...Option) (fsmetapb.FSMetadataClient, func()) {
	t.Helper()
	const bufSize = 1 << 20
	listener := bufconn.Listen(bufSize)
	grpcServer := grpc.NewServer()
	Register(grpcServer, executor, opts...)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	dialer := func(context.Context, string) (net.Conn, error) {
		return listener.Dial()
	}
	conn, err := grpc.NewClient("passthrough:///fsmeta-bufnet",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	require.NoError(t, err)
	return fsmetapb.NewFSMetadataClient(conn), func() {
		_ = conn.Close()
		grpcServer.Stop()
		_ = listener.Close()
	}
}
