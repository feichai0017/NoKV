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

func (e *fakeExecutor) Rename(context.Context, fsmeta.RenameRequest) error {
	return e.err
}

func (e *fakeExecutor) Unlink(context.Context, fsmeta.UnlinkRequest) error {
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
}

func newFakeWatchSub(buffer int) *fakeWatchSub {
	return &fakeWatchSub{events: make(chan fsmeta.WatchEvent, buffer)}
}

func (s *fakeWatchSub) Events() <-chan fsmeta.WatchEvent {
	return s.events
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
