package server

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/feichai0017/NoKV/fsmeta"
	fsmetapb "github.com/feichai0017/NoKV/pb/fsmeta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Executor is the fsmeta operation surface exported by the gRPC service.
// fsmeta/exec.Executor satisfies this interface.
type Executor interface {
	Create(ctx context.Context, req fsmeta.CreateRequest, inode fsmeta.InodeRecord) error
	Lookup(ctx context.Context, req fsmeta.LookupRequest) (fsmeta.DentryRecord, error)
	ReadDir(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryRecord, error)
	ReadDirPlus(ctx context.Context, req fsmeta.ReadDirRequest) ([]fsmeta.DentryAttrPair, error)
	SnapshotSubtree(ctx context.Context, req fsmeta.SnapshotSubtreeRequest) (fsmeta.SnapshotSubtreeToken, error)
	RenameSubtree(ctx context.Context, req fsmeta.RenameSubtreeRequest) error
	Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error
}

// Service exposes NoKV-native filesystem metadata operations over gRPC.
// It is intentionally a thin transport layer; all transaction semantics stay in
// the Executor implementation.
type Service struct {
	fsmetapb.UnimplementedFSMetadataServer

	executor Executor
	watcher  fsmeta.Watcher
	snapshot fsmeta.SnapshotPublisher
}

// Option configures an FSMetadata service.
type Option func(*Service)

// WithWatcher enables WatchSubtree streams for the service.
func WithWatcher(watcher fsmeta.Watcher) Option {
	return func(s *Service) {
		s.watcher = watcher
	}
}

// WithSnapshotPublisher records SnapshotSubtree epochs in rooted truth.
func WithSnapshotPublisher(publisher fsmeta.SnapshotPublisher) Option {
	return func(s *Service) {
		s.snapshot = publisher
	}
}

// NewService constructs an FSMetadata service around executor.
func NewService(executor Executor, opts ...Option) *Service {
	svc := &Service{executor: executor}
	for _, opt := range opts {
		if opt != nil {
			opt(svc)
		}
	}
	return svc
}

// Register registers one FSMetadata service.
func Register(reg grpc.ServiceRegistrar, executor Executor, opts ...Option) {
	fsmetapb.RegisterFSMetadataServer(reg, NewService(executor, opts...))
}

func (s *Service) Create(ctx context.Context, req *fsmetapb.CreateRequest) (*fsmetapb.CreateResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta create request is required")
	}
	createReq, inode := createRequestFromProto(req)
	if err := s.executor.Create(ctx, createReq, inode); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.CreateResponse{}, nil
}

func (s *Service) Lookup(ctx context.Context, req *fsmetapb.LookupRequest) (*fsmetapb.LookupResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta lookup request is required")
	}
	record, err := s.executor.Lookup(ctx, lookupRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.LookupResponse{Dentry: dentryToProto(record)}, nil
}

func (s *Service) ReadDir(ctx context.Context, req *fsmetapb.ReadDirRequest) (*fsmetapb.ReadDirResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta readdir request is required")
	}
	entries, err := s.executor.ReadDir(ctx, readDirRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &fsmetapb.ReadDirResponse{Entries: make([]*fsmetapb.DentryRecord, 0, len(entries))}
	for _, entry := range entries {
		resp.Entries = append(resp.Entries, dentryToProto(entry))
	}
	return resp, nil
}

func (s *Service) ReadDirPlus(ctx context.Context, req *fsmetapb.ReadDirRequest) (*fsmetapb.ReadDirPlusResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta readdirplus request is required")
	}
	entries, err := s.executor.ReadDirPlus(ctx, readDirRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	resp := &fsmetapb.ReadDirPlusResponse{Entries: make([]*fsmetapb.DentryAttrPair, 0, len(entries))}
	for _, entry := range entries {
		resp.Entries = append(resp.Entries, pairToProto(entry))
	}
	return resp, nil
}

func (s *Service) WatchSubtree(stream fsmetapb.FSMetadata_WatchSubtreeServer) error {
	if err := s.requireWatcher(); err != nil {
		return err
	}
	first, err := stream.Recv()
	if err != nil {
		return rpcStreamError(err)
	}
	subscribe := first.GetSubscribe()
	if subscribe == nil {
		return status.Error(codes.InvalidArgument, "fsmeta watch first message must subscribe")
	}
	watchReq := watchRequestFromProto(subscribe)
	sub, err := s.watcher.Subscribe(stream.Context(), watchReq)
	if err != nil {
		return rpcError(err)
	}
	defer sub.Close()
	if err := stream.Send(&fsmetapb.WatchSubtreeResponse{
		Payload: &fsmetapb.WatchSubtreeResponse_Ready{
			Ready: &fsmetapb.WatchReady{Cursor: watchCursorToProto(sub.ReadyCursor())},
		},
	}); err != nil {
		return rpcStreamError(err)
	}

	recvErr := make(chan error, 1)
	go func() {
		for {
			msg, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}
			if ack := msg.GetAck(); ack != nil {
				sub.Ack(watchCursorFromProto(ack.GetCursor()))
				continue
			}
			recvErr <- status.Error(codes.InvalidArgument, "fsmeta watch stream only accepts ack after subscribe")
			return
		}
	}()

	for {
		select {
		case err := <-recvErr:
			return rpcStreamError(err)
		case <-stream.Context().Done():
			return rpcStreamError(stream.Context().Err())
		case evt, ok := <-sub.Events():
			if !ok {
				return rpcError(sub.Err())
			}
			if err := stream.Send(&fsmetapb.WatchSubtreeResponse{
				Payload: &fsmetapb.WatchSubtreeResponse_Event{Event: watchEventToProto(evt)},
			}); err != nil {
				return rpcStreamError(err)
			}
		}
	}
}

func (s *Service) SnapshotSubtree(ctx context.Context, req *fsmetapb.SnapshotSubtreeRequest) (*fsmetapb.SnapshotSubtreeResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta snapshot subtree request is required")
	}
	token, err := s.executor.SnapshotSubtree(ctx, snapshotSubtreeRequestFromProto(req))
	if err != nil {
		return nil, rpcError(err)
	}
	if s.snapshot != nil {
		if err := s.snapshot.PublishSnapshotSubtree(ctx, token); err != nil {
			if retireErr := s.snapshot.RetireSnapshotSubtree(ctx, token); retireErr != nil {
				return nil, rpcError(errors.Join(err, fmt.Errorf("retire snapshot epoch after publish failure: %w", retireErr)))
			}
			return nil, rpcError(err)
		}
	}
	return snapshotSubtreeResponseToProto(token), nil
}

func (s *Service) RetireSnapshotSubtree(ctx context.Context, req *fsmetapb.RetireSnapshotSubtreeRequest) (*fsmetapb.RetireSnapshotSubtreeResponse, error) {
	if s == nil || s.snapshot == nil {
		return nil, status.Error(codes.FailedPrecondition, "fsmeta snapshot publisher is not configured")
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta retire snapshot subtree request is required")
	}
	if err := s.snapshot.RetireSnapshotSubtree(ctx, retireSnapshotSubtreeRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.RetireSnapshotSubtreeResponse{}, nil
}

func (s *Service) RenameSubtree(ctx context.Context, req *fsmetapb.RenameSubtreeRequest) (*fsmetapb.RenameSubtreeResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta rename subtree request is required")
	}
	if err := s.executor.RenameSubtree(ctx, renameSubtreeRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.RenameSubtreeResponse{}, nil
}

func (s *Service) Unlink(ctx context.Context, req *fsmetapb.UnlinkRequest) (*fsmetapb.UnlinkResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta unlink request is required")
	}
	if err := s.executor.Unlink(ctx, unlinkRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.UnlinkResponse{}, nil
}

func (s *Service) requireExecutor() error {
	if s == nil || s.executor == nil {
		return status.Error(codes.FailedPrecondition, "fsmeta executor is not configured")
	}
	return nil
}

func (s *Service) requireWatcher() error {
	if s == nil || s.watcher == nil {
		return status.Error(codes.FailedPrecondition, "fsmeta watcher is not configured")
	}
	return nil
}

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, fsmeta.ErrWatchOverflow):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, fsmeta.ErrWatchCursorExpired):
		return status.Error(codes.OutOfRange, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, fsmeta.ErrExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, fsmeta.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, fsmeta.ErrMountNotRegistered), errors.Is(err, fsmeta.ErrMountRetired):
		return status.Error(codes.FailedPrecondition, err.Error())
	case errors.Is(err, fsmeta.ErrQuotaExceeded):
		return status.Error(codes.ResourceExhausted, err.Error())
	case errors.Is(err, fsmeta.ErrInvalidMountID),
		errors.Is(err, fsmeta.ErrInvalidInodeID),
		errors.Is(err, fsmeta.ErrInvalidName),
		errors.Is(err, fsmeta.ErrInvalidSession),
		errors.Is(err, fsmeta.ErrInvalidRequest),
		errors.Is(err, fsmeta.ErrInvalidKey),
		errors.Is(err, fsmeta.ErrInvalidKeyKind),
		errors.Is(err, fsmeta.ErrInvalidValue),
		errors.Is(err, fsmeta.ErrInvalidValueKind),
		errors.Is(err, fsmeta.ErrInvalidPageSize):
		return status.Error(codes.InvalidArgument, err.Error())
	default:
		return status.Error(codes.Internal, err.Error())
	}
}

func rpcStreamError(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	return rpcError(err)
}
