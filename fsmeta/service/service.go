package service

import (
	"context"
	"errors"

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
	Rename(ctx context.Context, req fsmeta.RenameRequest) error
	Unlink(ctx context.Context, req fsmeta.UnlinkRequest) error
}

// Service exposes NoKV-native filesystem metadata operations over gRPC.
// It is intentionally a thin transport layer; all transaction semantics stay in
// the Executor implementation.
type Service struct {
	fsmetapb.UnimplementedFSMetadataServer

	executor Executor
}

// NewService constructs an FSMetadata service around executor.
func NewService(executor Executor) *Service {
	return &Service{executor: executor}
}

// Register registers one FSMetadata service.
func Register(reg grpc.ServiceRegistrar, executor Executor) {
	fsmetapb.RegisterFSMetadataServer(reg, NewService(executor))
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

func (s *Service) Rename(ctx context.Context, req *fsmetapb.RenameRequest) (*fsmetapb.RenameResponse, error) {
	if err := s.requireExecutor(); err != nil {
		return nil, err
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "fsmeta rename request is required")
	}
	if err := s.executor.Rename(ctx, renameRequestFromProto(req)); err != nil {
		return nil, rpcError(err)
	}
	return &fsmetapb.RenameResponse{}, nil
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

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	case errors.Is(err, fsmeta.ErrExists):
		return status.Error(codes.AlreadyExists, err.Error())
	case errors.Is(err, fsmeta.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
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
