// Package server exposes one metadata-root backend as a gRPC service. It
// mirrors the layout used by raftstore/server and coordinator/server: the
// companion meta/root/client package dials this service.
package server

import (
	"context"
	"errors"
	"time"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
)

// Backend is the metadata-root authority surface exported over gRPC.
type Backend interface {
	Snapshot() (rootstate.Snapshot, error)
	Append(ctx context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error)
	FenceAllocator(ctx context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error)
}

type leaderBackend interface {
	IsLeader() bool
	LeaderID() uint64
}

type observedBackend interface {
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
}

type tailBackend interface {
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
}

type leaseBackend interface {
	ApplyTenure(ctx context.Context, cmd rootproto.TenureCommand) (rootstate.EunomiaState, error)
	ApplyHandover(ctx context.Context, cmd rootproto.HandoverCommand) (rootstate.EunomiaState, error)
}

// Service exposes one metadata-root backend through the MetadataRoot RPC API.
type Service struct {
	metapb.UnimplementedMetadataRootServer

	backend Backend
}

// NewService constructs one metadata-root RPC service around backend.
func NewService(backend Backend) *Service {
	return &Service{backend: backend}
}

// Register registers one metadata-root RPC service.
func Register(reg grpc.ServiceRegistrar, backend Backend) {
	metapb.RegisterMetadataRootServer(reg, NewService(backend))
}

func (s *Service) Snapshot(context.Context, *metapb.MetadataRootSnapshotRequest) (*metapb.MetadataRootSnapshotResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootSnapshotResponse{}, nil
	}
	snapshot, err := s.backend.Snapshot()
	if err != nil {
		return nil, rpcError(err)
	}
	return &metapb.MetadataRootSnapshotResponse{Checkpoint: metawire.RootSnapshotToProto(snapshot, 0)}, nil
}

func (s *Service) Append(ctx context.Context, req *metapb.MetadataRootAppendRequest) (*metapb.MetadataRootAppendResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootAppendResponse{}, nil
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	events := make([]rootevent.Event, 0, len(req.GetEvents()))
	for _, pbEvent := range req.GetEvents() {
		event := metawire.RootEventFromProto(pbEvent)
		if event.Kind == rootevent.KindUnknown {
			return nil, statusInvalidArgument("metadata root append requires known event kind")
		}
		events = append(events, event)
	}
	commit, err := s.backend.Append(ctx, events...)
	if err != nil {
		return nil, rpcError(err)
	}
	return &metapb.MetadataRootAppendResponse{
		Cursor: metawire.RootCursorToProto(commit.Cursor),
		State:  metawire.RootStateToProto(commit.State),
	}, nil
}

func (s *Service) FenceAllocator(ctx context.Context, req *metapb.MetadataRootFenceAllocatorRequest) (*metapb.MetadataRootFenceAllocatorResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootFenceAllocatorResponse{}, nil
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	kind, ok := metawire.RootAllocatorKindFromProto(req.GetKind())
	if !ok {
		return nil, statusInvalidArgument("metadata root allocator kind is required")
	}
	current, err := s.backend.FenceAllocator(ctx, kind, req.GetMinimum())
	if err != nil {
		return nil, rpcError(err)
	}
	return &metapb.MetadataRootFenceAllocatorResponse{Current: current}, nil
}

func (s *Service) Status(context.Context, *metapb.MetadataRootStatusRequest) (*metapb.MetadataRootStatusResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootStatusResponse{IsLeader: true}, nil
	}
	if leader, ok := s.backend.(leaderBackend); ok {
		return &metapb.MetadataRootStatusResponse{IsLeader: leader.IsLeader(), LeaderId: leader.LeaderID()}, nil
	}
	return &metapb.MetadataRootStatusResponse{IsLeader: true}, nil
}

func (s *Service) ApplyTenure(ctx context.Context, req *metapb.MetadataRootApplyTenureRequest) (*metapb.MetadataRootApplyTenureResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootApplyTenureResponse{}, nil
	}
	backend, err := s.coordinatorProtocolBackend()
	if err != nil {
		return nil, err
	}
	cmd := metawire.RootTenureCommandFromProto(req.GetCommand())
	protocolState, err := backend.ApplyTenure(ctx, cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrPrimacy) {
			return &metapb.MetadataRootApplyTenureResponse{
				State:  metawire.RootEunomiaStateToProto(protocolState),
				Status: metapb.RootTenureApplyStatus_ROOT_TENURE_APPLY_STATUS_HELD,
			}, nil
		}
		return nil, coordinatorLeaseApplyRPCError(cmd.Kind, err)
	}
	return &metapb.MetadataRootApplyTenureResponse{
		State:  metawire.RootEunomiaStateToProto(protocolState),
		Status: metapb.RootTenureApplyStatus_ROOT_TENURE_APPLY_STATUS_GRANTED,
	}, nil
}

func (s *Service) ApplyHandover(ctx context.Context, req *metapb.MetadataRootApplyHandoverRequest) (*metapb.MetadataRootApplyHandoverResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootApplyHandoverResponse{}, nil
	}
	backend, err := s.coordinatorProtocolBackend()
	if err != nil {
		return nil, err
	}
	cmd := metawire.RootHandoverCommandFromProto(req.GetCommand())
	protocolState, err := backend.ApplyHandover(ctx, cmd)
	if err != nil {
		return nil, coordinatorHandoverApplyRPCError(cmd.Kind, err)
	}
	return &metapb.MetadataRootApplyHandoverResponse{
		State: metawire.RootEunomiaStateToProto(protocolState),
	}, nil
}

func (s *Service) ObserveCommitted(context.Context, *metapb.MetadataRootObserveCommittedRequest) (*metapb.MetadataRootObserveCommittedResponse, error) {
	observed, err := s.observeCommitted()
	if err != nil {
		return nil, rpcError(err)
	}
	checkpoint, tail := metawire.RootObservedToProto(observed)
	return &metapb.MetadataRootObserveCommittedResponse{Checkpoint: checkpoint, Tail: tail}, nil
}

func (s *Service) ObserveTail(_ context.Context, req *metapb.MetadataRootObserveTailRequest) (*metapb.MetadataRootObserveTailResponse, error) {
	after := metawire.RootTailTokenFromProto(req.GetAfter())
	advance, err := s.observeTail(after)
	if err != nil {
		return nil, rpcError(err)
	}
	pbAfter, token, checkpoint, tail := metawire.RootTailAdvanceToObservedResponse(advance)
	return &metapb.MetadataRootObserveTailResponse{
		After:      pbAfter,
		Token:      token,
		Checkpoint: checkpoint,
		Tail:       tail,
	}, nil
}

func (s *Service) WaitTail(_ context.Context, req *metapb.MetadataRootWaitTailRequest) (*metapb.MetadataRootWaitTailResponse, error) {
	after := metawire.RootTailTokenFromProto(req.GetAfter())
	timeout := time.Duration(req.GetTimeoutMillis()) * time.Millisecond
	advance, err := s.waitTail(after, timeout)
	if err != nil {
		return nil, rpcError(err)
	}
	pbAfter, token, checkpoint, tail := metawire.RootTailAdvanceToObservedResponse(advance)
	return &metapb.MetadataRootWaitTailResponse{
		After:      pbAfter,
		Token:      token,
		Checkpoint: checkpoint,
		Tail:       tail,
	}, nil
}

func (s *Service) observeCommitted() (rootstorage.ObservedCommitted, error) {
	if s == nil || s.backend == nil {
		return rootstorage.ObservedCommitted{}, nil
	}
	if observer, ok := s.backend.(observedBackend); ok {
		return observer.ObserveCommitted()
	}
	snapshot, err := s.backend.Snapshot()
	if err != nil {
		return rootstorage.ObservedCommitted{}, err
	}
	return metawire.RootFallbackObservedFromSnapshot(snapshot), nil
}

func (s *Service) observeTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if s == nil || s.backend == nil {
		return rootstorage.TailAdvance{After: after, Token: after}, nil
	}
	if tail, ok := s.backend.(tailBackend); ok {
		return tail.ObserveTail(after)
	}
	observed, err := s.observeCommitted()
	if err != nil {
		return rootstorage.TailAdvance{}, err
	}
	token := rootstorage.TailToken{Cursor: observed.LastCursor()}
	return observed.Advance(after, token), nil
}

func (s *Service) waitTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error) {
	if s == nil || s.backend == nil {
		return rootstorage.TailAdvance{After: after, Token: after}, nil
	}
	if tail, ok := s.backend.(tailBackend); ok {
		return tail.WaitForTail(after, timeout)
	}
	return s.observeTail(after)
}

func (s *Service) requireLeader() error {
	if s == nil || s.backend == nil {
		return nil
	}
	leader, ok := s.backend.(leaderBackend)
	if !ok || leader.IsLeader() {
		return nil
	}
	leaderID := leader.LeaderID()
	return statusNotLeader(leaderID)
}

func (s *Service) coordinatorProtocolBackend() (leaseBackend, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	backend, ok := s.backend.(leaseBackend)
	if !ok {
		return nil, statusUnimplemented("metadata root coordinator protocol is not supported")
	}
	return backend, nil
}

func coordinatorLeaseApplyRPCError(kind rootproto.TenureAct, err error) error {
	switch kind {
	case rootproto.TenureActIssue:
		switch {
		case errors.Is(err, rootstate.ErrInvalidTenure):
			return statusInvalidArgument(err.Error())
		case errors.Is(err, rootstate.ErrInheritance):
			return rpcError(err)
		}
	case rootproto.TenureActRelease:
		switch {
		case errors.Is(err, rootstate.ErrPrimacy),
			errors.Is(err, rootstate.ErrInvalidTenure):
			return statusFailedPrecondition(err)
		}
	}
	return rpcError(err)
}

func coordinatorHandoverApplyRPCError(kind rootproto.HandoverAct, err error) error {
	if errors.Is(err, rootstate.ErrInvalidTenure) {
		return statusInvalidArgument(err.Error())
	}
	switch kind {
	case rootproto.HandoverActSeal:
		if errors.Is(err, rootstate.ErrPrimacy) {
			return rpcError(err)
		}
	case rootproto.HandoverActConfirm:
		if errors.Is(err, rootstate.ErrFinality) {
			return statusInvalidArgument(err.Error())
		}
		if errors.Is(err, rootstate.ErrPrimacy) {
			return rpcError(err)
		}
	case rootproto.HandoverActClose:
		if errors.Is(err, rootstate.ErrPrimacy) ||
			errors.Is(err, rootstate.ErrFinality) {
			return rpcError(err)
		}
	case rootproto.HandoverActReattach:
		if errors.Is(err, rootstate.ErrPrimacy) ||
			errors.Is(err, rootstate.ErrFinality) {
			return rpcError(err)
		}
	}
	return rpcError(err)
}
