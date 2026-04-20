package remote

import (
	"context"
	"errors"
	"fmt"

	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"time"
)

// Backend is the metadata-root authority surface exported over gRPC.
type Backend interface {
	Snapshot() (rootstate.Snapshot, error)
	Append(events ...rootevent.Event) (rootstate.CommitInfo, error)
	FenceAllocator(kind rootstate.AllocatorKind, min uint64) (uint64, error)
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
	ApplyCoordinatorLease(cmd rootstate.CoordinatorLeaseCommand) (rootstate.CoordinatorProtocolState, error)
	ApplyCoordinatorClosure(cmd rootstate.CoordinatorClosureCommand) (rootstate.CoordinatorProtocolState, error)
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
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &metapb.MetadataRootSnapshotResponse{Checkpoint: metawire.RootSnapshotToProto(snapshot, 0)}, nil
}

func (s *Service) Append(_ context.Context, req *metapb.MetadataRootAppendRequest) (*metapb.MetadataRootAppendResponse, error) {
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
			return nil, status.Error(codes.InvalidArgument, "metadata root append requires known event kind")
		}
		events = append(events, event)
	}
	commit, err := s.backend.Append(events...)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &metapb.MetadataRootAppendResponse{
		Cursor: metawire.RootCursorToProto(commit.Cursor),
		State:  metawire.RootStateToProto(commit.State),
	}, nil
}

func (s *Service) FenceAllocator(_ context.Context, req *metapb.MetadataRootFenceAllocatorRequest) (*metapb.MetadataRootFenceAllocatorResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootFenceAllocatorResponse{}, nil
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	kind, err := allocatorKindFromProto(req.GetKind())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	current, err := s.backend.FenceAllocator(kind, req.GetMinimum())
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
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

func (s *Service) ApplyCoordinatorLease(_ context.Context, req *metapb.MetadataRootApplyCoordinatorLeaseRequest) (*metapb.MetadataRootApplyCoordinatorLeaseResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootApplyCoordinatorLeaseResponse{}, nil
	}
	backend, err := s.coordinatorProtocolBackend()
	if err != nil {
		return nil, err
	}
	cmd := metawire.RootCoordinatorLeaseCommandFromProto(req.GetCommand())
	protocolState, err := backend.ApplyCoordinatorLease(cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrCoordinatorLeaseHeld) {
			return &metapb.MetadataRootApplyCoordinatorLeaseResponse{
				State:  metawire.RootCoordinatorProtocolStateToProto(protocolState),
				Status: metapb.RootCoordinatorLeaseApplyStatus_ROOT_COORDINATOR_LEASE_APPLY_STATUS_HELD,
			}, nil
		}
		return nil, coordinatorLeaseApplyRPCError(cmd.Kind, err)
	}
	return &metapb.MetadataRootApplyCoordinatorLeaseResponse{
		State:  metawire.RootCoordinatorProtocolStateToProto(protocolState),
		Status: metapb.RootCoordinatorLeaseApplyStatus_ROOT_COORDINATOR_LEASE_APPLY_STATUS_GRANTED,
	}, nil
}

func (s *Service) ApplyCoordinatorClosure(_ context.Context, req *metapb.MetadataRootApplyCoordinatorClosureRequest) (*metapb.MetadataRootApplyCoordinatorClosureResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootApplyCoordinatorClosureResponse{}, nil
	}
	backend, err := s.coordinatorProtocolBackend()
	if err != nil {
		return nil, err
	}
	cmd := metawire.RootCoordinatorClosureCommandFromProto(req.GetCommand())
	protocolState, err := backend.ApplyCoordinatorClosure(cmd)
	if err != nil {
		return nil, coordinatorClosureApplyRPCError(cmd.Kind, err)
	}
	return &metapb.MetadataRootApplyCoordinatorClosureResponse{
		State: metawire.RootCoordinatorProtocolStateToProto(protocolState),
	}, nil
}

func (s *Service) ObserveCommitted(context.Context, *metapb.MetadataRootObserveCommittedRequest) (*metapb.MetadataRootObserveCommittedResponse, error) {
	observed, err := s.observeCommitted()
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	checkpoint, tail := observedToProto(observed)
	return &metapb.MetadataRootObserveCommittedResponse{Checkpoint: checkpoint, Tail: tail}, nil
}

func (s *Service) ObserveTail(_ context.Context, req *metapb.MetadataRootObserveTailRequest) (*metapb.MetadataRootObserveTailResponse, error) {
	after := tailTokenFromProto(req.GetAfter())
	advance, err := s.observeTail(after)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	pbAfter, token, checkpoint, tail := tailAdvanceToObservedResponse(advance)
	return &metapb.MetadataRootObserveTailResponse{
		After:      pbAfter,
		Token:      token,
		Checkpoint: checkpoint,
		Tail:       tail,
	}, nil
}

func (s *Service) WaitTail(_ context.Context, req *metapb.MetadataRootWaitTailRequest) (*metapb.MetadataRootWaitTailResponse, error) {
	after := tailTokenFromProto(req.GetAfter())
	timeout := time.Duration(req.GetTimeoutMillis()) * time.Millisecond
	advance, err := s.waitTail(after, timeout)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}
	pbAfter, token, checkpoint, tail := tailAdvanceToObservedResponse(advance)
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
	return fallbackObservedFromSnapshot(snapshot), nil
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
	if leaderID == 0 {
		return status.Error(codes.FailedPrecondition, "metadata root not leader")
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("metadata root not leader (leader_id=%d)", leaderID))
}

func allocatorKindToProto(kind rootstate.AllocatorKind) metapb.RootAllocatorKind {
	switch kind {
	case rootstate.AllocatorKindID:
		return metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_ID
	case rootstate.AllocatorKindTSO:
		return metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_TSO
	default:
		return metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_UNSPECIFIED
	}
}

func allocatorKindFromProto(kind metapb.RootAllocatorKind) (rootstate.AllocatorKind, error) {
	switch kind {
	case metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_ID:
		return rootstate.AllocatorKindID, nil
	case metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_TSO:
		return rootstate.AllocatorKindTSO, nil
	default:
		return rootstate.AllocatorKindUnknown, fmt.Errorf("metadata root allocator kind is required")
	}
}

func (s *Service) coordinatorProtocolBackend() (leaseBackend, error) {
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	backend, ok := s.backend.(leaseBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "metadata root coordinator protocol is not supported")
	}
	return backend, nil
}

func coordinatorLeaseApplyRPCError(kind rootstate.CoordinatorLeaseCommandKind, err error) error {
	if errors.Is(err, rootstate.ErrInvalidCoordinatorLease) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	switch kind {
	case rootstate.CoordinatorLeaseCommandIssue:
		switch {
		case errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage),
			errors.Is(err, rootstate.ErrCoordinatorLeaseLineage):
			return status.Error(codes.FailedPrecondition, err.Error())
		}
	case rootstate.CoordinatorLeaseCommandRelease:
		switch {
		case errors.Is(err, rootstate.ErrCoordinatorLeaseOwner),
			errors.Is(err, rootstate.ErrInvalidCoordinatorLease):
			return status.Error(codes.FailedPrecondition, err.Error())
		}
	}
	return status.Error(codes.Internal, err.Error())
}

func coordinatorClosureApplyRPCError(kind rootstate.CoordinatorClosureCommandKind, err error) error {
	if errors.Is(err, rootstate.ErrInvalidCoordinatorLease) || errors.Is(err, rootstate.ErrCoordinatorLeaseAudit) {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	switch kind {
	case rootstate.CoordinatorClosureCommandSeal:
		if errors.Is(err, rootstate.ErrCoordinatorLeaseOwner) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
	case rootstate.CoordinatorClosureCommandConfirm:
		if errors.Is(err, rootstate.ErrCoordinatorLeaseOwner) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
	case rootstate.CoordinatorClosureCommandClose:
		if errors.Is(err, rootstate.ErrCoordinatorLeaseOwner) ||
			errors.Is(err, rootstate.ErrCoordinatorLeaseClose) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
	case rootstate.CoordinatorClosureCommandReattach:
		if errors.Is(err, rootstate.ErrCoordinatorLeaseOwner) ||
			errors.Is(err, rootstate.ErrCoordinatorLeaseReattach) {
			return status.Error(codes.FailedPrecondition, err.Error())
		}
	}
	return status.Error(codes.Internal, err.Error())
}
