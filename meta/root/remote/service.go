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
	CampaignCoordinatorLease(holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error)
	ReleaseCoordinatorLease(holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error)
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

func (s *Service) Campaign(_ context.Context, req *metapb.MetadataRootCampaignRequest) (*metapb.MetadataRootCampaignResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootCampaignResponse{}, nil
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	campaigner, ok := s.backend.(leaseBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "metadata root coordinator lease campaign is not supported")
	}
	lease, err := campaigner.CampaignCoordinatorLease(
		req.GetHolderId(),
		req.GetExpiresUnixNano(),
		req.GetNowUnixNano(),
		req.GetIdFence(),
		req.GetTsoFence(),
	)
	if err != nil {
		if errors.Is(err, rootstate.ErrCoordinatorLeaseHeld) {
			return &metapb.MetadataRootCampaignResponse{Granted: false, Lease: metawire.RootCoordinatorLeaseToProto(lease)}, nil
		}
		if errors.Is(err, rootstate.ErrInvalidCoordinatorLease) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &metapb.MetadataRootCampaignResponse{Granted: true, Lease: metawire.RootCoordinatorLeaseToProto(lease)}, nil
}

func (s *Service) Release(_ context.Context, req *metapb.MetadataRootReleaseRequest) (*metapb.MetadataRootReleaseResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootReleaseResponse{}, nil
	}
	if err := s.requireLeader(); err != nil {
		return nil, err
	}
	releaser, ok := s.backend.(leaseBackend)
	if !ok {
		return nil, status.Error(codes.Unimplemented, "metadata root coordinator lease release is not supported")
	}
	lease, err := releaser.ReleaseCoordinatorLease(
		req.GetHolderId(),
		req.GetNowUnixNano(),
		req.GetIdFence(),
		req.GetTsoFence(),
	)
	if err != nil {
		if errors.Is(err, rootstate.ErrCoordinatorLeaseOwner) || errors.Is(err, rootstate.ErrInvalidCoordinatorLease) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	return &metapb.MetadataRootReleaseResponse{Lease: metawire.RootCoordinatorLeaseToProto(lease)}, nil
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
