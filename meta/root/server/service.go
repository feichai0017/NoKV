// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

// Package server exposes one metadata-root backend as a gRPC service. It
// mirrors the layout used by raftstore/server and coordinator/server: the
// companion meta/root/client package dials this service.
package server

import (
	"context"
	"errors"
	"time"

	nokverrors "github.com/feichai0017/NoKV/errors"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"google.golang.org/grpc"
)

const defaultMaxRootRPCMessageBytes = 64 << 20

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

type rootWriteReadyBackend interface {
	PrepareRootWrite(ctx context.Context) error
}

type observedBackend interface {
	ObserveCommitted() (rootstorage.ObservedCommitted, error)
}

type tailBackend interface {
	ObserveTail(after rootstorage.TailToken) (rootstorage.TailAdvance, error)
	WaitForTail(after rootstorage.TailToken, timeout time.Duration) (rootstorage.TailAdvance, error)
}

type leaseBackend interface {
	ApplyGrant(ctx context.Context, cmd rootproto.GrantCommand) (rootstate.EunomiaState, rootproto.GrantCertificate, error)
}

type perasAuthorityBackend interface {
	ApplyPerasAuthority(ctx context.Context, cmd rootproto.PerasAuthorityCommand) (rootstate.State, rootproto.PerasAuthorityGrant, error)
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

// GRPCServerOptions returns the production transport budget for metadata-root
// RPCs. Coordinator bootstrap can transfer a compact checkpoint that is larger
// than gRPC's 4 MiB default after long-running root workloads.
func GRPCServerOptions() []grpc.ServerOption {
	return []grpc.ServerOption{
		grpc.MaxRecvMsgSize(defaultMaxRootRPCMessageBytes),
		grpc.MaxSendMsgSize(defaultMaxRootRPCMessageBytes),
	}
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
	if err := s.requireRootWriteReady(ctx); err != nil {
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
	if err := s.requireRootWriteReady(ctx); err != nil {
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

func (s *Service) ApplyGrant(ctx context.Context, req *metapb.MetadataRootApplyGrantRequest) (*metapb.MetadataRootApplyGrantResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootApplyGrantResponse{}, nil
	}
	backend, err := s.coordinatorProtocolBackend(ctx)
	if err != nil {
		return nil, err
	}
	cmd := metawire.RootGrantCommandFromProto(req.GetCommand())
	protocolState, cert, err := backend.ApplyGrant(ctx, cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrPrimacy) {
			return &metapb.MetadataRootApplyGrantResponse{
				State:  metawire.RootEunomiaStateToProto(protocolState),
				Status: metapb.RootGrantApplyStatus_ROOT_GRANT_APPLY_STATUS_HELD,
			}, nil
		}
		return nil, coordinatorGrantApplyRPCError(cmd.Kind, err)
	}
	applyStatus := metapb.RootGrantApplyStatus_ROOT_GRANT_APPLY_STATUS_GRANTED
	if cmd.Kind == rootproto.GrantActSeal || cmd.Kind == rootproto.GrantActRetireExpired || cmd.Kind == rootproto.GrantActInherit {
		applyStatus = metapb.RootGrantApplyStatus_ROOT_GRANT_APPLY_STATUS_RETIRED
	}
	return &metapb.MetadataRootApplyGrantResponse{
		State:       metawire.RootEunomiaStateToProto(protocolState),
		Status:      applyStatus,
		Certificate: metawire.RootGrantCertificateToProto(cert),
	}, nil
}

func (s *Service) ApplyPerasAuthority(ctx context.Context, req *metapb.MetadataRootApplyPerasAuthorityRequest) (*metapb.MetadataRootApplyPerasAuthorityResponse, error) {
	if s == nil || s.backend == nil {
		return &metapb.MetadataRootApplyPerasAuthorityResponse{}, nil
	}
	if err := s.requireRootWriteReady(ctx); err != nil {
		return nil, err
	}
	backend, ok := s.backend.(perasAuthorityBackend)
	if !ok {
		return nil, statusUnimplemented("metadata root backend does not implement Peras authority protocol")
	}
	cmd := metawire.RootPerasAuthorityCommandFromProto(req.GetCommand())
	state, grant, err := backend.ApplyPerasAuthority(ctx, cmd)
	if err != nil {
		if errors.Is(err, rootstate.ErrPrimacy) {
			return &metapb.MetadataRootApplyPerasAuthorityResponse{
				State:  metawire.RootStateToProto(state),
				Status: metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_HELD,
			}, nil
		}
		return nil, rpcError(err)
	}
	applyStatus := metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_GRANTED
	switch cmd.Kind {
	case rootproto.PerasAuthorityActRetire:
		applyStatus = metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_RETIRED
	case rootproto.PerasAuthorityActSeal:
		applyStatus = metapb.RootPerasAuthorityApplyStatus_ROOT_PERAS_AUTHORITY_APPLY_STATUS_SEALED
	}
	return &metapb.MetadataRootApplyPerasAuthorityResponse{
		State:  metawire.RootStateToProto(state),
		Status: applyStatus,
		Grant:  metawire.RootPerasAuthorityGrantToProto(grant),
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

func (s *Service) requireRootWriteReady(ctx context.Context) error {
	if err := s.requireLeader(); err != nil {
		return err
	}
	if s == nil || s.backend == nil {
		return nil
	}
	ready, ok := s.backend.(rootWriteReadyBackend)
	if !ok {
		return nil
	}
	if err := ready.PrepareRootWrite(ctx); err != nil {
		if nokverrors.IsKind(err, nokverrors.KindNotLeader) {
			if leader, ok := s.backend.(leaderBackend); ok {
				return statusNotLeader(leader.LeaderID())
			}
			return statusNotLeader(0)
		}
		return rpcError(err)
	}
	return nil
}

func (s *Service) coordinatorProtocolBackend(ctx context.Context) (leaseBackend, error) {
	if err := s.requireRootWriteReady(ctx); err != nil {
		return nil, err
	}
	backend, ok := s.backend.(leaseBackend)
	if !ok {
		return nil, statusUnimplemented("metadata root coordinator protocol is not supported")
	}
	return backend, nil
}

func coordinatorGrantApplyRPCError(kind rootproto.GrantAct, err error) error {
	switch kind {
	case rootproto.GrantActIssue:
		switch {
		case errors.Is(err, rootstate.ErrInvalidGrant):
			return statusInvalidArgument(err.Error())
		case errors.Is(err, rootstate.ErrInheritance):
			return rpcError(err)
		}
	case rootproto.GrantActSeal, rootproto.GrantActRetireExpired, rootproto.GrantActInherit:
		switch {
		case errors.Is(err, rootstate.ErrPrimacy),
			errors.Is(err, rootstate.ErrInvalidGrant),
			errors.Is(err, rootstate.ErrFinality):
			return statusFailedPrecondition(err)
		}
	}
	return rpcError(err)
}
