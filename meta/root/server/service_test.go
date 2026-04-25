package server

import (
	"context"
	"errors"
	"testing"
	"time"

	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type fakeServiceBackend struct {
	snapshot            rootstate.Snapshot
	snapshotErr         error
	appendErr           error
	fenceErr            error
	observeCommittedErr error
	observeTailErr      error
	waitTailErr         error
	applyLeaseErr       error
	applyClosureErr     error
	observed            rootstorage.ObservedCommitted
	observeAdvance      rootstorage.TailAdvance
	waitAdvance         rootstorage.TailAdvance
	applyLeaseResult    rootstate.EunomiaState
	applyClosureResult  rootstate.EunomiaState
	isLeader            bool
	leaderID            uint64
	appendCalls         int
	fenceCalls          []rootstate.AllocatorKind
	appendedEvents      []rootevent.Event
}

func (f *fakeServiceBackend) Snapshot() (rootstate.Snapshot, error) {
	if f.snapshotErr != nil {
		return rootstate.Snapshot{}, f.snapshotErr
	}
	return rootstate.CloneSnapshot(f.snapshot), nil
}

func (f *fakeServiceBackend) Append(_ context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if f.appendErr != nil {
		return rootstate.CommitInfo{}, f.appendErr
	}
	f.appendCalls++
	for _, event := range events {
		cursor := rootstate.Cursor{
			Term:  max(f.snapshot.State.LastCommitted.Term, 1),
			Index: f.snapshot.State.LastCommitted.Index + 1,
		}
		rootstate.ApplyEventToSnapshot(&f.snapshot, cursor, event)
		f.appendedEvents = append(f.appendedEvents, rootevent.CloneEvent(event))
	}
	return rootstate.CommitInfo{
		Cursor: f.snapshot.State.LastCommitted,
		State:  f.snapshot.State,
	}, nil
}

func (f *fakeServiceBackend) FenceAllocator(_ context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if f.fenceErr != nil {
		return 0, f.fenceErr
	}
	f.fenceCalls = append(f.fenceCalls, kind)
	switch kind {
	case rootstate.AllocatorKindID:
		if min > f.snapshot.State.IDFence {
			f.snapshot.State.IDFence = min
		}
		return f.snapshot.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if min > f.snapshot.State.TSOFence {
			f.snapshot.State.TSOFence = min
		}
		return f.snapshot.State.TSOFence, nil
	default:
		return 0, nil
	}
}

func (f *fakeServiceBackend) IsLeader() bool   { return f.isLeader }
func (f *fakeServiceBackend) LeaderID() uint64 { return f.leaderID }

func (f *fakeServiceBackend) ObserveCommitted() (rootstorage.ObservedCommitted, error) {
	if f.observeCommittedErr != nil {
		return rootstorage.ObservedCommitted{}, f.observeCommittedErr
	}
	return rootstorage.CloneObservedCommitted(f.observed), nil
}

func (f *fakeServiceBackend) ObserveTail(rootstorage.TailToken) (rootstorage.TailAdvance, error) {
	if f.observeTailErr != nil {
		return rootstorage.TailAdvance{}, f.observeTailErr
	}
	return f.observeAdvance, nil
}

func (f *fakeServiceBackend) WaitForTail(rootstorage.TailToken, time.Duration) (rootstorage.TailAdvance, error) {
	if f.waitTailErr != nil {
		return rootstorage.TailAdvance{}, f.waitTailErr
	}
	return f.waitAdvance, nil
}

func (f *fakeServiceBackend) ApplyTenure(context.Context, rootproto.TenureCommand) (rootstate.EunomiaState, error) {
	return f.applyLeaseResult, f.applyLeaseErr
}

func (f *fakeServiceBackend) ApplyHandover(context.Context, rootproto.HandoverCommand) (rootstate.EunomiaState, error) {
	return f.applyClosureResult, f.applyClosureErr
}

type basicServiceBackend struct {
	snapshot  rootstate.Snapshot
	isLeader  bool
	leaderID  uint64
	appendErr error
	fenceErr  error
}

func (b *basicServiceBackend) Snapshot() (rootstate.Snapshot, error) {
	return rootstate.CloneSnapshot(b.snapshot), nil
}

func (b *basicServiceBackend) Append(_ context.Context, events ...rootevent.Event) (rootstate.CommitInfo, error) {
	if b.appendErr != nil {
		return rootstate.CommitInfo{}, b.appendErr
	}
	for _, event := range events {
		cursor := rootstate.Cursor{
			Term:  max(b.snapshot.State.LastCommitted.Term, 1),
			Index: b.snapshot.State.LastCommitted.Index + 1,
		}
		rootstate.ApplyEventToSnapshot(&b.snapshot, cursor, event)
	}
	return rootstate.CommitInfo{
		Cursor: b.snapshot.State.LastCommitted,
		State:  b.snapshot.State,
	}, nil
}

func (b *basicServiceBackend) FenceAllocator(_ context.Context, kind rootstate.AllocatorKind, min uint64) (uint64, error) {
	if b.fenceErr != nil {
		return 0, b.fenceErr
	}
	switch kind {
	case rootstate.AllocatorKindID:
		if min > b.snapshot.State.IDFence {
			b.snapshot.State.IDFence = min
		}
		return b.snapshot.State.IDFence, nil
	case rootstate.AllocatorKindTSO:
		if min > b.snapshot.State.TSOFence {
			b.snapshot.State.TSOFence = min
		}
		return b.snapshot.State.TSOFence, nil
	default:
		return 0, nil
	}
}

func (b *basicServiceBackend) IsLeader() bool   { return b.isLeader }
func (b *basicServiceBackend) LeaderID() uint64 { return b.leaderID }

func TestNewServiceAndRegister(t *testing.T) {
	backend := &fakeServiceBackend{}
	svc := NewService(backend)
	require.Same(t, backend, svc.backend)

	grpcServer := grpc.NewServer()
	Register(grpcServer, backend)
	_, ok := grpcServer.GetServiceInfo()[metapb.MetadataRoot_ServiceDesc.ServiceName]
	require.True(t, ok)
}

func TestServiceSnapshotPaths(t *testing.T) {
	t.Run("nil service", func(t *testing.T) {
		var svc *Service
		resp, err := svc.Snapshot(context.Background(), &metapb.MetadataRootSnapshotRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Checkpoint)
	})

	t.Run("backend error", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{snapshotErr: errors.New("boom")})
		_, err := svc.Snapshot(context.Background(), &metapb.MetadataRootSnapshotRequest{})
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("success", func(t *testing.T) {
		backend := &fakeServiceBackend{snapshot: testServerSnapshot()}
		resp, err := NewService(backend).Snapshot(context.Background(), &metapb.MetadataRootSnapshotRequest{})
		require.NoError(t, err)
		got, tailOffset := metawire.RootSnapshotFromProto(resp.Checkpoint)
		require.Equal(t, backend.snapshot.State, got.State)
		require.Equal(t, backend.snapshot.Descriptors[1], got.Descriptors[1])
		require.Zero(t, tailOffset)
	})
}

func TestServiceAppendPaths(t *testing.T) {
	t.Run("nil service", func(t *testing.T) {
		var svc *Service
		resp, err := svc.Append(context.Background(), &metapb.MetadataRootAppendRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
		require.Nil(t, resp.Cursor)
	})

	t.Run("non leader with leader id", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{isLeader: false, leaderID: 9})
		_, err := svc.Append(context.Background(), &metapb.MetadataRootAppendRequest{})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "leader_id=9")
	})

	t.Run("non leader without leader id", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{isLeader: false})
		_, err := svc.Append(context.Background(), &metapb.MetadataRootAppendRequest{})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
		require.Contains(t, err.Error(), "metadata root not leader")
	})

	t.Run("invalid event kind", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{isLeader: true})
		_, err := svc.Append(context.Background(), &metapb.MetadataRootAppendRequest{
			Events: []*metapb.RootEvent{{Kind: metapb.RootEventKind_ROOT_EVENT_KIND_UNSPECIFIED}},
		})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("backend error", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{isLeader: true, appendErr: errors.New("append failed")})
		_, err := svc.Append(context.Background(), &metapb.MetadataRootAppendRequest{
			Events: []*metapb.RootEvent{metawire.RootEventToProto(rootevent.IDAllocatorFenced(11))},
		})
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("success", func(t *testing.T) {
		backend := &fakeServiceBackend{
			snapshot: testServerSnapshot(),
			isLeader: true,
		}
		resp, err := NewService(backend).Append(context.Background(), &metapb.MetadataRootAppendRequest{
			Events: []*metapb.RootEvent{metawire.RootEventToProto(rootevent.IDAllocatorFenced(33))},
		})
		require.NoError(t, err)
		require.Equal(t, 1, backend.appendCalls)
		require.Len(t, backend.appendedEvents, 1)
		require.Equal(t, rootevent.KindIDAllocatorFenced, backend.appendedEvents[0].Kind)
		require.Equal(t, uint64(33), resp.State.IdFence)
		require.Equal(t, uint64(1), resp.Cursor.Term)
		require.Equal(t, uint64(2), resp.Cursor.Index)
	})
}

func TestServiceFenceAllocatorAndStatus(t *testing.T) {
	t.Run("nil service", func(t *testing.T) {
		var svc *Service
		resp, err := svc.FenceAllocator(context.Background(), &metapb.MetadataRootFenceAllocatorRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)

		statusResp, err := svc.Status(context.Background(), &metapb.MetadataRootStatusRequest{})
		require.NoError(t, err)
		require.True(t, statusResp.IsLeader)
	})

	t.Run("require leader and invalid kind", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{isLeader: false})
		_, err := svc.FenceAllocator(context.Background(), &metapb.MetadataRootFenceAllocatorRequest{})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))

		svc = NewService(&fakeServiceBackend{isLeader: true})
		_, err = svc.FenceAllocator(context.Background(), &metapb.MetadataRootFenceAllocatorRequest{})
		require.Equal(t, codes.InvalidArgument, status.Code(err))
	})

	t.Run("backend error", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{isLeader: true, fenceErr: errors.New("nope")})
		_, err := svc.FenceAllocator(context.Background(), &metapb.MetadataRootFenceAllocatorRequest{
			Kind:    metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_ID,
			Minimum: 77,
		})
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("success and status", func(t *testing.T) {
		backend := &fakeServiceBackend{
			snapshot: testServerSnapshot(),
			isLeader: true,
		}
		resp, err := NewService(backend).FenceAllocator(context.Background(), &metapb.MetadataRootFenceAllocatorRequest{
			Kind:    metapb.RootAllocatorKind_ROOT_ALLOCATOR_KIND_TSO,
			Minimum: 99,
		})
		require.NoError(t, err)
		require.Equal(t, uint64(99), resp.Current)
		require.Equal(t, []rootstate.AllocatorKind{rootstate.AllocatorKindTSO}, backend.fenceCalls)

		statusResp, err := NewService(&fakeServiceBackend{isLeader: false, leaderID: 42}).Status(context.Background(), &metapb.MetadataRootStatusRequest{})
		require.NoError(t, err)
		require.False(t, statusResp.IsLeader)
		require.Equal(t, uint64(42), statusResp.LeaderId)
	})

	t.Run("status without leader backend defaults to leader", func(t *testing.T) {
		resp, err := NewService(&struct{ Backend }{Backend: &basicServiceBackend{snapshot: testServerSnapshot()}}).Status(context.Background(), &metapb.MetadataRootStatusRequest{})
		require.NoError(t, err)
		require.True(t, resp.IsLeader)
	})
}

func TestServiceObserveFallbackAndTailPaths(t *testing.T) {
	snapshot := testServerSnapshot()
	observed := rootstorage.ObservedCommitted{
		Checkpoint: rootstorage.Checkpoint{Snapshot: snapshot, TailOffset: 3},
		Tail: rootstorage.CommittedTail{
			RequestedOffset: 1,
			StartOffset:     2,
			EndOffset:       3,
			Records: []rootstorage.CommittedEvent{{
				Cursor: rootstate.Cursor{Term: 2, Index: 4},
				Event:  rootevent.IDAllocatorFenced(44),
			}},
		},
	}
	after := rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Revision: 3}
	token := rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 4}, Revision: 4}
	advance := observed.Advance(after, token)

	t.Run("observe committed fallback", func(t *testing.T) {
		svc := NewService(&basicServiceBackend{snapshot: snapshot, isLeader: true})
		resp, err := svc.ObserveCommitted(context.Background(), &metapb.MetadataRootObserveCommittedRequest{})
		require.NoError(t, err)
		got := metawire.RootObservedFromProto(resp.Checkpoint, resp.Tail)
		require.Equal(t, snapshot.State, got.Checkpoint.Snapshot.State)
		require.Empty(t, got.Tail.Records)
	})

	t.Run("observe committed backend and error", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{observed: observed})
		resp, err := svc.ObserveCommitted(context.Background(), &metapb.MetadataRootObserveCommittedRequest{})
		require.NoError(t, err)
		require.Equal(t, observed, metawire.RootObservedFromProto(resp.Checkpoint, resp.Tail))

		svc = NewService(&fakeServiceBackend{observeCommittedErr: errors.New("observe failed")})
		_, err = svc.ObserveCommitted(context.Background(), &metapb.MetadataRootObserveCommittedRequest{})
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("observe tail fallback and backend", func(t *testing.T) {
		svc := NewService(&basicServiceBackend{snapshot: snapshot, isLeader: true})
		resp, err := svc.ObserveTail(context.Background(), &metapb.MetadataRootObserveTailRequest{
			After: metawire.RootTailTokenToProto(after),
		})
		require.NoError(t, err)
		got := metawire.RootTailAdvanceFromProto(resp.After, resp.Token, resp.Checkpoint, resp.Tail)
		require.Equal(t, after, got.After)
		require.Equal(t, snapshot.State.LastCommitted, got.Token.Cursor)

		svc = NewService(&fakeServiceBackend{observeAdvance: advance})
		resp, err = svc.ObserveTail(context.Background(), &metapb.MetadataRootObserveTailRequest{
			After: metawire.RootTailTokenToProto(after),
		})
		require.NoError(t, err)
		require.Equal(t, advance, metawire.RootTailAdvanceFromProto(resp.After, resp.Token, resp.Checkpoint, resp.Tail))

		svc = NewService(&fakeServiceBackend{observeTailErr: errors.New("tail failed")})
		_, err = svc.ObserveTail(context.Background(), &metapb.MetadataRootObserveTailRequest{})
		require.Equal(t, codes.Internal, status.Code(err))
	})

	t.Run("wait tail fallback and backend", func(t *testing.T) {
		svc := NewService(&basicServiceBackend{snapshot: snapshot, isLeader: true})
		resp, err := svc.WaitTail(context.Background(), &metapb.MetadataRootWaitTailRequest{
			After:         metawire.RootTailTokenToProto(after),
			TimeoutMillis: 5,
		})
		require.NoError(t, err)
		got := metawire.RootTailAdvanceFromProto(resp.After, resp.Token, resp.Checkpoint, resp.Tail)
		require.Equal(t, snapshot.State.LastCommitted, got.Token.Cursor)

		svc = NewService(&fakeServiceBackend{waitAdvance: advance})
		resp, err = svc.WaitTail(context.Background(), &metapb.MetadataRootWaitTailRequest{
			After:         metawire.RootTailTokenToProto(after),
			TimeoutMillis: 5,
		})
		require.NoError(t, err)
		require.Equal(t, advance, metawire.RootTailAdvanceFromProto(resp.After, resp.Token, resp.Checkpoint, resp.Tail))

		svc = NewService(&fakeServiceBackend{waitTailErr: errors.New("wait failed")})
		_, err = svc.WaitTail(context.Background(), &metapb.MetadataRootWaitTailRequest{})
		require.Equal(t, codes.Internal, status.Code(err))
	})
}

func TestServiceApplyTenure(t *testing.T) {
	leaseState := rootstate.EunomiaState{
		Tenure: rootstate.Tenure{
			HolderID:        "coord-1",
			ExpiresUnixNano: 1234,
			Era:             7,
			Mandate:         rootproto.MandateDefault,
		},
	}
	cmd := rootproto.TenureCommand{
		Kind:            rootproto.TenureActIssue,
		HolderID:        "coord-1",
		ExpiresUnixNano: 1234,
		NowUnixNano:     1000,
	}

	t.Run("nil service", func(t *testing.T) {
		var svc *Service
		resp, err := svc.ApplyTenure(context.Background(), &metapb.MetadataRootApplyTenureRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("unimplemented", func(t *testing.T) {
		svc := NewService(&basicServiceBackend{snapshot: testServerSnapshot(), isLeader: true})
		_, err := svc.ApplyTenure(context.Background(), &metapb.MetadataRootApplyTenureRequest{
			Command: metawire.RootTenureCommandToProto(cmd),
		})
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("held maps to response status", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{
			isLeader:         true,
			applyLeaseResult: leaseState,
			applyLeaseErr:    rootstate.ErrPrimacy,
		})
		resp, err := svc.ApplyTenure(context.Background(), &metapb.MetadataRootApplyTenureRequest{
			Command: metawire.RootTenureCommandToProto(cmd),
		})
		require.NoError(t, err)
		require.Equal(t, metapb.RootTenureApplyStatus_ROOT_TENURE_APPLY_STATUS_HELD, resp.Status)
		require.Equal(t, leaseState, metawire.RootEunomiaStateFromProto(resp.State))
	})

	t.Run("success", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{
			isLeader:         true,
			applyLeaseResult: leaseState,
		})
		resp, err := svc.ApplyTenure(context.Background(), &metapb.MetadataRootApplyTenureRequest{
			Command: metawire.RootTenureCommandToProto(cmd),
		})
		require.NoError(t, err)
		require.Equal(t, metapb.RootTenureApplyStatus_ROOT_TENURE_APPLY_STATUS_GRANTED, resp.Status)
		require.Equal(t, leaseState, metawire.RootEunomiaStateFromProto(resp.State))
	})
}

func TestServiceApplyHandover(t *testing.T) {
	closureState := rootstate.EunomiaState{
		Handover: rootstate.Handover{
			HolderID:     "coord-1",
			LegacyEra:    3,
			SuccessorEra: 4,
			LegacyDigest: "digest",
			Stage:        rootproto.HandoverStageClosed,
		},
	}
	cmd := rootproto.HandoverCommand{
		Kind:        rootproto.HandoverActClose,
		HolderID:    "coord-1",
		NowUnixNano: 1000,
	}

	t.Run("nil service", func(t *testing.T) {
		var svc *Service
		resp, err := svc.ApplyHandover(context.Background(), &metapb.MetadataRootApplyHandoverRequest{})
		require.NoError(t, err)
		require.NotNil(t, resp)
	})

	t.Run("unimplemented", func(t *testing.T) {
		svc := NewService(&basicServiceBackend{snapshot: testServerSnapshot(), isLeader: true})
		_, err := svc.ApplyHandover(context.Background(), &metapb.MetadataRootApplyHandoverRequest{
			Command: metawire.RootHandoverCommandToProto(cmd),
		})
		require.Equal(t, codes.Unimplemented, status.Code(err))
	})

	t.Run("success", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{
			isLeader:           true,
			applyClosureResult: closureState,
		})
		resp, err := svc.ApplyHandover(context.Background(), &metapb.MetadataRootApplyHandoverRequest{
			Command: metawire.RootHandoverCommandToProto(cmd),
		})
		require.NoError(t, err)
		require.Equal(t, closureState, metawire.RootEunomiaStateFromProto(resp.State))
	})

	t.Run("mapped failed precondition", func(t *testing.T) {
		svc := NewService(&fakeServiceBackend{
			isLeader:        true,
			applyClosureErr: rootstate.ErrFinality,
		})
		_, err := svc.ApplyHandover(context.Background(), &metapb.MetadataRootApplyHandoverRequest{
			Command: metawire.RootHandoverCommandToProto(cmd),
		})
		require.Equal(t, codes.FailedPrecondition, status.Code(err))
	})
}

func TestCoordinatorApplyErrorMappings(t *testing.T) {
	require.Equal(t, codes.InvalidArgument, status.Code(coordinatorLeaseApplyRPCError(rootproto.TenureActIssue, rootstate.ErrInvalidTenure)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorLeaseApplyRPCError(rootproto.TenureActIssue, rootstate.ErrInheritance)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorLeaseApplyRPCError(rootproto.TenureActIssue, rootstate.ErrInheritance)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorLeaseApplyRPCError(rootproto.TenureActRelease, rootstate.ErrPrimacy)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorLeaseApplyRPCError(rootproto.TenureActRelease, rootstate.ErrInvalidTenure)))
	require.Equal(t, codes.Internal, status.Code(coordinatorLeaseApplyRPCError(rootproto.TenureActUnknown, errors.New("boom"))))

	require.Equal(t, codes.InvalidArgument, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActSeal, rootstate.ErrInvalidTenure)))
	require.Equal(t, codes.InvalidArgument, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActConfirm, rootstate.ErrFinality)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActSeal, rootstate.ErrPrimacy)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActConfirm, rootstate.ErrPrimacy)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActClose, rootstate.ErrPrimacy)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActClose, rootstate.ErrFinality)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActReattach, rootstate.ErrPrimacy)))
	require.Equal(t, codes.FailedPrecondition, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActReattach, rootstate.ErrFinality)))
	require.Equal(t, codes.Internal, status.Code(coordinatorHandoverApplyRPCError(rootproto.HandoverActUnknown, errors.New("boom"))))
}

func testServerSnapshot() rootstate.Snapshot {
	desc := descriptor.Descriptor{
		RegionID:  1,
		StartKey:  []byte("a"),
		EndKey:    []byte("m"),
		Epoch:     metaregion.Epoch{Version: 1, ConfVersion: 1},
		Peers:     []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		State:     metaregion.ReplicaStateRunning,
		RootEpoch: 1,
	}
	desc.EnsureHash()
	return rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:    2,
			MembershipEpoch: 3,
			LastCommitted:   rootstate.Cursor{Term: 1, Index: 1},
			IDFence:         10,
			TSOFence:        20,
		},
		Descriptors: map[uint64]descriptor.Descriptor{
			desc.RegionID: desc,
		},
		PendingPeerChanges:  map[uint64]rootstate.PendingPeerChange{},
		PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{},
	}
}
