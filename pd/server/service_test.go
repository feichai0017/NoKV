package server

import (
	"context"
	"errors"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootmaterialize "github.com/feichai0017/NoKV/meta/root/materialize"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	pdpb "github.com/feichai0017/NoKV/pb/pd"
	pdstorage "github.com/feichai0017/NoKV/pd/storage"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"net"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	metapb "github.com/feichai0017/NoKV/pb/meta"
	"github.com/feichai0017/NoKV/pd/core"
	"github.com/feichai0017/NoKV/pd/tso"
)

type fakeStorage struct {
	eventCalls int
	saveCalls  int
	eventErr   error
	saveErr    error
	lastID     uint64
	lastTS     uint64
	leader     bool
	leaderID   uint64
	lastEvent  rootevent.Event
	snapshot   pdstorage.Snapshot
}

func (f *fakeStorage) Load() (pdstorage.Snapshot, error) {
	return pdstorage.Snapshot{
		ClusterEpoch:       f.snapshot.ClusterEpoch,
		Descriptors:        rootCloneDescriptorsForTest(f.snapshot.Descriptors),
		PendingPeerChanges: rootstate.ClonePendingPeerChanges(f.snapshot.PendingPeerChanges),
		Allocator:          f.snapshot.Allocator,
	}, nil
}

func (f *fakeStorage) AppendRootEvent(event rootevent.Event) error {
	f.eventCalls++
	f.lastEvent = event
	if f.eventErr != nil {
		return f.eventErr
	}
	if event.Kind == rootevent.KindUnknown {
		return errors.New("invalid root event")
	}
	snapshot := rootstate.Snapshot{
		State: rootstate.State{
			ClusterEpoch:  f.snapshot.ClusterEpoch,
			IDFence:       f.snapshot.Allocator.IDCurrent,
			TSOFence:      f.snapshot.Allocator.TSCurrent,
			LastCommitted: rootstate.Cursor{Term: 1, Index: uint64(f.eventCalls)},
		},
		Descriptors:        rootCloneDescriptorsForTest(f.snapshot.Descriptors),
		PendingPeerChanges: rootstate.ClonePendingPeerChanges(f.snapshot.PendingPeerChanges),
	}
	rootmaterialize.ApplyEventToSnapshot(&snapshot, snapshot.State.LastCommitted, event)
	f.snapshot.ClusterEpoch = snapshot.State.ClusterEpoch
	f.snapshot.Descriptors = rootCloneDescriptorsForTest(snapshot.Descriptors)
	f.snapshot.PendingPeerChanges = rootstate.ClonePendingPeerChanges(snapshot.PendingPeerChanges)
	f.snapshot.Allocator.IDCurrent = snapshot.State.IDFence
	f.snapshot.Allocator.TSCurrent = snapshot.State.TSOFence
	return nil
}

func (f *fakeStorage) SaveAllocatorState(idCurrent, tsCurrent uint64) error {
	f.saveCalls++
	f.lastID = idCurrent
	f.lastTS = tsCurrent
	return f.saveErr
}

func (f *fakeStorage) Close() error {
	return nil
}

func (f *fakeStorage) Refresh() error {
	return nil
}

func (f *fakeStorage) IsLeader() bool {
	return f == nil || f.leader || f.leaderID == 0
}

func (f *fakeStorage) LeaderID() uint64 {
	if f == nil {
		return 0
	}
	return f.leaderID
}

type fakeSyncStorage struct {
	fakeStorage
	snapshot pdstorage.Snapshot
}

func (f *fakeSyncStorage) Load() (pdstorage.Snapshot, error) {
	return pdstorage.Snapshot{
		ClusterEpoch:       f.snapshot.ClusterEpoch,
		Descriptors:        rootCloneDescriptorsForTest(f.snapshot.Descriptors),
		PendingPeerChanges: rootstate.ClonePendingPeerChanges(f.snapshot.PendingPeerChanges),
		Allocator:          f.snapshot.Allocator,
	}, nil
}

func (f *fakeSyncStorage) Refresh() error {
	return nil
}

func rootCloneDescriptorsForTest(in map[uint64]descriptor.Descriptor) map[uint64]descriptor.Descriptor {
	out := make(map[uint64]descriptor.Descriptor, len(in))
	for id, desc := range in {
		out[id] = desc.Clone()
	}
	return out
}

func testRegionDescriptorProto(desc descriptor.Descriptor) *metapb.RegionDescriptor {
	return metacodec.DescriptorToProto(desc)
}

func TestServiceStoreHeartbeatAndGetRegionByKey(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))

	storeResp, err := svc.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 3,
		LeaderNum: 1,
		Capacity:  1000,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})),
	})
	require.NoError(t, err)

	getResp, err := svc.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.NotNil(t, getResp.GetRegionDescriptor())
	require.Equal(t, uint64(11), getResp.GetRegionDescriptor().GetRegionId())
}

func TestServiceRemoveRegion(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.NoError(t, err)

	resp, err := svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())

	getResp, err := svc.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	resp, err = svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.False(t, resp.GetRemoved())
}

func TestServiceRegionHeartbeatRejectsStaleAndOverlap(t *testing.T) {
	svc := NewService(core.NewCluster(), nil, nil)
	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 2}, nil)),
	})
	require.NoError(t, err)

	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(2, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServiceAllocIDAndTSO(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(100), tso.NewAllocator(500))

	idResp, err := svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), idResp.GetFirstId())
	require.Equal(t, uint64(3), idResp.GetCount())

	tsResp, err := svc.Tso(context.Background(), &pdpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(500), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
}

func TestServiceRequestValidation(t *testing.T) {
	svc := NewService(nil, nil, nil)

	_, err := svc.StoreHeartbeat(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.AllocID(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.Tso(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 0})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServiceStoreHeartbeatReturnsLeaderTransferHint(t *testing.T) {
	svc := NewService(core.NewCluster(), nil, nil)
	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(100, []byte(""), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}})),
	})
	require.NoError(t, err)

	_, err = svc.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{
		StoreId:   2,
		LeaderNum: 1,
		RegionNum: 1,
	})
	require.NoError(t, err)

	resp, err := svc.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{
		StoreId:   1,
		LeaderNum: 10,
		RegionNum: 1,
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Len(t, resp.GetOperations(), 1)
	op := resp.GetOperations()[0]
	require.Equal(t, pdpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER, op.GetType())
	require.Equal(t, uint64(100), op.GetRegionId())
	require.Equal(t, uint64(101), op.GetSourcePeerId())
	require.Equal(t, uint64(201), op.GetTargetPeerId())
}

func TestServicePersistsRegionCatalog(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(42, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, rootevent.KindRegionBootstrap, store.lastEvent.Kind)
	require.Equal(t, uint64(1), store.lastEvent.RegionDescriptor.Descriptor.RootEpoch)

	_, err = svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 42})
	require.NoError(t, err)
	require.Equal(t, 2, store.eventCalls)
	require.Equal(t, rootevent.KindRegionTombstoned, store.lastEvent.Kind)
}

func TestServiceRegionHeartbeatSkipsUnchangedDescriptorPersistence(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	desc := testDescriptor(42, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(desc),
	})
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)

	before, ok := svc.cluster.RegionLastHeartbeat(42)
	require.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(desc),
	})
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	after, ok := svc.cluster.RegionLastHeartbeat(42)
	require.True(t, ok)
	require.True(t, after.After(before) || after.Equal(before))

	lookup, ok := svc.cluster.GetRegionDescriptor(42)
	require.True(t, ok)
	require.Equal(t, uint64(1), lookup.RootEpoch)
	_, ok = svc.cluster.GetRegionDescriptorByKey([]byte("m"))
	require.True(t, ok)
}

func TestServicePublishRootEvent(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	event := rootevent.RegionSplitCommitted(
		41,
		[]byte("m"),
		testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
		testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
	)
	resp, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)

	left, ok := svc.cluster.GetRegionDescriptorByKey([]byte("b"))
	require.True(t, ok)
	require.Equal(t, uint64(41), left.RegionID)

	right, ok := svc.cluster.GetRegionDescriptorByKey([]byte("x"))
	require.True(t, ok)
	require.Equal(t, uint64(42), right.RegionID)
}

func TestServicePublishRootEventAppliedPeerChangeClearsPending(t *testing.T) {
	cluster := core.NewCluster()
	target := testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 5
	target.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(target))

	store := &fakeStorage{
		leader: true,
		snapshot: pdstorage.Snapshot{
			ClusterEpoch:       5,
			Descriptors:        map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target}},
		},
	}
	svc := NewService(cluster, core.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	applied := rootevent.PeerAdded(target.RegionID, 2, 201, func() descriptor.Descriptor {
		desc := target.Clone()
		desc.RootEpoch = 0
		return desc
	}())
	resp, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(applied),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, uint64(5), store.snapshot.ClusterEpoch)
	require.Empty(t, store.snapshot.PendingPeerChanges)
}

func TestServicePublishRootEventPersistsPeerPlan(t *testing.T) {
	cluster := core.NewCluster()
	current := testDescriptor(12, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 0
	target.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: pdstorage.Snapshot{
			ClusterEpoch: 5,
			Descriptors:  map[uint64]descriptor.Descriptor{current.RegionID: current},
		},
	}
	svc := NewService(cluster, core.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, rootevent.KindPeerAdditionPlanned, store.lastEvent.Kind)
	require.Equal(t, uint64(6), store.lastEvent.PeerChange.Region.RootEpoch)
}

func TestServicePublishRootEventSkipsDuplicatePeerPlan(t *testing.T) {
	cluster := core.NewCluster()
	current := testDescriptor(13, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: pdstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, core.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
	require.Equal(t, uint64(6), store.snapshot.ClusterEpoch)
	require.Len(t, store.snapshot.PendingPeerChanges, 1)
}

func TestServicePublishRootEventRejectsConflictingPeerPlan(t *testing.T) {
	cluster := core.NewCluster()
	current := testDescriptor(14, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	conflicting := current.Clone()
	conflicting.Peers = append(conflicting.Peers, metaregion.Peer{StoreID: 3, PeerID: 301})
	conflicting.Epoch.ConfVersion++
	conflicting.RootEpoch = 6
	conflicting.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: pdstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, core.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	_, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsMismatchedPeerApply(t *testing.T) {
	cluster := core.NewCluster()
	current := testDescriptor(15, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(current))

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	mismatched := current.Clone()
	mismatched.Peers = append(mismatched.Peers, metaregion.Peer{StoreID: 3, PeerID: 301})
	mismatched.Epoch.ConfVersion++
	mismatched.RootEpoch = 6
	mismatched.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: pdstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, core.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	_, err := svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdded(mismatched.RegionID, 3, 301, mismatched)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventValidationAndPersistenceError(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.PublishRootEvent(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	store := &fakeStorage{eventErr: errors.New("persist root event failed")}
	svc.SetStorage(store)
	event := rootevent.RegionMerged(
		10,
		11,
		testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
	)
	_, err = svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(event),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("m"))
	require.False(t, ok)
}

func TestServiceRegionCatalogPersistenceErrors(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{eventErr: errors.New("persist update failed")}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("b"))
	require.False(t, ok)

	store.eventErr = nil
	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)),
	})
	require.NoError(t, err)
	store.eventErr = errors.New("persist delete failed")
	_, err = svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	resp, lookupErr := svc.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, lookupErr)
	require.False(t, resp.GetNotFound())
}

func TestServicePersistsAllocatorState(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(10), tso.NewAllocator(100))
	store := &fakeStorage{}
	svc.SetStorage(store)

	idResp, err := svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(11), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)

	tsResp, err := svc.Tso(context.Background(), &pdpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(11), store.lastID)
	require.Equal(t, uint64(102), store.lastTS)
}

func TestServiceAllocatorStatePersistenceError(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{saveErr: errors.New("persist failed")}
	svc.SetStorage(store)

	_, err := svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.saveErr = nil
	idResp, err := svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), idResp.GetFirstId())

	store.saveErr = errors.New("persist failed")
	_, err = svc.Tso(context.Background(), &pdpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.saveErr = nil
	tsResp, err := svc.Tso(context.Background(), &pdpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), tsResp.GetTimestamp())
}

func TestServiceRejectsWritesOnFollower(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(10), tso.NewAllocator(100))
	store := &fakeStorage{leader: false, leaderID: 2}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, strings.Contains(err.Error(), errNotLeaderPrefix))

	_, err = svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionTombstoned(8)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.Tso(context.Background(), &pdpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.StoreHeartbeat(context.Background(), &pdpb.StoreHeartbeatRequest{StoreId: 1})
	require.NoError(t, err)
	_, err = svc.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
}

func TestServiceRefreshFromStorageReloadsViewAndAllocatorState(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeSyncStorage{
		fakeStorage: fakeStorage{leader: false, leaderID: 2},
		snapshot: pdstorage.Snapshot{
			ClusterEpoch: 4,
			Descriptors: map[uint64]descriptor.Descriptor{
				9: testDescriptor(9, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
			},
			Allocator: pdstorage.AllocatorState{
				IDCurrent: 120,
				TSCurrent: 450,
			},
		},
	}
	svc.SetStorage(store)

	require.NoError(t, svc.RefreshFromStorage())

	getResp, err := svc.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(9), getResp.GetRegionDescriptor().GetRegionId())

	idResp, err := svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Nil(t, idResp)

	store.leader = true
	store.leaderID = 0

	idResp, err = svc.AllocID(context.Background(), &pdpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(121), idResp.GetFirstId())

	tsResp, err := svc.Tso(context.Background(), &pdpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(451), tsResp.GetTimestamp())
}

func TestServicePublishRootEventAssignsRootEpoch(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.NoError(t, err)

	event := rootevent.PeerAdded(1, 2, 201, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	event.PeerChange.Region.RootEpoch = 0
	_, err = svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.Equal(t, rootevent.KindPeerAdded, store.lastEvent.Kind)
	require.Equal(t, uint64(2), store.lastEvent.PeerChange.Region.RootEpoch)
	require.Equal(t, uint64(2), store.snapshot.ClusterEpoch)
}

func TestServiceMutatingWritesRespectExpectedClusterEpoch(t *testing.T) {
	svc := NewService(core.NewCluster(), core.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{snapshot: pdstorage.Snapshot{ClusterEpoch: 7}}
	svc.SetStorage(store)

	_, err := svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor:     testRegionDescriptorProto(testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
		ExpectedClusterEpoch: 6,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)

	_, err = svc.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor:     testRegionDescriptorProto(testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
		ExpectedClusterEpoch: 7,
	})
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, uint64(8), store.snapshot.ClusterEpoch)

	event := rootevent.PeerAdded(11, 2, 201, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	_, err = svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event:                metacodec.RootEventToProto(event),
		ExpectedClusterEpoch: 7,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 1, store.eventCalls)

	_, err = svc.PublishRootEvent(context.Background(), &pdpb.PublishRootEventRequest{
		Event:                metacodec.RootEventToProto(event),
		ExpectedClusterEpoch: 8,
	})
	require.NoError(t, err)
	require.Equal(t, 2, store.eventCalls)
	require.Equal(t, uint64(9), store.snapshot.ClusterEpoch)

	_, err = svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{
		RegionId:             11,
		ExpectedClusterEpoch: 8,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 2, store.eventCalls)

	resp, err := svc.RemoveRegion(context.Background(), &pdpb.RemoveRegionRequest{
		RegionId:             11,
		ExpectedClusterEpoch: 9,
	})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())
	require.Equal(t, 3, store.eventCalls)
	require.Equal(t, uint64(10), store.snapshot.ClusterEpoch)
}

func TestServiceRefreshFromReplicatedRootFollowerServesRead(t *testing.T) {
	peerAddrs := reserveReplicatedRootPeerAddrs(t)
	rootStores := make(map[uint64]*pdstorage.RootStore, 3)
	services := make(map[uint64]*Service, 3)
	for _, id := range []uint64{1, 2, 3} {
		store, err := pdstorage.OpenRootReplicatedStore(pdstorage.ReplicatedRootConfig{
			WorkDir:       filepath.Join(t.TempDir(), "root", "node-"+strconv.FormatUint(id, 10)),
			NodeID:        id,
			TransportAddr: peerAddrs[id],
			PeerAddrs:     peerAddrs,
		})
		require.NoError(t, err)
		rootStores[id] = store
		t.Cleanup(func() { require.NoError(t, store.Close()) })

		cluster := core.NewCluster()
		bootstrap, err := pdstorage.Bootstrap(store, cluster.PublishRegionDescriptor, 1, 1)
		require.NoError(t, err)
		svc := NewService(cluster, core.NewIDAllocator(bootstrap.IDStart), tso.NewAllocator(bootstrap.TSStart))
		svc.SetStorage(store)
		services[id] = svc
	}

	var leaderID uint64
	require.Eventually(t, func() bool {
		for id, store := range rootStores {
			if store.IsLeader() {
				leaderID = id
				return true
			}
		}
		return false
	}, 5*time.Second, 50*time.Millisecond)

	followerID := replicatedFollowerID(leaderID)
	leader := services[leaderID]
	follower := services[followerID]

	_, err := leader.RegionHeartbeat(context.Background(), &pdpb.RegionHeartbeatRequest{
		RegionDescriptor: testRegionDescriptorProto(testDescriptor(91, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)),
	})
	require.NoError(t, err)

	getResp, err := follower.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	require.Eventually(t, func() bool {
		if err := follower.RefreshFromStorage(); err != nil {
			return false
		}
		resp, err := follower.GetRegionByKey(context.Background(), &pdpb.GetRegionByKeyRequest{Key: []byte("m")})
		return err == nil && !resp.GetNotFound() && resp.GetRegionDescriptor().GetRegionId() == 91
	}, 5*time.Second, 50*time.Millisecond)
}

func reserveReplicatedRootPeerAddrs(t *testing.T) map[uint64]string {
	t.Helper()
	out := make(map[uint64]string, 3)
	for _, id := range []uint64{1, 2, 3} {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		out[id] = ln.Addr().String()
		require.NoError(t, ln.Close())
	}
	return out
}

func replicatedFollowerID(leaderID uint64) uint64 {
	for _, id := range []uint64{1, 2, 3} {
		if id != leaderID {
			return id
		}
	}
	return 0
}

func testDescriptor(id uint64, start, end []byte, epoch metaregion.Epoch, peers []metaregion.Peer) descriptor.Descriptor {
	desc := descriptor.Descriptor{
		RegionID: id,
		StartKey: append([]byte(nil), start...),
		EndKey:   append([]byte(nil), end...),
		Epoch:    epoch,
		Peers:    append([]metaregion.Peer(nil), peers...),
		State:    metaregion.ReplicaStateRunning,
	}
	desc.EnsureHash()
	return desc
}
