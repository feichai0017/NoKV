package server

import (
	"context"
	"errors"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	metacodec "github.com/feichai0017/NoKV/meta/codec"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/tso"
)

type fakeStorage struct {
	eventCalls int
	saveCalls  int
	eventErr   error
	saveErr    error
	loadErr    error
	lastID     uint64
	lastTS     uint64
	leader     bool
	leaderID   uint64
	lastEvent  rootevent.Event
	snapshot   coordstorage.Snapshot
}

func (f *fakeStorage) Load() (coordstorage.Snapshot, error) {
	if f.loadErr != nil {
		return coordstorage.Snapshot{}, f.loadErr
	}
	return coordstorage.CloneSnapshot(f.snapshot), nil
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
		Descriptors:         rootCloneDescriptorsForTest(f.snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(f.snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(f.snapshot.PendingRangeChanges),
	}
	rootstate.ApplyEventToSnapshot(&snapshot, snapshot.State.LastCommitted, event)
	f.snapshot = coordstorage.SnapshotFromRoot(snapshot)
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
	snapshot coordstorage.Snapshot
}

func (f *fakeSyncStorage) Load() (coordstorage.Snapshot, error) {
	return coordstorage.CloneSnapshot(f.snapshot), nil
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

func publishDescriptorEvent(t *testing.T, svc *Service, desc descriptor.Descriptor, expected uint64) error {
	t.Helper()
	event := rootevent.RegionBootstrapped(desc)
	if svc != nil && svc.cluster != nil && svc.cluster.HasRegion(desc.RegionID) {
		event = rootevent.RegionDescriptorPublished(desc)
	}
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metacodec.RootEventToProto(event),
		ExpectedClusterEpoch: expected,
	})
	return err
}

func TestServiceStoreHeartbeatAndGetRegionByKey(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	storeResp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   1,
		RegionNum: 3,
		LeaderNum: 1,
		Capacity:  1000,
		Available: 800,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())

	err = publishDescriptorEvent(t, svc, testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}}), 0)
	require.NoError(t, err)

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.NotNil(t, getResp.GetRegionDescriptor())
	require.Equal(t, uint64(11), getResp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, getResp.GetServedFreshness())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_HEALTHY, getResp.GetDegradedMode())
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_FRESH, getResp.GetCatchUpState())
	require.True(t, getResp.GetServedByLeader())
	require.Zero(t, getResp.GetRootLag())
}

func TestServiceGetRegionByKeyStrongReadRejectsFollower(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	storage := &fakeStorage{
		leader:   false,
		leaderID: 7,
		snapshot: coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
	}
	svc.SetStorage(storage)
	require.NoError(t, svc.cluster.PublishRegionDescriptor(testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errNotLeaderPrefix)
}

func TestServiceGetRegionByKeyRequiredRootToken(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	svc.cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		nil,
		nil,
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
	)
	storage := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
			Descriptors: map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		},
	}
	svc.SetStorage(storage)

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key: []byte("a"),
		RequiredRootToken: &coordpb.RootToken{
			Term:     1,
			Index:    10,
			Revision: 10,
		},
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "required rooted token not satisfied")

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key: []byte("a"),
		RequiredRootToken: &coordpb.RootToken{
			Term:     1,
			Index:    1,
			Revision: 1,
		},
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), resp.GetServedRootToken().GetRevision())
	require.Equal(t, uint64(1), resp.GetServedRootToken().GetTerm())
	require.Equal(t, uint64(3), resp.GetServedRootToken().GetIndex())
}

func TestServiceGetRegionByKeyBestEffortWithUnavailableRoot(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	storage := &fakeStorage{
		leader:   true,
		snapshot: coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
	}
	svc.SetStorage(storage)
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	storage.loadErr = errors.New("root unavailable")

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE, resp.GetDegradedMode())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, resp.GetServedFreshness())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_BOUNDED,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRootUnavailable)
}

func TestServiceGetRegionByKeyReportsRootLagging(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{
			11: testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
		},
		nil,
		nil,
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 3},
	)
	storage := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			RootToken: rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 5}, Revision: 7},
			Descriptors: map[uint64]descriptor.Descriptor{
				11: testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
			},
		},
	}
	svc.SetStorage(storage)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())
	require.Equal(t, uint64(4), resp.GetRootLag())
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_LAGGING, resp.GetCatchUpState())
	require.Equal(t, uint64(3), resp.GetServedRootToken().GetRevision())
	require.Equal(t, uint64(7), resp.GetCurrentRootToken().GetRevision())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: 3,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "root lag exceeds bound")

	resp, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: 4,
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "root lag exceeds strong freshness")
}

func TestServiceGetRegionByKeyBoundedRejectsBootstrapRequired(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	desc := testDescriptor(21, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	svc.cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		nil,
		nil,
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 2}, Revision: 2},
	)
	storage := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			RootToken:    rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 9}, Revision: 7},
			CatchUpState: coordstorage.CatchUpStateBootstrapRequired,
			Descriptors:  map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		},
	}
	svc.SetStorage(storage)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED, resp.GetCatchUpState())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: 16,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "bootstrap required before bounded freshness")
}

func TestServiceRemoveRegion(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	resp, err := svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.True(t, getResp.GetNotFound())

	resp, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 11})
	require.NoError(t, err)
	require.False(t, resp.GetRemoved())
}

func TestServiceRegionDescriptorUpdateRejectsStaleAndOverlap(t *testing.T) {
	svc := NewService(catalog.NewCluster(), nil, nil)
	err := publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 2}, nil), 0)
	require.NoError(t, err)

	err = publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	err = publishDescriptorEvent(t, svc, testDescriptor(2, []byte("l"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestServiceAllocIDAndTSO(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(100), tso.NewAllocator(500))

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), idResp.GetFirstId())
	require.Equal(t, uint64(3), idResp.GetCount())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(500), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
}

func TestServiceRequestValidation(t *testing.T) {
	svc := NewService(nil, nil, nil)

	_, err := svc.StoreHeartbeat(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{})
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

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 0})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestServiceRegionLivenessTouchesExistingRegion(t *testing.T) {
	svc := NewService(catalog.NewCluster(), nil, nil)
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	resp, err := svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: 11})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())

	resp, err = svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: 99})
	require.NoError(t, err)
	require.False(t, resp.GetAccepted())
}

func TestServiceStoreHeartbeatReturnsLeaderTransferHint(t *testing.T) {
	svc := NewService(catalog.NewCluster(), nil, nil)
	err := publishDescriptorEvent(t, svc, testDescriptor(100, []byte(""), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}, {StoreID: 2, PeerID: 201}}), 0)
	require.NoError(t, err)

	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   2,
		LeaderNum: 1,
		RegionNum: 1,
	})
	require.NoError(t, err)

	resp, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:   1,
		LeaderNum: 10,
		RegionNum: 1,
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Len(t, resp.GetOperations(), 1)
	op := resp.GetOperations()[0]
	require.Equal(t, coordpb.SchedulerOperationType_SCHEDULER_OPERATION_TYPE_LEADER_TRANSFER, op.GetType())
	require.Equal(t, uint64(100), op.GetRegionId())
	require.Equal(t, uint64(101), op.GetSourcePeerId())
	require.Equal(t, uint64(201), op.GetTargetPeerId())
}

func TestServicePersistsRegionCatalog(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	err := publishDescriptorEvent(t, svc, testDescriptor(42, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, rootevent.KindRegionBootstrap, store.lastEvent.Kind)
	require.Equal(t, uint64(1), store.lastEvent.RegionDescriptor.Descriptor.RootEpoch)

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 42})
	require.NoError(t, err)
	require.Equal(t, 2, store.eventCalls)
	require.Equal(t, rootevent.KindRegionTombstoned, store.lastEvent.Kind)
}

func TestServiceRegionLivenessSkipsTruthPersistence(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	desc := testDescriptor(42, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	err := publishDescriptorEvent(t, svc, desc, 0)
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)

	before, ok := svc.cluster.RegionLastHeartbeat(42)
	require.True(t, ok)
	time.Sleep(10 * time.Millisecond)

	_, err = svc.RegionLiveness(context.Background(), &coordpb.RegionLivenessRequest{RegionId: desc.RegionID})
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
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	event := rootevent.RegionSplitCommitted(
		41,
		[]byte("m"),
		testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
		testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
	)
	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
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

func TestServicePublishRootEventAppliedPeerChangeMarksPendingApplied(t *testing.T) {
	cluster := catalog.NewCluster()
	target := testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 5
	target.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(target))

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch:       5,
			Descriptors:        map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target}},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	applied := rootevent.PeerAdded(target.RegionID, 2, 201, func() descriptor.Descriptor {
		desc := target.Clone()
		desc.RootEpoch = 0
		return desc
	}())
	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(applied),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, uint64(5), store.snapshot.ClusterEpoch)
	require.NotContains(t, store.snapshot.PendingPeerChanges, target.RegionID)
	transitions := svc.cluster.TransitionSnapshot()
	require.NotContains(t, transitions.PendingPeerChanges, target.RegionID)
}

func TestServicePublishRootEventPersistsPeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
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
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 5,
			Descriptors:  map[uint64]descriptor.Descriptor{current.RegionID: current},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, rootevent.KindPeerAdditionPlanned, store.lastEvent.Kind)
	require.Equal(t, uint64(6), store.lastEvent.PeerChange.Region.RootEpoch)
	transitions := svc.cluster.TransitionSnapshot()
	require.Contains(t, transitions.PendingPeerChanges, target.RegionID)
	require.Equal(t, rootstate.PendingPeerChangeAddition, transitions.PendingPeerChanges[target.RegionID].Kind)
}

func TestServicePublishRootEventSkipsDuplicatePeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
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
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
	require.Equal(t, uint64(6), store.snapshot.ClusterEpoch)
	require.Len(t, store.snapshot.PendingPeerChanges, 1)
}

func TestServicePublishRootEventSkipsCompletedPeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	target := testDescriptor(131, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 6
	target.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(target))

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsConflictingPeerPlan(t *testing.T) {
	cluster := catalog.NewCluster()
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
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsMismatchedPeerApply(t *testing.T) {
	cluster := catalog.NewCluster()
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
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
			PendingPeerChanges: map[uint64]rootstate.PendingPeerChange{
				target.RegionID: {Kind: rootstate.PendingPeerChangeAddition, StoreID: 2, PeerID: 201, Target: target},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdded(mismatched.RegionID, 3, 301, mismatched)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventSkipsDuplicateSplitPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 6
	right.RootEpoch = 6
	left.EnsureHash()
	right.EnsureHash()
	require.NoError(t, cluster.PublishRootEvent(rootevent.RegionSplitPlanned(40, []byte("m"), left, right)))

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors: map[uint64]descriptor.Descriptor{
				left.RegionID:  left,
				right.RegionID: right,
			},
			PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
				40: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 40, LeftRegionID: left.RegionID, RightRegionID: right.RegionID, Left: left, Right: right},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionSplitPlanned(40, []byte("m"), left, right)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServiceRefreshFromStorageReplacesPendingView(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(61, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(62, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 9,
			Descriptors: map[uint64]descriptor.Descriptor{
				left.RegionID:  left,
				right.RegionID: right,
			},
			PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
				60: {Kind: rootstate.PendingRangeChangeSplit, ParentRegionID: 60, LeftRegionID: left.RegionID, RightRegionID: right.RegionID, Left: left, Right: right},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	require.NoError(t, svc.RefreshFromStorage())
	transitions := svc.cluster.TransitionSnapshot()
	require.Contains(t, transitions.PendingRangeChanges, uint64(60))
	require.Len(t, svc.cluster.RegionSnapshot(), 2)
}

func TestServiceListTransitionsReturnsOperatorView(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(160, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{target.RegionID: target},
		map[uint64]rootstate.PendingPeerChange{
			target.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    current,
				Target:  target,
			},
		},
		nil,
		rootstorage.TailToken{},
	)

	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	resp, err := svc.ListTransitions(context.Background(), &coordpb.ListTransitionsRequest{})
	require.NoError(t, err)
	require.Len(t, resp.GetEntries(), 1)
	require.Equal(t, coordpb.TransitionKind_TRANSITION_KIND_PEER_CHANGE, resp.GetEntries()[0].GetKind())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_PENDING, resp.GetEntries()[0].GetStatus())
	require.NotNil(t, resp.GetEntries()[0].GetPendingPeerChange())
}

func TestServiceAssessRootEventReturnsConflictAssessment(t *testing.T) {
	cluster := catalog.NewCluster()
	current := testDescriptor(161, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
	})
	current.RootEpoch = 5
	current.EnsureHash()

	target := current.Clone()
	target.Peers = append(target.Peers, metaregion.Peer{StoreID: 2, PeerID: 201})
	target.Epoch.ConfVersion++
	target.RootEpoch = 6
	target.EnsureHash()

	cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{target.RegionID: target},
		map[uint64]rootstate.PendingPeerChange{
			target.RegionID: {
				Kind:    rootstate.PendingPeerChangeAddition,
				StoreID: 2,
				PeerID:  201,
				Base:    current,
				Target:  target,
			},
		},
		nil,
		rootstorage.TailToken{},
	)

	conflicting := current.Clone()
	conflicting.Peers = append(conflicting.Peers, metaregion.Peer{StoreID: 3, PeerID: 301})
	conflicting.Epoch.ConfVersion++
	conflicting.RootEpoch = 0
	conflicting.EnsureHash()

	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	resp, err := svc.AssessRootEvent(context.Background(), &coordpb.AssessRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_CONFLICT, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_CONFLICT, resp.GetAssessment().GetRetryClass())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, resp.GetAssessment().GetDecision())
}

func TestServicePublishRootEventSkipsCompletedSplitPlan(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(141, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(142, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 6
	right.RootEpoch = 6
	left.EnsureHash()
	right.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(left))
	require.NoError(t, cluster.PublishRegionDescriptor(right))

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors: map[uint64]descriptor.Descriptor{
				left.RegionID:  left,
				right.RegionID: right,
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionSplitPlanned(140, []byte("m"), left, right)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventSkipsCompletedMergePlan(t *testing.T) {
	cluster := catalog.NewCluster()
	merged := testDescriptor(151, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil)
	merged.RootEpoch = 7
	merged.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(merged))

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 7,
			Descriptors: map[uint64]descriptor.Descriptor{
				merged.RegionID: merged,
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionMergePlanned(149, 150, merged)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventRejectsMismatchedMergeApply(t *testing.T) {
	cluster := catalog.NewCluster()
	merged := testDescriptor(50, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil)
	merged.RootEpoch = 7
	merged.EnsureHash()
	require.NoError(t, cluster.PublishRegionDescriptor(merged))

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 7,
			Descriptors:  map[uint64]descriptor.Descriptor{merged.RegionID: merged},
			PendingRangeChanges: map[uint64]rootstate.PendingRangeChange{
				merged.RegionID: {Kind: rootstate.PendingRangeChangeMerge, LeftRegionID: 50, RightRegionID: 51, Merged: merged},
			},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	svc.SetStorage(store)

	mismatched := merged.Clone()
	mismatched.RootEpoch = 0
	mismatched.EnsureHash()
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionMerged(50, 52, mismatched)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)
}

func TestServicePublishRootEventValidationAndPersistenceError(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.PublishRootEvent(context.Background(), nil)
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{})
	require.Error(t, err)
	require.Equal(t, codes.InvalidArgument, status.Code(err))

	store := &fakeStorage{eventErr: errors.New("persist root event failed")}
	svc.SetStorage(store)
	event := rootevent.RegionMerged(
		10,
		11,
		testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
	)
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(event),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("m"))
	require.False(t, ok)
}

func TestServiceRegionCatalogPersistenceErrors(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{eventErr: errors.New("persist update failed")}
	svc.SetStorage(store)

	err := publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("b"))
	require.False(t, ok)

	store.eventErr = nil
	err = publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	store.eventErr = errors.New("persist delete failed")
	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	resp, lookupErr := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, lookupErr)
	require.False(t, resp.GetNotFound())
}

func TestServicePersistsAllocatorState(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	store := &fakeStorage{}
	svc.SetStorage(store)

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(11), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(11), store.lastID)
	require.Equal(t, uint64(102), store.lastTS)
}

func TestServiceAllocatorStatePersistenceError(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{saveErr: errors.New("persist failed")}
	svc.SetStorage(store)

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.saveErr = nil
	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), idResp.GetFirstId())

	store.saveErr = errors.New("persist failed")
	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))

	store.saveErr = nil
	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(1), tsResp.GetTimestamp())
}

func TestServiceRejectsWritesOnFollower(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	store := &fakeStorage{leader: false, leaderID: 2}
	svc.SetStorage(store)

	err := publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, strings.Contains(err.Error(), errNotLeaderPrefix))

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(rootevent.RegionTombstoned(8)),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{RegionId: 8})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))

	_, err = svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{StoreId: 1})
	require.NoError(t, err)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
}

func TestServiceRefreshFromStorageReloadsViewAndAllocatorState(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeSyncStorage{
		fakeStorage: fakeStorage{leader: false, leaderID: 2},
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 4,
			Descriptors: map[uint64]descriptor.Descriptor{
				9: testDescriptor(9, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
			},
			Allocator: coordstorage.AllocatorState{
				IDCurrent: 120,
				TSCurrent: 450,
			},
		},
	}
	svc.SetStorage(store)

	require.NoError(t, svc.RefreshFromStorage())

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("m")})
	require.NoError(t, err)
	require.False(t, getResp.GetNotFound())
	require.Equal(t, uint64(9), getResp.GetRegionDescriptor().GetRegionId())

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Nil(t, idResp)

	store.leader = true
	store.leaderID = 0

	idResp, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(121), idResp.GetFirstId())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(451), tsResp.GetTimestamp())
}

func TestServicePublishRootEventAssignsRootEpoch(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{}
	svc.SetStorage(store)

	err := publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	event := rootevent.PeerAdded(1, 2, 201, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	event.PeerChange.Region.RootEpoch = 0
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metacodec.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.Equal(t, rootevent.KindPeerAdded, store.lastEvent.Kind)
	require.Equal(t, uint64(2), store.lastEvent.PeerChange.Region.RootEpoch)
	require.Equal(t, uint64(2), store.snapshot.ClusterEpoch)
}

func TestServiceMutatingWritesRespectExpectedClusterEpoch(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))
	store := &fakeStorage{snapshot: coordstorage.Snapshot{ClusterEpoch: 7}}
	svc.SetStorage(store)

	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 6)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 0, store.eventCalls)

	err = publishDescriptorEvent(t, svc, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 7)
	require.NoError(t, err)
	require.Equal(t, 1, store.eventCalls)
	require.Equal(t, uint64(8), store.snapshot.ClusterEpoch)

	event := rootevent.PeerAdded(11, 2, 201, testDescriptor(11, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metacodec.RootEventToProto(event),
		ExpectedClusterEpoch: 7,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 1, store.eventCalls)

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metacodec.RootEventToProto(event),
		ExpectedClusterEpoch: 8,
	})
	require.NoError(t, err)
	require.Equal(t, 2, store.eventCalls)
	require.Equal(t, uint64(9), store.snapshot.ClusterEpoch)

	_, err = svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{
		RegionId:             11,
		ExpectedClusterEpoch: 8,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 2, store.eventCalls)

	resp, err := svc.RemoveRegion(context.Background(), &coordpb.RemoveRegionRequest{
		RegionId:             11,
		ExpectedClusterEpoch: 9,
	})
	require.NoError(t, err)
	require.True(t, resp.GetRemoved())
	require.Equal(t, 3, store.eventCalls)
	require.Equal(t, uint64(10), store.snapshot.ClusterEpoch)
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
