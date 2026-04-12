package server

import (
	"context"
	"errors"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"github.com/feichai0017/NoKV/raftstore/descriptor"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	"github.com/feichai0017/NoKV/coordinator/idalloc"
	"github.com/feichai0017/NoKV/coordinator/tso"
)

type fakeStorage struct {
	eventCalls    int
	saveCalls     int
	campaignCalls int
	releaseCalls  int
	eventErr      error
	saveErr       error
	loadErr       error
	campaignErr   error
	releaseErr    error
	lastID        uint64
	lastTS        uint64
	leader        bool
	leaderID      uint64
	lastEvent     rootevent.Event
	snapshot      coordstorage.Snapshot
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
			ClusterEpoch:     f.snapshot.ClusterEpoch,
			IDFence:          f.snapshot.Allocator.IDCurrent,
			TSOFence:         f.snapshot.Allocator.TSCurrent,
			CoordinatorLease: f.snapshot.CoordinatorLease,
			LastCommitted:    rootstate.Cursor{Term: 1, Index: uint64(f.eventCalls)},
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
	if f.saveErr != nil {
		return f.saveErr
	}
	if idCurrent > f.snapshot.Allocator.IDCurrent {
		f.snapshot.Allocator.IDCurrent = idCurrent
	}
	if tsCurrent > f.snapshot.Allocator.TSCurrent {
		f.snapshot.Allocator.TSCurrent = tsCurrent
	}
	return nil
}

func (f *fakeStorage) CampaignCoordinatorLease(holderID string, expiresUnixNano, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	f.campaignCalls++
	if f.campaignErr != nil {
		return rootstate.CoordinatorLease{}, f.campaignErr
	}
	if err := rootstate.ValidateCoordinatorLeaseCampaign(f.snapshot.CoordinatorLease, holderID, expiresUnixNano, nowUnixNano); err != nil {
		return f.snapshot.CoordinatorLease, err
	}
	f.snapshot.CoordinatorLease = rootstate.CoordinatorLease{
		HolderID:        holderID,
		ExpiresUnixNano: expiresUnixNano,
		IDFence:         idFence,
		TSOFence:        tsoFence,
	}
	if idFence > f.snapshot.Allocator.IDCurrent {
		f.snapshot.Allocator.IDCurrent = idFence
	}
	if tsoFence > f.snapshot.Allocator.TSCurrent {
		f.snapshot.Allocator.TSCurrent = tsoFence
	}
	return f.snapshot.CoordinatorLease, nil
}

func (f *fakeStorage) ReleaseCoordinatorLease(holderID string, nowUnixNano int64, idFence, tsoFence uint64) (rootstate.CoordinatorLease, error) {
	f.releaseCalls++
	if f.releaseErr != nil {
		return rootstate.CoordinatorLease{}, f.releaseErr
	}
	if err := rootstate.ValidateCoordinatorLeaseRelease(f.snapshot.CoordinatorLease, holderID, nowUnixNano); err != nil {
		return f.snapshot.CoordinatorLease, err
	}
	f.snapshot.CoordinatorLease = rootstate.CoordinatorLease{
		HolderID:        holderID,
		ExpiresUnixNano: nowUnixNano,
		IDFence:         idFence,
		TSOFence:        tsoFence,
	}
	if idFence > f.snapshot.Allocator.IDCurrent {
		f.snapshot.Allocator.IDCurrent = idFence
	}
	if tsoFence > f.snapshot.Allocator.TSCurrent {
		f.snapshot.Allocator.TSCurrent = tsoFence
	}
	return f.snapshot.CoordinatorLease, nil
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

type serialAppendStorage struct {
	fakeStorage
	inAppend int32
	entered  chan struct{}
	release  chan struct{}
}

func (f *fakeSyncStorage) Load() (coordstorage.Snapshot, error) {
	return coordstorage.CloneSnapshot(f.snapshot), nil
}

func (f *fakeSyncStorage) Refresh() error {
	return nil
}

func (f *serialAppendStorage) AppendRootEvent(event rootevent.Event) error {
	if !atomic.CompareAndSwapInt32(&f.inAppend, 0, 1) {
		return errors.New("concurrent append")
	}
	if f.entered != nil {
		select {
		case f.entered <- struct{}{}:
		default:
		}
	}
	if f.release != nil {
		<-f.release
	}
	defer atomic.StoreInt32(&f.inAppend, 0)
	return f.fakeStorage.AppendRootEvent(event)
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
		Event:                metawire.RootEventToProto(event),
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

func TestServiceDiagnosticsSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	storage := &fakeStorage{
		leader:   true,
		leaderID: 7,
		snapshot: coordstorage.Snapshot{
			RootToken: rootstorage.TailToken{
				Cursor:   rootstate.Cursor{Term: 2, Index: 9},
				Revision: 4,
			},
			CatchUpState: coordstorage.CatchUpStateLagging,
			Allocator: coordstorage.AllocatorState{
				IDCurrent: 55,
				TSCurrent: 88,
			},
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:        "c1",
				ExpiresUnixNano: now.Add(5 * time.Second).UnixNano(),
				IDFence:         60,
				TSOFence:        90,
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				11: testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}}),
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(50), tso.NewAllocator(80), storage)
	svc.now = func() time.Time { return now }
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ReloadFromStorage())

	snapshot := svc.DiagnosticsSnapshot()
	allocator := snapshot["allocator"].(map[string]any)
	root := snapshot["root"].(map[string]any)
	lease := snapshot["lease"].(map[string]any)

	require.Equal(t, uint64(55), allocator["id_current"])
	require.Equal(t, uint64(88), allocator["tso_current"])
	require.Equal(t, true, root["configured"])
	require.Equal(t, "CATCH_UP_STATE_FRESH", root["catch_up_state"])
	require.Equal(t, "DEGRADED_MODE_HEALTHY", root["degraded_mode"])
	require.Equal(t, uint64(7), root["storage_leader_id"])
	require.NotZero(t, root["last_reload_unix_nano"])
	require.Equal(t, true, lease["enabled"])
	require.Equal(t, "c1", lease["holder_id"])
	require.Equal(t, true, lease["active"])
	require.Equal(t, true, lease["held_by_self"])
	require.Equal(t, true, lease["usable_by_self"])
}

func TestServiceGetRegionByKeyStrongReadRejectsFollower(t *testing.T) {
	storage := &fakeStorage{
		leader:   false,
		leaderID: 7,
		snapshot: coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
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
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	cluster.ReplaceRootSnapshot(
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

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
	storage := &fakeStorage{
		leader:   true,
		snapshot: coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
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
	cluster := catalog.NewCluster()
	cluster.ReplaceRootSnapshot(
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

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
		MaxRootLag: proto.Uint64(3),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "root lag exceeds bound")

	resp, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: proto.Uint64(4),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())

	resp, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_BOUNDED,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(4), resp.GetRootLag())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("a"),
		Freshness: coordpb.Freshness_FRESHNESS_STRONG,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "root lag exceeds strong freshness")
}

func TestServiceGetRegionByKeyBoundedRejectsBootstrapRequired(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(21, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	cluster.ReplaceRootSnapshot(
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED, resp.GetCatchUpState())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:        []byte("a"),
		Freshness:  coordpb.Freshness_FRESHNESS_BOUNDED,
		MaxRootLag: proto.Uint64(16),
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "bootstrap required before bounded freshness")
}

func TestRootLagOnlyCountsServedBehindCurrent(t *testing.T) {
	require.Equal(t, uint64(1), rootLag(
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 7},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 8}, Revision: 7},
	))
	require.Equal(t, uint64(0), rootLag(
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 8}, Revision: 7},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 7},
	))
	require.Equal(t, uint64(0), rootLag(
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 8}, Revision: 6},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 9}, Revision: 7},
	))
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
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	event := rootevent.RegionSplitCommitted(
		41,
		[]byte("m"),
		testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil),
		testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil),
	)
	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	applied := rootevent.PeerAdded(target.RegionID, 2, 201, func() descriptor.Descriptor {
		desc := target.Clone()
		desc.RootEpoch = 0
		return desc
	}())
	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(applied),
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.NotNil(t, resp.GetAssessment())
	require.Equal(t, "peer:12:add:2:201", resp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_OPEN, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, resp.GetAssessment().GetDecision())
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.NotNil(t, resp.GetAssessment())
	require.Equal(t, "peer:13:add:2:201", resp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_PENDING, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_SKIP, resp.GetAssessment().GetDecision())
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.NotNil(t, resp.GetAssessment())
	require.Equal(t, "peer:131:add:2:201", resp.GetAssessment().GetTransitionId())
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_COMPLETED, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_SKIP, resp.GetAssessment().GetDecision())
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdded(mismatched.RegionID, 3, 301, mismatched)),
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionSplitPlanned(40, []byte("m"), left, right)),
	})
	require.NoError(t, err)
	require.True(t, resp.GetAccepted())
	require.Equal(t, 0, store.eventCalls)
}

func TestServiceRefreshFromStorageReplacesPendingTransitions(t *testing.T) {
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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
	require.Equal(t, "peer:160:add:2:201", resp.GetEntries()[0].GetTransitionId())
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
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(conflicting.RegionID, 3, 301, conflicting)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_CONFLICT, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionRetryClass_TRANSITION_RETRY_CLASS_CONFLICT, resp.GetAssessment().GetRetryClass())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_APPLY, resp.GetAssessment().GetDecision())
	require.Equal(t, "peer:161:add:3:301", resp.GetAssessment().GetTransitionId())
}

func TestServiceAssessRootEventUsesStorageSnapshot(t *testing.T) {
	cluster := catalog.NewCluster()
	target := testDescriptor(171, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, []metaregion.Peer{
		{StoreID: 1, PeerID: 101},
		{StoreID: 2, PeerID: 201},
	})
	target.RootEpoch = 6
	target.EnsureHash()

	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			ClusterEpoch: 6,
			Descriptors:  map[uint64]descriptor.Descriptor{target.RegionID: target},
		},
	}

	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.AssessRootEvent(context.Background(), &coordpb.AssessRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.PeerAdditionPlanned(target.RegionID, 2, 201, target)),
	})
	require.NoError(t, err)
	require.Equal(t, coordpb.TransitionStatus_TRANSITION_STATUS_COMPLETED, resp.GetAssessment().GetStatus())
	require.Equal(t, coordpb.TransitionDecision_TRANSITION_DECISION_SKIP, resp.GetAssessment().GetDecision())
	require.Equal(t, "peer:171:add:2:201", resp.GetAssessment().GetTransitionId())
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionSplitPlanned(140, []byte("m"), left, right)),
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	resp, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionMergePlanned(149, 150, merged)),
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
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	mismatched := merged.Clone()
	mismatched.RootEpoch = 0
	mismatched.EnsureHash()
	_, err := svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionMerged(50, 52, mismatched)),
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
	svc = NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)
	event := rootevent.RegionMerged(
		10,
		11,
		testDescriptor(10, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
	)
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.Error(t, err)
	require.Equal(t, codes.Internal, status.Code(err))
	_, ok := svc.cluster.GetRegionDescriptorByKey([]byte("m"))
	require.False(t, ok)
}

func TestServicePublishRootEventSerializesStorageAppend(t *testing.T) {
	store := &serialAppendStorage{
		fakeStorage: fakeStorage{snapshot: coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}},
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	req1 := &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(testDescriptor(
			41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		))),
	}
	req2 := &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(testDescriptor(
			42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 102}},
		))),
	}

	errCh := make(chan error, 2)
	go func() {
		_, err := svc.PublishRootEvent(context.Background(), req1)
		errCh <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("first append did not start")
	}

	go func() {
		_, err := svc.PublishRootEvent(context.Background(), req2)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		t.Fatalf("second publish finished before first append released: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-errCh)
	require.NoError(t, <-errCh)
	require.Equal(t, 2, store.eventCalls)
}

func TestServiceRefreshFromStorageSerializesWithWrites(t *testing.T) {
	store := &serialAppendStorage{
		fakeStorage: fakeStorage{snapshot: coordstorage.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}},
		entered:     make(chan struct{}, 1),
		release:     make(chan struct{}),
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	req := &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionBootstrapped(testDescriptor(
			41, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}},
		))),
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := svc.PublishRootEvent(context.Background(), req)
		errCh <- err
	}()
	select {
	case <-store.entered:
	case <-time.After(time.Second):
		t.Fatal("append did not start")
	}

	refreshDone := make(chan error, 1)
	go func() {
		refreshDone <- svc.RefreshFromStorage()
	}()
	select {
	case err := <-refreshDone:
		t.Fatalf("refresh completed while write was in progress: %v", err)
	case <-time.After(50 * time.Millisecond):
	}

	close(store.release)
	require.NoError(t, <-errCh)
	require.NoError(t, <-refreshDone)
}

func TestServiceRegionCatalogPersistenceErrors(t *testing.T) {
	store := &fakeStorage{eventErr: errors.New("persist update failed")}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(10009), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(10009), store.lastID)
	require.Equal(t, uint64(10099), store.lastTS)
}

func TestServiceIDWindowPersistsFenceOncePerWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.idWindowSize = 5

	first, err := svc.reserveIDs(3)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)
	require.Equal(t, uint64(12), svc.ids.Current())

	first, err = svc.reserveIDs(2)
	require.NoError(t, err)
	require.Equal(t, uint64(13), first)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(14), svc.ids.Current())

	first, err = svc.reserveIDs(1)
	require.NoError(t, err)
	require.Equal(t, uint64(15), first)
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(19), store.lastID)
	require.Equal(t, uint64(15), svc.ids.Current())
}

func TestServiceTSOWindowPersistsFenceOncePerWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.tsoWindowSize = 5

	first, count, err := svc.reserveTSO(3)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(3), count)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(102), svc.tso.Current())

	first, count, err = svc.reserveTSO(2)
	require.NoError(t, err)
	require.Equal(t, uint64(103), first)
	require.Equal(t, uint64(2), count)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(104), svc.tso.Current())

	first, count, err = svc.reserveTSO(1)
	require.NoError(t, err)
	require.Equal(t, uint64(105), first)
	require.Equal(t, uint64(1), count)
	require.Equal(t, 2, store.saveCalls)
	require.Equal(t, uint64(109), store.lastTS)
	require.Equal(t, uint64(105), svc.tso.Current())
}

func TestServiceReloadDoesNotConsumeActiveIDWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.idWindowSize = 5

	first, err := svc.reserveIDs(2)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(11), svc.ids.Current())

	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint64(11), svc.ids.Current())

	first, err = svc.reserveIDs(1)
	require.NoError(t, err)
	require.Equal(t, uint64(12), first)
	require.Equal(t, 1, store.saveCalls)
}

func TestServiceReloadDoesNotConsumeActiveTSOWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.tsoWindowSize = 5

	first, _, err := svc.reserveTSO(2)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(101), svc.tso.Current())

	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint64(101), svc.tso.Current())

	first, _, err = svc.reserveTSO(1)
	require.NoError(t, err)
	require.Equal(t, uint64(102), first)
	require.Equal(t, 1, store.saveCalls)
}

func TestServiceAllocatorStatePersistenceError(t *testing.T) {
	store := &fakeStorage{saveErr: errors.New("persist failed")}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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

func TestServiceCoordinatorLeaseReusedAcrossAllocatorRequests(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, 1, store.campaignCalls)
	require.Equal(t, "c1", store.snapshot.CoordinatorLease.HolderID)

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, 1, store.campaignCalls)
}

func TestServiceCoordinatorLeaseRenewsInsideRenewWindow(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 100*time.Millisecond, 20*time.Millisecond)

	now := time.Unix(0, 0)
	svc.now = func() time.Time { return now }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 1, store.campaignCalls)

	now = now.Add(85 * time.Millisecond)
	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 2, store.campaignCalls)
}

func TestServiceCoordinatorLeaseStopsBeforeExpiryByClockSkew(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 100*time.Millisecond, 20*time.Millisecond)
	svc.leaseClockSkew = 40 * time.Millisecond

	now := time.Unix(0, 0)
	svc.now = func() time.Time { return now }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 1, store.campaignCalls)

	now = now.Add(65 * time.Millisecond)
	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 2, store.campaignCalls)
}

func TestServiceCoordinatorLeaseLoopRenewsInBackground(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 80*time.Millisecond, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunCoordinatorLeaseLoop(ctx)

	require.Eventually(t, func() bool {
		return store.campaignCalls >= 2
	}, 300*time.Millisecond, 10*time.Millisecond)
}

func TestServiceCoordinatorLeaseLoopSkipsFollower(t *testing.T) {
	store := &fakeStorage{leader: false, leaderID: 2}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 80*time.Millisecond, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go svc.RunCoordinatorLeaseLoop(ctx)

	time.Sleep(80 * time.Millisecond)
	require.Equal(t, 0, store.campaignCalls)
}

func TestServiceReleaseCoordinatorLease(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReleaseCoordinatorLease())
	require.Equal(t, 1, store.releaseCalls)
	require.Equal(t, int64(200), store.snapshot.CoordinatorLease.ExpiresUnixNano)
	require.False(t, store.snapshot.CoordinatorLease.ActiveAt(200))
}

func TestServiceCoordinatorLeaseRejectsOtherHolder(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:        "c2",
				ExpiresUnixNano: 10_000,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errCoordinatorLeasePrefix)

	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errCoordinatorLeasePrefix)
}

func TestServiceStoreHeartbeatSuppressesOperationsWithoutCoordinatorLease(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: coordstorage.Snapshot{
			CoordinatorLease: rootstate.CoordinatorLease{
				HolderID:        "other",
				ExpiresUnixNano: 10_000,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), nil, nil, store)
	svc.ConfigureCoordinatorLease("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

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
	require.Empty(t, resp.GetOperations())
}

func TestServiceRejectsWritesOnFollower(t *testing.T) {
	store := &fakeStorage{leader: false, leaderID: 2}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(8, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.True(t, strings.Contains(err.Error(), errNotLeaderPrefix))

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(rootevent.RegionTombstoned(8)),
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
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

	err := publishDescriptorEvent(t, svc, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)

	event := rootevent.PeerAdded(1, 2, 201, testDescriptor(1, []byte("a"), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 2}, nil))
	event.PeerChange.Region.RootEpoch = 0
	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event: metawire.RootEventToProto(event),
	})
	require.NoError(t, err)
	require.Equal(t, rootevent.KindPeerAdded, store.lastEvent.Kind)
	require.Equal(t, uint64(2), store.lastEvent.PeerChange.Region.RootEpoch)
	require.Equal(t, uint64(2), store.snapshot.ClusterEpoch)
}

func TestServiceMutatingWritesRespectExpectedClusterEpoch(t *testing.T) {
	store := &fakeStorage{snapshot: coordstorage.Snapshot{ClusterEpoch: 7}}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), store)

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
		Event:                metawire.RootEventToProto(event),
		ExpectedClusterEpoch: 7,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Equal(t, 1, store.eventCalls)

	_, err = svc.PublishRootEvent(context.Background(), &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(event),
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
