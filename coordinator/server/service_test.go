package server

import (
	"context"
	"errors"
	coordablation "github.com/feichai0017/NoKV/coordinator/ablation"
	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	"github.com/feichai0017/NoKV/coordinator/rootview"
	metaregion "github.com/feichai0017/NoKV/meta/region"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
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
	loadCalls     int
	campaignCalls int
	releaseCalls  int
	sealCalls     int
	confirmCalls  int
	closeCalls    int
	reattachCalls int
	eventErr      error
	saveErr       error
	loadErr       error
	campaignErr   error
	releaseErr    error
	sealErr       error
	confirmErr    error
	closeErr      error
	reattachErr   error
	lastID        uint64
	lastTS        uint64
	leader        bool
	leaderID      uint64
	lastEvent     rootevent.Event
	snapshot      rootview.Snapshot
}

func (f *fakeStorage) protocolState() rootstate.SuccessionState {
	return rootstate.SuccessionState{
		Tenure:   f.snapshot.Tenure,
		Legacy:   f.snapshot.Legacy,
		Handover: f.snapshot.Handover,
	}
}

func (f *fakeStorage) Load() (rootview.Snapshot, error) {
	f.loadCalls++
	if f.loadErr != nil {
		return rootview.Snapshot{}, f.loadErr
	}
	return rootview.CloneSnapshot(f.snapshot), nil
}

func (f *fakeStorage) AppendRootEvent(_ context.Context, event rootevent.Event) error {
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
			Tenure:        f.snapshot.Tenure,
			LastCommitted: rootstate.Cursor{Term: 1, Index: uint64(f.eventCalls)},
		},
		Descriptors:         rootCloneDescriptorsForTest(f.snapshot.Descriptors),
		PendingPeerChanges:  rootstate.ClonePendingPeerChanges(f.snapshot.PendingPeerChanges),
		PendingRangeChanges: rootstate.ClonePendingRangeChanges(f.snapshot.PendingRangeChanges),
	}
	rootstate.ApplyEventToSnapshot(&snapshot, snapshot.State.LastCommitted, event)
	f.snapshot = rootview.SnapshotFromRoot(snapshot)
	return nil
}

func (f *fakeStorage) SaveAllocatorState(_ context.Context, idCurrent, tsCurrent uint64) error {
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

func (f *fakeStorage) ApplyTenure(_ context.Context, cmd rootproto.TenureCommand) (rootstate.SuccessionState, error) {
	switch cmd.Kind {
	case rootproto.TenureActIssue:
		f.campaignCalls++
		if f.campaignErr != nil {
			return rootstate.SuccessionState{}, f.campaignErr
		}
		if err := rootstate.ValidateTenureClaim(f.snapshot.Tenure, f.snapshot.Legacy, cmd.HolderID, cmd.LineageDigest, cmd.ExpiresUnixNano, cmd.NowUnixNano); err != nil {
			return f.protocolState(), err
		}
		if err := rootstate.ValidateInheritance(f.snapshot.Tenure, f.snapshot.Legacy, cmd.InheritedFrontiers); err != nil {
			return f.protocolState(), err
		}
		generation := rootstate.NextTenureEpoch(f.snapshot.Tenure, f.snapshot.Legacy, cmd.HolderID, cmd.NowUnixNano)
		f.snapshot.Tenure = rootstate.Tenure{
			HolderID:        cmd.HolderID,
			ExpiresUnixNano: cmd.ExpiresUnixNano,
			Epoch:           generation,
			Mandate:         rootproto.MandateDefault,
			LineageDigest:   cmd.LineageDigest,
		}
		if idFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateAllocID); idFence > f.snapshot.Allocator.IDCurrent {
			f.snapshot.Allocator.IDCurrent = idFence
		}
		if tsoFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateTSO); tsoFence > f.snapshot.Allocator.TSCurrent {
			f.snapshot.Allocator.TSCurrent = tsoFence
		}
	case rootproto.TenureActRelease:
		f.releaseCalls++
		if f.releaseErr != nil {
			return rootstate.SuccessionState{}, f.releaseErr
		}
		if err := rootstate.ValidateTenureYield(f.snapshot.Tenure, cmd.HolderID, cmd.NowUnixNano); err != nil {
			return f.protocolState(), err
		}
		f.snapshot.Tenure = rootstate.Tenure{
			HolderID:        cmd.HolderID,
			ExpiresUnixNano: cmd.NowUnixNano,
			Epoch:           f.snapshot.Tenure.Epoch,
			IssuedAt:        f.snapshot.Tenure.IssuedAt,
			Mandate:         f.snapshot.Tenure.Mandate,
		}
		if idFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateAllocID); idFence > f.snapshot.Allocator.IDCurrent {
			f.snapshot.Allocator.IDCurrent = idFence
		}
		if tsoFence := cmd.InheritedFrontiers.Frontier(rootproto.MandateTSO); tsoFence > f.snapshot.Allocator.TSCurrent {
			f.snapshot.Allocator.TSCurrent = tsoFence
		}
	default:
		return rootstate.SuccessionState{}, rootstate.ErrInvalidTenure
	}
	return f.protocolState(), nil
}

func (f *fakeStorage) ApplyHandover(_ context.Context, cmd rootproto.HandoverCommand) (rootstate.SuccessionState, error) {
	switch cmd.Kind {
	case rootproto.HandoverActSeal:
		f.sealCalls++
		if f.sealErr != nil {
			return rootstate.SuccessionState{}, f.sealErr
		}
		if err := rootstate.ValidateLegacyFormation(f.snapshot.Tenure, cmd.HolderID); err != nil {
			return f.protocolState(), err
		}
		mandate := f.snapshot.Tenure.Mandate
		if mandate == 0 {
			mandate = rootproto.MandateDefault
		}
		f.snapshot.Legacy = rootstate.Legacy{
			HolderID:  cmd.HolderID,
			Epoch:     f.snapshot.Tenure.Epoch,
			Mandate:   mandate,
			Frontiers: cmd.Frontiers,
		}
	case rootproto.HandoverActConfirm:
		f.confirmCalls++
		if f.confirmErr != nil {
			return rootstate.SuccessionState{}, f.confirmErr
		}
		if strings.TrimSpace(cmd.HolderID) == "" || strings.TrimSpace(cmd.HolderID) != f.snapshot.Tenure.HolderID {
			return f.protocolState(), rootstate.ErrPrimacy
		}
		auditStatus, err := succession.ValidateHandoverConfirmation(
			f.snapshot.Tenure,
			succession.Frontiers(rootstate.State{
				IDFence:  f.snapshot.Allocator.IDCurrent,
				TSOFence: f.snapshot.Allocator.TSCurrent,
			}, rootstate.MaxDescriptorRevision(f.snapshot.Descriptors)),
			f.snapshot.Legacy,
			cmd.NowUnixNano,
		)
		if err != nil {
			return f.protocolState(), err
		}
		f.snapshot.Handover = rootstate.Handover{
			HolderID:       cmd.HolderID,
			LegacyEpoch:    auditStatus.LegacyEpoch,
			SuccessorEpoch: f.snapshot.Tenure.Epoch,
			LegacyDigest:   auditStatus.LegacyDigest,
			Stage:          rootproto.HandoverStageConfirmed,
		}
	case rootproto.HandoverActClose:
		f.closeCalls++
		if f.closeErr != nil {
			return rootstate.SuccessionState{}, f.closeErr
		}
		if err := succession.ValidateHandoverFinality(f.snapshot.Tenure, f.snapshot.Handover, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return f.protocolState(), err
		}
		f.snapshot.Handover.Stage = rootproto.HandoverStageClosed
	case rootproto.HandoverActReattach:
		f.reattachCalls++
		if f.reattachErr != nil {
			return rootstate.SuccessionState{}, f.reattachErr
		}
		if err := succession.ValidateHandoverReattach(f.snapshot.Tenure, f.snapshot.Handover, strings.TrimSpace(cmd.HolderID), cmd.NowUnixNano); err != nil {
			return f.protocolState(), err
		}
		f.snapshot.Handover.Stage = rootproto.HandoverStageReattached
	default:
		return rootstate.SuccessionState{}, rootstate.ErrFinality
	}
	return f.protocolState(), nil
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
	snapshot rootview.Snapshot
}

type serialAppendStorage struct {
	fakeStorage
	inAppend int32
	entered  chan struct{}
	release  chan struct{}
}

func (f *fakeSyncStorage) Load() (rootview.Snapshot, error) {
	return rootview.CloneSnapshot(f.snapshot), nil
}

func (f *fakeSyncStorage) Refresh() error {
	return nil
}

func (f *serialAppendStorage) AppendRootEvent(ctx context.Context, event rootevent.Event) error {
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
	return f.fakeStorage.AppendRootEvent(ctx, event)
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
		StoreId:           1,
		RegionNum:         3,
		LeaderNum:         1,
		Capacity:          1000,
		Available:         800,
		DroppedOperations: 5,
	})
	require.NoError(t, err)
	require.True(t, storeResp.GetAccepted())
	stores := svc.cluster.StoreSnapshot()
	require.Len(t, stores, 1)
	require.Equal(t, uint64(5), stores[0].DroppedOperations)

	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}})
	desc.RootEpoch = 1
	err = publishDescriptorEvent(t, svc, desc, 0)
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
	require.Equal(t, uint64(1), getResp.GetDescriptorRevision())
	require.Zero(t, getResp.GetRequiredDescriptorRevision())
}

func TestServiceDiagnosticsSnapshot(t *testing.T) {
	now := time.Unix(100, 0)
	storage := &fakeStorage{
		leader:   true,
		leaderID: 7,
		snapshot: rootview.Snapshot{
			RootToken: rootstorage.TailToken{
				Cursor:   rootstate.Cursor{Term: 2, Index: 9},
				Revision: 4,
			},
			CatchUpState: rootview.CatchUpStateLagging,
			Allocator: rootview.AllocatorState{
				IDCurrent: 55,
				TSCurrent: 88,
			},
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: now.Add(5 * time.Second).UnixNano(),
				Epoch:           3,
				IssuedAt:        rootstate.Cursor{Term: 2, Index: 9},
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 44, TSOFence: 77}, 5),
				SealedAt:  rootstate.Cursor{Term: 2, Index: 8},
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				11: testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, []metaregion.Peer{{StoreID: 1, PeerID: 101}}),
			},
		},
	}
	storage.snapshot.Tenure.LineageDigest = rootstate.DigestOfLegacy(storage.snapshot.Legacy)
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(50), tso.NewAllocator(80), storage)
	svc.now = func() time.Time { return now }
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ReloadFromStorage())
	_, err := svc.StoreHeartbeat(context.Background(), &coordpb.StoreHeartbeatRequest{
		StoreId:         1,
		RegionNum:       1,
		LeaderNum:       1,
		LeaderRegionIds: []uint64{11},
	})
	require.NoError(t, err)

	snapshot := svc.DiagnosticsSnapshot()
	allocator := snapshot["allocator"].(map[string]any)
	root := snapshot["root"].(map[string]any)
	lease := snapshot["lease"].(map[string]any)
	seal := snapshot["seal"].(map[string]any)
	audit := snapshot["audit"].(map[string]any)
	regions := snapshot["region_descriptors"].([]map[string]any)

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
	require.Equal(t, uint64(3), lease["epoch"])
	require.Equal(t, uint32(rootproto.MandateDefault), lease["mandate"])
	require.Equal(t, map[string]any{"term": uint64(2), "index": uint64(9)}, lease["issued_at"])
	require.Equal(t, "c1", seal["holder_id"])
	require.Equal(t, uint64(2), seal["epoch"])
	require.Equal(t, []map[string]any{
		{"mandate": rootproto.MandateAllocID, "duty_name": "alloc_id", "frontier": uint64(44)},
		{"mandate": rootproto.MandateTSO, "duty_name": "tso", "frontier": uint64(77)},
		{"mandate": rootproto.MandateGetRegionByKey, "duty_name": "get_region_by_key", "frontier": uint64(5)},
	}, seal["consumed_frontiers"])
	require.Equal(t, map[string]any{"term": uint64(2), "index": uint64(8)}, seal["sealed_at"])
	require.Equal(t, uint64(2), audit["legacy_epoch"])
	require.Equal(t, true, audit["successor_present"])
	require.Equal(t, true, audit["successor_lineage_satisfied"])
	require.Equal(t, true, audit["successor_monotone_covered"])
	require.Equal(t, false, audit["successor_descriptor_covered"])
	require.Equal(t, true, audit["sealed_generation_retired"])
	require.Equal(t, false, audit["finality_satisfied"])
	require.Equal(t, "unspecified", audit["handover_stage"])
	require.Len(t, regions, 1)
	require.Equal(t, uint64(11), regions[0]["region_id"])
	require.Equal(t, uint64(1), regions[0]["leader_store_id"])
	require.NotZero(t, regions[0]["leader_reported_unix"])
}

func TestServiceGetRegionByKeyStrongReadRejectsFollower(t *testing.T) {
	storage := &fakeStorage{
		leader:   false,
		leaderID: 7,
		snapshot: rootview.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
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
	desc.RootEpoch = 5
	cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		nil,
		nil,
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
	)
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
			Descriptors: map[uint64]descriptor.Descriptor{desc.RegionID: desc},
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: 10_000,
				Epoch:           4,
			},
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
	require.Equal(t, uint64(5), resp.GetDescriptorRevision())
	require.Zero(t, resp.GetRequiredDescriptorRevision())
	require.Equal(t, uint64(4), resp.GetEpoch())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_HEALTHY, resp.GetSyncHealth())
}

func TestServiceGetRegionByKeyRequiredDescriptorRevision(t *testing.T) {
	cluster := catalog.NewCluster()
	desc := testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 7
	cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		nil,
		nil,
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
	)
	storage := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 5},
			Descriptors: map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("a"),
		RequiredDescriptorRevision: 8,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRequiredDescriptorNotSatisfied)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("a"),
		RequiredDescriptorRevision: 7,
	})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, uint64(7), resp.GetDescriptorRevision())
	require.Equal(t, uint64(7), resp.GetRequiredDescriptorRevision())
	require.Equal(t, uint64(7), resp.GetRegionDescriptor().GetRootEpoch())
	require.Zero(t, resp.GetEpoch())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_HEALTHY, resp.GetSyncHealth())
}

func TestServiceGetRegionByKeyRejectsSplitPendingDescriptor(t *testing.T) {
	cluster := catalog.NewCluster()
	left := testDescriptor(41, []byte("a"), []byte("m"), metaregion.Epoch{Version: 2, ConfVersion: 1}, nil)
	right := testDescriptor(42, []byte("m"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	left.RootEpoch = 6
	right.RootEpoch = 6
	cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{
			left.RegionID:  left,
			right.RegionID: right,
		},
		nil,
		map[uint64]rootstate.PendingRangeChange{
			40: {
				Kind:           rootstate.PendingRangeChangeSplit,
				ParentRegionID: 40,
				LeftRegionID:   left.RegionID,
				RightRegionID:  right.RegionID,
				Left:           left,
				Right:          right,
			},
		},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 6},
	)
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRangeChangePending)
	require.Contains(t, err.Error(), "split")
}

func TestServiceGetRegionByKeyRejectsMergePendingDescriptor(t *testing.T) {
	cluster := catalog.NewCluster()
	merged := testDescriptor(51, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 2}, nil)
	merged.RootEpoch = 9
	cluster.ReplaceRootSnapshot(
		map[uint64]descriptor.Descriptor{
			merged.RegionID: merged,
		},
		nil,
		map[uint64]rootstate.PendingRangeChange{
			merged.RegionID: {
				Kind:          rootstate.PendingRangeChangeMerge,
				LeftRegionID:  49,
				RightRegionID: 50,
				Merged:        merged,
			},
		},
		rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 9},
	)
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRangeChangePending)
	require.Contains(t, err.Error(), "merge")
}

func TestServiceGetRegionByKeyUsesCachedRootSnapshot(t *testing.T) {
	desc := descriptor.Descriptor{
		RegionID:  10,
		StartKey:  []byte("a"),
		EndKey:    []byte("z"),
		RootEpoch: 7,
	}
	desc.EnsureHash()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			Descriptors: map[uint64]descriptor.Descriptor{10: desc},
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 3},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	require.NoError(t, svc.ReloadFromStorage())
	store.loadCalls = 0

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, err)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("c")})
	require.NoError(t, err)
	require.Equal(t, 0, store.loadCalls)
}

func TestServiceGetRegionByKeyRefreshesCachedRootSnapshotAsync(t *testing.T) {
	desc := descriptor.Descriptor{
		RegionID:  10,
		StartKey:  []byte("a"),
		EndKey:    []byte("z"),
		RootEpoch: 7,
	}
	desc.EnsureHash()
	store := &fakeStorage{
		snapshot: rootview.Snapshot{
			Descriptors: map[uint64]descriptor.Descriptor{10: desc},
			RootToken:   rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 1, Index: 3}, Revision: 3},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureRootSnapshotRefresh(10 * time.Millisecond)
	require.NoError(t, svc.ReloadFromStorage())
	store.loadCalls = 0

	time.Sleep(20 * time.Millisecond)
	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("b")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return store.loadCalls >= 1
	}, time.Second, 10*time.Millisecond)
}

func TestServiceGetRegionByKeyBestEffortWithUnavailableRoot(t *testing.T) {
	storage := &fakeStorage{
		leader:   true,
		snapshot: rootview.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureRootSnapshotRefresh(10 * time.Millisecond)
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	storage.loadCalls = 0
	storage.loadErr = errors.New("root unavailable")

	time.Sleep(20 * time.Millisecond)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return storage.loadCalls >= 1
	}, time.Second, 10*time.Millisecond)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE, resp.GetDegradedMode())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BEST_EFFORT, resp.GetServedFreshness())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_DEGRADED, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_ROOT_UNAVAILABLE, resp.GetSyncHealth())

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
		snapshot: rootview.Snapshot{
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
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_BOUNDED_STALE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_LAGGING, resp.GetSyncHealth())

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
		snapshot: rootview.Snapshot{
			RootToken:    rootstorage.TailToken{Cursor: rootstate.Cursor{Term: 2, Index: 9}, Revision: 7},
			CatchUpState: rootview.CatchUpStateBootstrapRequired,
			Descriptors:  map[uint64]descriptor.Descriptor{desc.RegionID: desc},
		},
	}
	svc := NewService(cluster, idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED, resp.GetCatchUpState())
	require.Equal(t, coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING, resp.GetDegradedMode())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_DEGRADED, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_BOOTSTRAP_REQUIRED, resp.GetSyncHealth())

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

func TestServiceGetRegionByKeyNotFoundCarriesAnswerabilityContract(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1))

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:                        []byte("missing"),
		Freshness:                  coordpb.Freshness_FRESHNESS_BOUNDED,
		RequiredDescriptorRevision: 7,
	})
	require.NoError(t, err)
	require.True(t, resp.GetNotFound())
	require.Equal(t, coordpb.Freshness_FRESHNESS_BOUNDED, resp.GetServedFreshness())
	require.Equal(t, uint64(7), resp.GetRequiredDescriptorRevision())
	require.Equal(t, coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE, resp.GetServingClass())
	require.Equal(t, coordpb.SyncHealth_SYNC_HEALTH_HEALTHY, resp.GetSyncHealth())
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
	require.Zero(t, idResp.GetEpoch())
	require.Equal(t, uint64(102), idResp.GetConsumedFrontier())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(500), tsResp.GetTimestamp())
	require.Equal(t, uint64(2), tsResp.GetCount())
	require.Zero(t, tsResp.GetEpoch())
	require.Equal(t, uint64(501), tsResp.GetConsumedFrontier())
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		snapshot: rootview.Snapshot{
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
		fakeStorage: fakeStorage{snapshot: rootview.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}},
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
		fakeStorage: fakeStorage{snapshot: rootview.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)}},
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
	cluster := catalog.NewCluster()
	desc := testDescriptor(1, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 7
	require.NoError(t, cluster.PublishRegionDescriptor(desc))
	svc := NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)

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

	first, err := svc.reserveIDs(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(99), store.lastTS)
	require.Equal(t, uint64(12), svc.ids.Current())

	first, err = svc.reserveIDs(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(13), first)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(14), svc.ids.Current())

	first, err = svc.reserveIDs(context.Background(), 1)
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

	first, count, err := svc.reserveTSO(context.Background(), 3)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(3), count)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(102), svc.tso.Current())

	first, count, err = svc.reserveTSO(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(103), first)
	require.Equal(t, uint64(2), count)
	require.Equal(t, 1, store.saveCalls)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(104), svc.tso.Current())

	first, count, err = svc.reserveTSO(context.Background(), 1)
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

	first, err := svc.reserveIDs(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, uint64(14), store.lastID)
	require.Equal(t, uint64(11), svc.ids.Current())

	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint64(11), svc.ids.Current())

	first, err = svc.reserveIDs(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(12), first)
	require.Equal(t, 1, store.saveCalls)
}

func TestServiceReloadDoesNotConsumeActiveTSOWindow(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.tsoWindowSize = 5

	first, _, err := svc.reserveTSO(context.Background(), 2)
	require.NoError(t, err)
	require.Equal(t, uint64(100), first)
	require.Equal(t, uint64(104), store.lastTS)
	require.Equal(t, uint64(101), svc.tso.Current())

	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint64(101), svc.tso.Current())

	first, _, err = svc.reserveTSO(context.Background(), 1)
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

func TestServiceTenureReusedAcrossAllocatorRequests(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(10), idResp.GetFirstId())
	require.Equal(t, uint64(1), idResp.GetEpoch())
	require.Equal(t, uint64(10), idResp.GetConsumedFrontier())
	require.Equal(t, 1, store.campaignCalls)
	require.Equal(t, "c1", store.snapshot.Tenure.HolderID)
	require.Equal(t, uint64(1), store.snapshot.Tenure.Epoch)

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsResp.GetTimestamp())
	require.Equal(t, uint64(1), tsResp.GetEpoch())
	require.Equal(t, uint64(100), tsResp.GetConsumedFrontier())
	require.Equal(t, 1, store.campaignCalls)
}

func TestServiceMonotoneRepliesCarryLeaseEvidence(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(50), tso.NewAllocator(900), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	idResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(1), idResp.GetEpoch())
	require.Equal(t, uint64(52), idResp.GetConsumedFrontier())

	tsResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 4})
	require.NoError(t, err)
	require.Equal(t, uint64(1), tsResp.GetEpoch())
	require.Equal(t, uint64(903), tsResp.GetConsumedFrontier())
}

func TestServiceTenureRenewsInsideRenewWindow(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 100*time.Millisecond, 20*time.Millisecond)

	now := time.Unix(0, 0)
	svc.now = func() time.Time { return now }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 1, store.campaignCalls)

	now = now.Add(85 * time.Millisecond)
	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 2, store.campaignCalls)
	require.Equal(t, uint64(1), store.snapshot.Tenure.Epoch)
}

func TestServiceTenureRenewDoesNotReloadAllocators(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 100*time.Millisecond, 20*time.Millisecond)

	now := time.Unix(0, 0)
	svc.now = func() time.Time { return now }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 1, store.loadCalls)

	now = now.Add(85 * time.Millisecond)
	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 2, store.loadCalls)
	require.Equal(t, 2, store.campaignCalls)
}

func TestServiceTenureStopsBeforeExpiryByClockSkew(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 100*time.Millisecond, 20*time.Millisecond)
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

func TestServiceTenureLoopRenewsInBackground(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 80*time.Millisecond, 30*time.Millisecond)

	ctx := t.Context()
	go svc.RunTenureLoop(ctx)

	require.Eventually(t, func() bool {
		return store.campaignCalls >= 2
	}, 300*time.Millisecond, 10*time.Millisecond)
}

func TestServiceTenureLoopSkipsFollower(t *testing.T) {
	store := &fakeStorage{leader: false, leaderID: 2}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 80*time.Millisecond, 30*time.Millisecond)

	ctx := t.Context()
	go svc.RunTenureLoop(ctx)

	time.Sleep(80 * time.Millisecond)
	require.Equal(t, 0, store.campaignCalls)
}

func TestServiceTenureLoopReleasesOnContextCancel(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 80*time.Millisecond, 30*time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		svc.RunTenureLoop(ctx)
	}()

	require.Eventually(t, func() bool {
		return store.campaignCalls >= 1
	}, 300*time.Millisecond, 10*time.Millisecond)

	cancel()
	require.Eventually(t, func() bool {
		return store.releaseCalls >= 1
	}, time.Second, 10*time.Millisecond)
	<-done
}

func TestServiceTenureRetryDelayBacksOff(t *testing.T) {
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100))
	svc.now = func() time.Time { return time.Unix(0, 123456789) }

	first := svc.coordinatorLeaseRetryDelay(1)
	fourth := svc.coordinatorLeaseRetryDelay(4)
	sixteenth := svc.coordinatorLeaseRetryDelay(16)

	require.GreaterOrEqual(t, first, 160*time.Millisecond)
	require.LessOrEqual(t, first, 240*time.Millisecond)
	require.GreaterOrEqual(t, fourth, 1280*time.Millisecond)
	require.LessOrEqual(t, fourth, 1920*time.Millisecond)
	require.GreaterOrEqual(t, sixteenth, 48*time.Second)
	require.LessOrEqual(t, sixteenth, 60*time.Second)
}

func TestServiceReleaseTenure(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)

	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReleaseTenure())
	require.Equal(t, 1, store.releaseCalls)
	require.Equal(t, int64(200), store.snapshot.Tenure.ExpiresUnixNano)
	require.Equal(t, uint64(1), store.snapshot.Tenure.Epoch)
	require.False(t, store.snapshot.Tenure.ActiveAt(200))
}

func TestServiceReleaseTenureDoesNotReloadAllocators(t *testing.T) {
	store := &fakeStorage{leader: true}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, 1, store.loadCalls)

	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReleaseTenure())
	require.Equal(t, 2, store.loadCalls)
	require.Equal(t, 1, store.releaseCalls)
}

func TestServiceSealTenure(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
		},
	}
	cluster := catalog.NewCluster()
	svc := NewService(cluster, idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())
	desc := testDescriptor(11, []byte("a"), []byte("z"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil)
	desc.RootEpoch = 7
	require.NoError(t, cluster.PublishRegionDescriptor(desc))
	store.snapshot.Descriptors = map[uint64]descriptor.Descriptor{desc.RegionID: desc}

	allocResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 2})
	require.NoError(t, err)
	require.Equal(t, uint64(10), allocResp.GetFirstId())

	tsoResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 3})
	require.NoError(t, err)
	require.Equal(t, uint64(100), tsoResp.GetTimestamp())
	campaignCallsBeforeSeal := store.campaignCalls

	require.NoError(t, svc.SealTenure())
	require.Equal(t, 1, store.sealCalls)
	require.Equal(t, "c1", store.snapshot.Legacy.HolderID)
	require.Equal(t, uint64(2), store.snapshot.Legacy.Epoch)
	require.Equal(t, uint64(11), store.snapshot.Legacy.Frontiers.Frontier(rootproto.MandateAllocID))
	require.Equal(t, uint64(102), store.snapshot.Legacy.Frontiers.Frontier(rootproto.MandateTSO))
	require.Equal(t, uint64(7), store.snapshot.Legacy.Frontiers.Frontier(rootproto.MandateGetRegionByKey))

	nextID, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, uint64(3), nextID.GetEpoch())
	require.Equal(t, rootstate.DigestOfLegacy(store.snapshot.Legacy), store.snapshot.Tenure.LineageDigest)
	require.Equal(t, campaignCallsBeforeSeal+1, store.campaignCalls)

	routeResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, uint64(3), routeResp.GetEpoch())
}

func TestServiceSealTenurePreActionGateRejectsStaleHolder(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c2",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	err := svc.SealTenure()
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)
	require.Equal(t, 0, store.sealCalls)
}

func TestServiceSealTenureAblationNoop(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ConfigureAblation(coordablation.Config{DisableSeal: true}))
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.SealTenure())
	require.Equal(t, 0, store.sealCalls)
	require.Equal(t, rootstate.Legacy{}, store.snapshot.Legacy)
}

func TestServiceAblationDisableBudgetUsesLargeRunway(t *testing.T) {
	store := &fakeStorage{}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.idWindowSize = 1
	require.NoError(t, svc.ConfigureAblation(coordablation.Config{DisableBudget: true}))

	first, err := svc.reserveIDs(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(10), first)
	require.Equal(t, 1, store.saveCalls)

	first, err = svc.reserveIDs(context.Background(), 1)
	require.NoError(t, err)
	require.Equal(t, uint64(11), first)
	require.Equal(t, 1, store.saveCalls)
	require.Greater(t, store.lastID, uint64(100))
}

func TestServiceMonotoneDutyFailsWhenGenerationSealedAndCannotRenew(t *testing.T) {
	store := &fakeStorage{
		leader:      true,
		campaignErr: rootstate.ErrPrimacy,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID: "c1",
				Epoch:    2,
				Mandate:  rootproto.MandateDefault,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)
}

func TestServiceAblationDisableReplyEvidenceMarksWitnessSuppressed(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ConfigureAblation(coordablation.Config{DisableReplyEvidence: true}))
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	allocResp, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, rootproto.ContinuationWitnessGenerationSuppressed, allocResp.GetEpoch())
	require.Zero(t, allocResp.GetConsumedFrontier())

	tsoResp, err := svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.NoError(t, err)
	require.Equal(t, rootproto.ContinuationWitnessGenerationSuppressed, tsoResp.GetEpoch())
	require.Zero(t, tsoResp.GetConsumedFrontier())

	getResp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Equal(t, rootproto.ContinuationWitnessGenerationSuppressed, getResp.GetEpoch())
}

func TestServiceAblationFailStopOnRootUnreachRejectsBestEffortMetadata(t *testing.T) {
	storage := &fakeStorage{
		leader:   true,
		snapshot: rootview.Snapshot{Descriptors: make(map[uint64]descriptor.Descriptor)},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(1), tso.NewAllocator(1), storage)
	svc.ConfigureRootSnapshotRefresh(10 * time.Millisecond)
	require.NoError(t, svc.ConfigureAblation(coordablation.Config{FailStopOnRootUnreach: true}))
	err := publishDescriptorEvent(t, svc, testDescriptor(11, []byte(""), []byte("m"), metaregion.Epoch{Version: 1, ConfVersion: 1}, nil), 0)
	require.NoError(t, err)
	storage.loadCalls = 0
	storage.loadErr = errors.New("root unavailable")

	time.Sleep(20 * time.Millisecond)
	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		return storage.loadCalls >= 1
	}, time.Second, 10*time.Millisecond)

	_, err = svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errRootUnavailable)
}

func TestServiceMetadataAnswerFailsWhenGenerationSealedAndCannotRenew(t *testing.T) {
	store := &fakeStorage{
		leader:      true,
		campaignErr: rootstate.ErrPrimacy,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 0, TSOFence: 0}, 7),
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{Key: []byte("a")})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)
}

func TestServiceConfirmHandover(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Allocator: rootview.AllocatorState{
				IDCurrent: 12,
				TSCurrent: 34,
			},
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Epoch:           3,
				Mandate:         rootproto.MandateDefault,
				LineageDigest:   "seal-digest",
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
				SealedAt:  rootstate.Cursor{Term: 1, Index: 9},
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	store.snapshot.Tenure.LineageDigest = rootstate.DigestOfLegacy(store.snapshot.Legacy)

	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.ConfirmHandover())
	require.Equal(t, 1, store.confirmCalls)
	require.Equal(t, uint64(2), store.snapshot.Handover.LegacyEpoch)
	require.Equal(t, uint64(3), store.snapshot.Handover.SuccessorEpoch)
	require.Equal(t, rootstate.DigestOfLegacy(store.snapshot.Legacy), store.snapshot.Handover.LegacyDigest)
	require.Equal(t, rootproto.HandoverStageConfirmed, store.snapshot.Handover.Stage)
}

func TestServiceCloseHandover(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Epoch:           3,
				Mandate:         rootproto.MandateDefault,
				LineageDigest:   "seal-digest",
			},
			Handover: rootstate.Handover{
				HolderID:       "c1",
				LegacyEpoch:    2,
				SuccessorEpoch: 3,
				LegacyDigest:   "seal-digest",
				Stage:          rootproto.HandoverStageConfirmed,
			},
		},
	}

	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.CloseHandover())
	require.Equal(t, 1, store.closeCalls)
	require.Equal(t, uint64(3), store.snapshot.Handover.SuccessorEpoch)
	require.Equal(t, uint64(2), store.snapshot.Handover.LegacyEpoch)
	require.Equal(t, "seal-digest", store.snapshot.Handover.LegacyDigest)
	require.Equal(t, rootproto.HandoverStageClosed, store.snapshot.Handover.Stage)
}

func TestServiceReattachHandover(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Epoch:           3,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
			},
			Handover: rootstate.Handover{
				HolderID:       "c1",
				LegacyEpoch:    2,
				SuccessorEpoch: 3,
				LegacyDigest:   "seal-digest",
				Stage:          rootproto.HandoverStageClosed,
			},
		},
	}
	store.snapshot.Tenure.LineageDigest = "seal-digest"

	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.ReattachHandover())
	require.Equal(t, 1, store.reattachCalls)
	require.Equal(t, uint64(3), store.snapshot.Handover.SuccessorEpoch)
	require.Equal(t, uint64(2), store.snapshot.Handover.LegacyEpoch)
	require.Equal(t, "seal-digest", store.snapshot.Handover.LegacyDigest)
	require.Equal(t, rootproto.HandoverStageReattached, store.snapshot.Handover.Stage)
}

func TestServiceAblationDisableReattachNoop(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Epoch:           3,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
			},
			Handover: rootstate.Handover{
				HolderID:       "c1",
				LegacyEpoch:    2,
				SuccessorEpoch: 3,
				LegacyDigest:   "seal-digest",
				Stage:          rootproto.HandoverStageClosed,
			},
		},
	}
	store.snapshot.Tenure.LineageDigest = "seal-digest"

	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	require.NoError(t, svc.ConfigureAblation(coordablation.Config{DisableReattach: true}))
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	require.NoError(t, svc.ReattachHandover())
	require.Equal(t, 0, store.reattachCalls)
	require.Equal(t, rootproto.HandoverStageClosed, store.snapshot.Handover.Stage)
}

func TestServiceReattachHandoverRejectsLineageMismatch(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 20_000).UnixNano(),
				Epoch:           3,
				Mandate:         rootproto.MandateDefault,
				LineageDigest:   "other-digest",
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 12, TSOFence: 34}, 7),
			},
			Handover: rootstate.Handover{
				HolderID:       "c1",
				LegacyEpoch:    2,
				SuccessorEpoch: 3,
				LegacyDigest:   "seal-digest",
				Stage:          rootproto.HandoverStageClosed,
			},
		},
	}

	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	err := svc.ReattachHandover()
	require.ErrorIs(t, err, rootstate.ErrFinality)
	require.Equal(t, 1, store.reattachCalls)
	require.Equal(t, rootproto.HandoverStageClosed, store.snapshot.Handover.Stage)
}

func TestServiceMonotoneDutyFailsWhenInheritanceNotMet(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateAllocID | rootproto.MandateTSO,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 50, TSOFence: 150}, 0),
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)
}

func TestServiceMonotoneDutyFailsWhenDescriptorCoverageNotMet(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(0, 10_000).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
			Legacy: rootstate.Legacy{
				HolderID:  "c1",
				Epoch:     2,
				Mandate:   rootproto.MandateDefault,
				Frontiers: succession.Frontiers(rootstate.State{IDFence: 10, TSOFence: 100}, 8),
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				1: {RegionID: 1, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)
}

func TestServiceAllocIDFailsWhenDutyNotAdmitted(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(10, 0).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateTSO | rootproto.MandateGetRegionByKey,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint32(rootproto.MandateTSO|rootproto.MandateGetRegionByKey), store.snapshot.Tenure.Mandate)
	require.Equal(t, uint32(rootproto.MandateTSO|rootproto.MandateGetRegionByKey), svc.currentTenure().Mandate)

	err := svc.successionGate(gateMandateAdmission, rootproto.MandateAllocID)
	require.Error(t, err)
	require.Contains(t, err.Error(), "mandate mismatch")

	_, err = svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "mandate mismatch")
}

func TestServiceGetRegionByKeyFailsWhenDutyNotAdmitted(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(10, 0).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateAllocID | rootproto.MandateTSO,
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				11: {RegionID: 11, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())
	require.Equal(t, uint32(rootproto.MandateAllocID|rootproto.MandateTSO), store.snapshot.Tenure.Mandate)
	require.Equal(t, uint32(rootproto.MandateAllocID|rootproto.MandateTSO), svc.currentTenure().Mandate)

	_, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "mandate mismatch")
}

func TestServiceGetRegionByKeyAllowsReadOnlyServingFromCurrentRootedGeneration(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c1",
				ExpiresUnixNano: time.Unix(10, 0).UnixNano(),
				Epoch:           2,
				Mandate:         rootproto.MandateDefault,
			},
			Descriptors: map[uint64]descriptor.Descriptor{
				11: {RegionID: 11, StartKey: []byte("a"), EndKey: []byte("z"), RootEpoch: 7},
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c2", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 200) }
	require.NoError(t, svc.ReloadFromStorage())

	resp, err := svc.GetRegionByKey(context.Background(), &coordpb.GetRegionByKeyRequest{
		Key:       []byte("m"),
		Freshness: coordpb.Freshness_FRESHNESS_BEST_EFFORT,
	})
	require.NoError(t, err)
	require.False(t, resp.GetNotFound())
	require.Equal(t, uint64(11), resp.GetRegionDescriptor().GetRegionId())
	require.Equal(t, uint64(2), resp.GetEpoch())
}

func TestServiceTenureRejectsOtherHolder(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "c2",
				ExpiresUnixNano: 10_000,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), idalloc.NewIDAllocator(10), tso.NewAllocator(100), store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
	svc.now = func() time.Time { return time.Unix(0, 100) }

	_, err := svc.AllocID(context.Background(), &coordpb.AllocIDRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)

	_, err = svc.Tso(context.Background(), &coordpb.TsoRequest{Count: 1})
	require.Error(t, err)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), errTenurePrefix)
}

func TestServiceStoreHeartbeatSuppressesOperationsWithoutTenure(t *testing.T) {
	store := &fakeStorage{
		leader: true,
		snapshot: rootview.Snapshot{
			Tenure: rootstate.Tenure{
				HolderID:        "other",
				ExpiresUnixNano: 10_000,
			},
		},
	}
	svc := NewService(catalog.NewCluster(), nil, nil, store)
	svc.ConfigureTenure("c1", 10*time.Second, 3*time.Second)
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
		snapshot: rootview.Snapshot{
			ClusterEpoch: 4,
			Descriptors: map[uint64]descriptor.Descriptor{
				9: testDescriptor(9, []byte("a"), []byte("z"), metaregion.Epoch{Version: 3, ConfVersion: 1}, nil),
			},
			Allocator: rootview.AllocatorState{
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
	store := &fakeStorage{snapshot: rootview.Snapshot{ClusterEpoch: 7}}
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
