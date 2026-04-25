package server

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/feichai0017/NoKV/coordinator/catalog"
	pdview "github.com/feichai0017/NoKV/coordinator/view"
	rootevent "github.com/feichai0017/NoKV/meta/root/event"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StoreHeartbeat records store-level stats.
func (s *Service) StoreHeartbeat(ctx context.Context, req *coordpb.StoreHeartbeatRequest) (*coordpb.StoreHeartbeatResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "store heartbeat request is nil")
	}
	err := s.cluster.UpsertStoreHeartbeat(pdview.StoreStats{
		StoreID:           req.GetStoreId(),
		ClientAddr:        req.GetClientAddr(),
		RaftAddr:          req.GetRaftAddr(),
		RegionNum:         req.GetRegionNum(),
		LeaderNum:         req.GetLeaderNum(),
		Capacity:          req.GetCapacity(),
		Available:         req.GetAvailable(),
		DroppedOperations: req.GetDroppedOperations(),
	})
	if err != nil {
		if errors.Is(err, catalog.ErrInvalidStoreID) {
			return nil, status.Error(codes.InvalidArgument, err.Error())
		}
		if errors.Is(err, catalog.ErrStoreNotJoined) || errors.Is(err, catalog.ErrStoreRetired) {
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		}
		return nil, status.Error(codes.Internal, err.Error())
	}
	// Record which regions this store claims raft leadership of. If the
	// store previously claimed leaders it no longer owns (e.g. raft
	// transferred leadership), clear the stale claims so another store's
	// subsequent report wins.
	s.cluster.RecordRegionLeaders(req.GetStoreId(), req.GetLeaderRegionIds())
	operations := s.leaseScopedStoreOperations(ctx, req.GetStoreId())
	return &coordpb.StoreHeartbeatResponse{
		Accepted:   true,
		Operations: operations,
	}, nil
}

// GetStore returns the current runtime endpoint for one store.
func (s *Service) GetStore(ctx context.Context, req *coordpb.GetStoreRequest) (*coordpb.GetStoreResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil || req.GetStoreId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "get store request missing store_id")
	}
	info, ok := s.cluster.StoreInfoByID(req.GetStoreId())
	if !ok {
		return &coordpb.GetStoreResponse{NotFound: true}, nil
	}
	return &coordpb.GetStoreResponse{Store: s.storeInfoToProto(info)}, nil
}

// ListStores returns the current runtime store registry.
func (s *Service) ListStores(ctx context.Context, _ *coordpb.ListStoresRequest) (*coordpb.ListStoresResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	stores := s.cluster.StoreInfos()
	out := make([]*coordpb.StoreInfo, 0, len(stores))
	for _, info := range stores {
		out = append(out, s.storeInfoToProto(info))
	}
	return &coordpb.ListStoresResponse{Stores: out}, nil
}

func (s *Service) GetMount(ctx context.Context, req *coordpb.GetMountRequest) (*coordpb.GetMountResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil || req.GetMountId() == "" {
		return nil, status.Error(codes.InvalidArgument, "get mount request missing mount_id")
	}
	mount, ok := s.cluster.MountByID(req.GetMountId())
	if !ok {
		return &coordpb.GetMountResponse{NotFound: true}, nil
	}
	return &coordpb.GetMountResponse{Mount: mountInfoToProto(mount)}, nil
}

func (s *Service) ListMounts(ctx context.Context, _ *coordpb.ListMountsRequest) (*coordpb.ListMountsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	mounts := s.cluster.MountSnapshot()
	out := make([]*coordpb.MountInfo, 0, len(mounts))
	for _, mount := range mounts {
		out = append(out, mountInfoToProto(mount))
	}
	return &coordpb.ListMountsResponse{Mounts: out}, nil
}

func mountInfoToProto(mount rootstate.MountRecord) *coordpb.MountInfo {
	return &coordpb.MountInfo{
		MountId:       mount.MountID,
		RootInode:     mount.RootInode,
		SchemaVersion: mount.SchemaVersion,
		State:         mountStateToProto(mount.State),
		RegisteredAt:  metawire.RootCursorToProto(mount.RegisteredAt),
		RetiredAt:     metawire.RootCursorToProto(mount.RetiredAt),
	}
}

func mountStateToProto(state rootstate.MountState) coordpb.MountState {
	switch state {
	case rootstate.MountStateActive:
		return coordpb.MountState_MOUNT_STATE_ACTIVE
	case rootstate.MountStateRetired:
		return coordpb.MountState_MOUNT_STATE_RETIRED
	default:
		return coordpb.MountState_MOUNT_STATE_UNKNOWN
	}
}

func (s *Service) storeInfoToProto(info catalog.StoreInfo) *coordpb.StoreInfo {
	stats := info.Stats
	// Zero-time guard: avoid uint64 wrap when stats.UpdatedAt is the zero
	// time (no heartbeat received yet). PR #153 fix carried over to the
	// Stage 2 catalog.StoreInfo signature.
	var lastHeartbeat uint64
	if !stats.UpdatedAt.IsZero() {
		lastHeartbeat = uint64(stats.UpdatedAt.UnixNano())
	}
	return &coordpb.StoreInfo{
		StoreId:               stats.StoreID,
		ClientAddr:            stats.ClientAddr,
		RaftAddr:              stats.RaftAddr,
		State:                 s.storeState(info),
		RegionNum:             stats.RegionNum,
		LeaderNum:             stats.LeaderNum,
		Capacity:              stats.Capacity,
		Available:             stats.Available,
		DroppedOperations:     stats.DroppedOperations,
		LastHeartbeatUnixNano: lastHeartbeat,
	}
}

func (s *Service) storeState(info catalog.StoreInfo) coordpb.StoreState {
	if info.Membership.State == rootstate.StoreMembershipRetired {
		return coordpb.StoreState_STORE_STATE_TOMBSTONE
	}
	stats := info.Stats
	if stats.StoreID == 0 || stats.UpdatedAt.IsZero() {
		return coordpb.StoreState_STORE_STATE_UNKNOWN
	}
	now := time.Now()
	ttl := defaultStoreHeartbeatTTL
	if s != nil {
		if s.now != nil {
			now = s.now()
		}
		// storeHeartbeatTTL is read via atomic load to avoid a data race with
		// ConfigureStoreHeartbeatTTL writers; reads here happen on the RPC
		// path concurrently with reconfiguration.
		if v := time.Duration(s.storeHeartbeatTTL.Load()); v > 0 {
			ttl = v
		}
	}
	if ttl > 0 && stats.UpdatedAt.Add(ttl).Before(now) {
		return coordpb.StoreState_STORE_STATE_DOWN
	}
	return coordpb.StoreState_STORE_STATE_UP
}

// RegionLiveness records one runtime heartbeat without mutating rooted truth.
func (s *Service) RegionLiveness(ctx context.Context, req *coordpb.RegionLivenessRequest) (*coordpb.RegionLivenessResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil || req.GetRegionId() == 0 {
		return nil, status.Error(codes.InvalidArgument, "region liveness request missing region_id")
	}
	accepted := s.cluster.TouchRegionHeartbeat(req.GetRegionId())
	return &coordpb.RegionLivenessResponse{Accepted: accepted}, nil
}

// PublishRootEvent records one explicit rooted topology truth event.
func (s *Service) PublishRootEvent(ctx context.Context, req *coordpb.PublishRootEventRequest) (*coordpb.PublishRootEventResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil || req.GetEvent() == nil {
		return nil, status.Error(codes.InvalidArgument, "publish root event request missing event")
	}
	event := metawire.RootEventFromProto(req.GetEvent())
	if event.Kind == rootevent.KindUnknown {
		return nil, status.Error(codes.InvalidArgument, "publish root event requires known kind")
	}
	event, err := s.normalizeRootEvent(event)
	if err != nil {
		return nil, status.Error(codes.Internal, "normalize root event: "+err.Error())
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	if err := s.requireExpectedClusterEpoch(req.GetExpectedClusterEpoch()); err != nil {
		return nil, err
	}
	assessment, err := s.assessRootEventLifecycle(event)
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	resp := &coordpb.PublishRootEventResponse{
		Assessment: transitionAssessmentToProto(assessment),
	}
	if assessment.Decision == rootstate.RootEventLifecycleSkip {
		resp.Accepted = true
		return resp, nil
	}
	if err := s.cluster.ValidateRootEvent(event); err != nil {
		switch {
		case errors.Is(err, catalog.ErrInvalidRegionID), errors.Is(err, catalog.ErrInvalidMountID):
			return nil, status.Error(codes.InvalidArgument, err.Error())
		case errors.Is(err, catalog.ErrRegionHeartbeatStale), errors.Is(err, catalog.ErrRegionRangeOverlap),
			errors.Is(err, catalog.ErrMountNotFound), errors.Is(err, catalog.ErrMountRetired), errors.Is(err, catalog.ErrMountConflict),
			errors.Is(err, catalog.ErrSubtreeAuthorityNotFound), errors.Is(err, catalog.ErrSubtreeAuthorityConflict),
			errors.Is(err, catalog.ErrSubtreeAuthorityHandoff):
			return nil, status.Error(codes.FailedPrecondition, err.Error())
		default:
			return nil, status.Error(codes.Internal, err.Error())
		}
	}
	if s.storage != nil {
		if err := s.storage.AppendRootEvent(ctx, event); err != nil {
			return nil, status.Error(codes.Internal, "persist root event: "+err.Error())
		}
		if _, err := s.reloadRootedView(false); err != nil {
			return nil, status.Error(codes.Internal, "reload rooted view: "+err.Error())
		}
		resp.Accepted = true
		return resp, nil
	}
	if err := s.cluster.PublishRootEvent(event); err != nil {
		return nil, status.Error(codes.Internal, "apply root event after persist: "+err.Error())
	}
	resp.Accepted = true
	return resp, nil
}

func (s *Service) assessRootEventLifecycle(event rootevent.Event) (rootstate.TransitionAssessment, error) {
	if s == nil || s.storage == nil {
		if s == nil || s.cluster == nil {
			return rootstate.TransitionAssessment{}, nil
		}
		return s.cluster.ObserveRootEventLifecycle(event), nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return rootstate.TransitionAssessment{}, fmt.Errorf("load rooted snapshot: %w", err)
	}
	rooted := rootstate.Snapshot{
		Stores:              snapshot.Stores,
		Descriptors:         snapshot.Descriptors,
		PendingPeerChanges:  snapshot.PendingPeerChanges,
		PendingRangeChanges: snapshot.PendingRangeChanges,
	}
	assessment := rootstate.AssessTransition(rooted, event)
	_, err = rootstate.EvaluateRootEventLifecycle(rooted, event)
	return assessment, err
}

// RemoveRegion deletes region metadata from the Coordinator in-memory catalog.
func (s *Service) RemoveRegion(ctx context.Context, req *coordpb.RemoveRegionRequest) (*coordpb.RemoveRegionResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "remove region request is nil")
	}
	regionID := req.GetRegionId()
	if regionID == 0 {
		return nil, status.Error(codes.InvalidArgument, "remove region requires region_id > 0")
	}
	if err := s.requireLeaderForWrite(); err != nil {
		return nil, err
	}
	removed := s.cluster.HasRegion(regionID)
	if !removed {
		return &coordpb.RemoveRegionResponse{Removed: false}, nil
	}
	_, err := s.PublishRootEvent(ctx, &coordpb.PublishRootEventRequest{
		Event:                metawire.RootEventToProto(rootevent.RegionTombstoned(regionID)),
		ExpectedClusterEpoch: req.GetExpectedClusterEpoch(),
	})
	if err != nil {
		return nil, err
	}
	return &coordpb.RemoveRegionResponse{Removed: true}, nil
}
