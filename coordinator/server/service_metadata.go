package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	coordprotocol "github.com/feichai0017/NoKV/coordinator/protocol"
	coordstorage "github.com/feichai0017/NoKV/coordinator/storage"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	rootstorage "github.com/feichai0017/NoKV/meta/root/storage"
	metawire "github.com/feichai0017/NoKV/meta/wire"
	coordpb "github.com/feichai0017/NoKV/pb/coordinator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// GetRegionByKey returns region metadata for the specified key.
func (s *Service) GetRegionByKey(ctx context.Context, req *coordpb.GetRegionByKeyRequest) (*coordpb.GetRegionByKeyResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.Error(codes.Canceled, err.Error())
	}
	if req == nil {
		return nil, status.Error(codes.InvalidArgument, "get region by key request is nil")
	}
	state, err := s.currentReadState()
	admission, err := s.admitMetadataAnswerability(req, state, err)
	if err != nil {
		return nil, err
	}
	desc, ok := s.cluster.GetRegionDescriptorByKey(req.GetKey())
	if !ok {
		resp := admission.responseBase()
		resp.NotFound = true
		return resp, nil
	}
	if pending, ok := s.cluster.PendingRangeChangeForDescriptor(desc.RegionID); ok {
		return nil, status.Error(codes.FailedPrecondition, pendingRangeChangeError(pending))
	}
	if err := admission.admitDescriptorRevision(desc.RootEpoch); err != nil {
		return nil, err
	}
	resp := admission.responseBase()
	resp.RegionDescriptor = metawire.DescriptorToProto(desc)
	resp.DescriptorRevision = desc.RootEpoch
	return resp, nil
}

func pendingRangeChangeError(change rootstate.PendingRangeChange) string {
	switch change.Kind {
	case rootstate.PendingRangeChangeSplit:
		return fmt.Sprintf("%s: split parent=%d left=%d right=%d", errRangeChangePending, change.ParentRegionID, change.LeftRegionID, change.RightRegionID)
	case rootstate.PendingRangeChangeMerge:
		return fmt.Sprintf("%s: merge left=%d right=%d merged=%d", errRangeChangePending, change.LeftRegionID, change.RightRegionID, change.Merged.RegionID)
	default:
		return errRangeChangePending
	}
}

type readState struct {
	servedToken    rootstorage.TailToken
	currentToken   rootstorage.TailToken
	rootLag        uint64
	catchUpState   coordstorage.CatchUpState
	degraded       coordpb.DegradedMode
	servedByLeader bool
	certGeneration uint64
	sealGeneration uint64
	leasePresent   bool
	leaseActive    bool
	leaseSealed    bool
	leaseDutyMask  uint32
}

type metadataAnswerability struct {
	state                      readState
	freshness                  coordpb.Freshness
	requiredRootToken          rootstorage.TailToken
	requiredDescriptorRevision uint64
	maxRootLag                 *uint64
	servingClass               coordpb.ServingClass
	syncHealth                 coordpb.SyncHealth
}

func (a metadataAnswerability) admitDescriptorRevision(revision uint64) error {
	if revision < a.requiredDescriptorRevision {
		return status.Error(codes.FailedPrecondition, errRequiredDescriptorNotSatisfied)
	}
	return nil
}

func (a metadataAnswerability) responseBase() *coordpb.GetRegionByKeyResponse {
	return &coordpb.GetRegionByKeyResponse{
		NotFound:                   false,
		ServedRootToken:            rootTokenToProto(a.state.servedToken),
		ServedFreshness:            a.freshness,
		DegradedMode:               a.state.degraded,
		ServedByLeader:             a.state.servedByLeader,
		CurrentRootToken:           rootTokenToProto(a.state.currentToken),
		RootLag:                    a.state.rootLag,
		CatchUpState:               catchUpStateToProto(a.state.catchUpState),
		RequiredDescriptorRevision: a.requiredDescriptorRevision,
		CertGeneration:             a.state.certGeneration,
		ObservedSealGeneration:     a.state.sealGeneration,
		ServingClass:               a.servingClass,
		SyncHealth:                 a.syncHealth,
	}
}

func (s *Service) admitMetadataAnswerability(req *coordpb.GetRegionByKeyRequest, state readState, loadErr error) (metadataAnswerability, error) {
	admission := metadataAnswerability{
		state:                      state,
		freshness:                  coordprotocol.NormalizeFreshness(req.GetFreshness()),
		requiredRootToken:          rootTokenFromProto(req.GetRequiredRootToken()),
		requiredDescriptorRevision: req.GetRequiredDescriptorRevision(),
		maxRootLag:                 req.MaxRootLag,
	}
	if loadErr != nil {
		if s != nil && s.ablation.FailStopOnRootUnreach {
			return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRootUnavailable)
		}
		if admission.freshness == coordpb.Freshness_FRESHNESS_STRONG || admission.freshness == coordpb.Freshness_FRESHNESS_BOUNDED {
			return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRootUnavailable)
		}
		admission.state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
	}
	if loadErr == nil && s != nil && s.coordinatorLeaseEnabled() && admission.state.leasePresent {
		if admission.state.leaseDutyMask&rootproto.CoordinatorDutyGetRegionByKey == 0 {
			return metadataAnswerability{}, statusCoordinatorLease(fmt.Errorf("%w: required_duty_mask=%d rooted_duty_mask=%d generation=%d", rootstate.ErrCoordinatorLeaseDuty, rootproto.CoordinatorDutyGetRegionByKey, admission.state.leaseDutyMask, admission.state.certGeneration))
		}
		if !admission.state.leaseActive {
			return metadataAnswerability{}, statusCoordinatorLease(fmt.Errorf("%w: rooted lease expired generation=%d", rootstate.ErrInvalidCoordinatorLease, admission.state.certGeneration))
		}
		if admission.state.leaseSealed {
			return metadataAnswerability{}, statusCoordinatorLease(fmt.Errorf("%w: generation=%d sealed_generation=%d", rootstate.ErrCoordinatorLeaseHeld, admission.state.certGeneration, admission.state.certGeneration))
		}
	}
	if !rootTokenSatisfied(admission.state.servedToken, admission.requiredRootToken) {
		return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRequiredRootedTokenNotSatisfied)
	}
	if admission.freshness == coordpb.Freshness_FRESHNESS_BOUNDED &&
		admission.maxRootLag != nil &&
		!boundedLagSatisfied(admission.state.rootLag, *admission.maxRootLag) {
		return metadataAnswerability{}, status.Error(codes.FailedPrecondition, errRootLagExceedsBound)
	}
	servingClass, syncHealth, err := s.admitReadServing(admission.freshness, admission.state)
	if err != nil {
		return metadataAnswerability{}, err
	}
	admission.servingClass = servingClass
	admission.syncHealth = syncHealth
	return admission, nil
}

func (s *Service) currentReadState() (readState, error) {
	if s == nil {
		return readState{
			degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
			servedByLeader: true,
			catchUpState:   coordstorage.CatchUpStateFresh,
		}, nil
	}
	servedToken := rootstorage.TailToken{}
	if s.cluster != nil {
		servedToken = s.cluster.CatalogRootToken()
	}
	state := readState{
		degraded:       coordpb.DegradedMode_DEGRADED_MODE_HEALTHY,
		servedByLeader: s.storage == nil || s.storage.IsLeader(),
		servedToken:    servedToken,
		currentToken:   servedToken,
		catchUpState:   coordstorage.CatchUpStateFresh,
	}
	if s.storage == nil {
		state.rootLag = rootLag(state.currentToken, state.servedToken)
		return state, nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()
	snapshot, err := s.currentRootSnapshot()
	if err != nil {
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
		state.catchUpState = coordstorage.CatchUpStateUnavailable
		return state, err
	}
	if snapshot.CoordinatorLease.CertGeneration != 0 && strings.TrimSpace(snapshot.CoordinatorLease.HolderID) != "" {
		state.leasePresent = true
		state.leaseActive = snapshot.CoordinatorLease.ActiveAt(nowUnixNano)
		state.leaseSealed = rootstate.CoordinatorGenerationSealed(snapshot.CoordinatorLease, snapshot.CoordinatorSeal)
		state.leaseDutyMask = snapshot.CoordinatorLease.DutyMask
	}
	if snapshot.CoordinatorSeal.Present() {
		state.sealGeneration = snapshot.CoordinatorSeal.CertGeneration
	}
	state.currentToken = snapshot.RootToken
	state.rootLag = rootLag(state.currentToken, state.servedToken)
	state.catchUpState = snapshot.CatchUpState
	state.certGeneration = s.metadataReplyGeneration(snapshot.CoordinatorLease.CertGeneration)
	if s.cachedRootSnapshotStale() {
		if errText := s.lastRootReloadError(); strings.TrimSpace(errText) != "" {
			state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_UNAVAILABLE
			state.catchUpState = coordstorage.CatchUpStateUnavailable
			return state, errors.New(errText)
		}
	}
	if state.rootLag == 0 {
		state.catchUpState = coordstorage.CatchUpStateFresh
		return state, nil
	}
	if state.catchUpState == coordstorage.CatchUpStateFresh || state.catchUpState == coordstorage.CatchUpStateUnspecified {
		state.catchUpState = coordstorage.CatchUpStateLagging
	}
	if state.rootLag > 0 {
		state.degraded = coordpb.DegradedMode_DEGRADED_MODE_ROOT_LAGGING
	}
	return state, nil
}

func (s *Service) notLeaderError() error {
	if s == nil || s.storage == nil {
		return statusNotLeader(0)
	}
	leaderID := s.storage.LeaderID()
	if leaderID == 0 {
		return statusNotLeader(0)
	}
	return statusNotLeader(leaderID)
}

func rootTokenToProto(token rootstorage.TailToken) *coordpb.RootToken {
	return &coordpb.RootToken{
		Term:     token.Cursor.Term,
		Index:    token.Cursor.Index,
		Revision: token.Revision,
	}
}

func rootTokenFromProto(token *coordpb.RootToken) rootstorage.TailToken {
	if token == nil {
		return rootstorage.TailToken{}
	}
	return rootstorage.TailToken{
		Cursor: rootstate.Cursor{
			Term:  token.GetTerm(),
			Index: token.GetIndex(),
		},
		Revision: token.GetRevision(),
	}
}

func rootTokenSatisfied(current, required rootstorage.TailToken) bool {
	if required.Cursor.Term == 0 && required.Cursor.Index == 0 && required.Revision == 0 {
		return true
	}
	if current.Revision != 0 || required.Revision != 0 {
		return current.Revision >= required.Revision && !rootstate.CursorAfter(required.Cursor, current.Cursor)
	}
	return !rootstate.CursorAfter(required.Cursor, current.Cursor)
}

func rootLag(current, served rootstorage.TailToken) uint64 {
	if current.Revision > 0 || served.Revision > 0 {
		if current.Revision > served.Revision {
			return current.Revision - served.Revision
		}
		if current.Revision == served.Revision && rootstate.CursorAfter(current.Cursor, served.Cursor) {
			return 1
		}
		return 0
	}
	if rootstate.CursorAfter(current.Cursor, served.Cursor) {
		return 1
	}
	return 0
}

func boundedLagSatisfied(lag, bound uint64) bool {
	return lag <= bound
}

func catchUpStateToProto(state coordstorage.CatchUpState) coordpb.CatchUpState {
	switch state {
	case coordstorage.CatchUpStateFresh:
		return coordpb.CatchUpState_CATCH_UP_STATE_FRESH
	case coordstorage.CatchUpStateLagging:
		return coordpb.CatchUpState_CATCH_UP_STATE_LAGGING
	case coordstorage.CatchUpStateBootstrapRequired:
		return coordpb.CatchUpState_CATCH_UP_STATE_BOOTSTRAP_REQUIRED
	case coordstorage.CatchUpStateUnavailable:
		return coordpb.CatchUpState_CATCH_UP_STATE_UNAVAILABLE
	default:
		return coordpb.CatchUpState_CATCH_UP_STATE_UNSPECIFIED
	}
}

func (s *Service) admitReadServing(freshness coordpb.Freshness, state readState) (coordpb.ServingClass, coordpb.SyncHealth, error) {
	servingClass, syncHealth := coordprotocol.MetadataServingContract(
		state.degraded,
		catchUpStateToProto(state.catchUpState),
		state.rootLag,
		state.servedByLeader,
	)

	switch freshness {
	case coordpb.Freshness_FRESHNESS_STRONG:
		if servingClass != coordpb.ServingClass_SERVING_CLASS_AUTHORITATIVE {
			if !state.servedByLeader {
				return servingClass, syncHealth, s.notLeaderError()
			}
			return servingClass, syncHealth, status.Error(codes.FailedPrecondition, errRootLagExceedsStrongFreshness)
		}
	case coordpb.Freshness_FRESHNESS_BOUNDED:
		if servingClass == coordpb.ServingClass_SERVING_CLASS_DEGRADED {
			switch syncHealth {
			case coordpb.SyncHealth_SYNC_HEALTH_ROOT_UNAVAILABLE:
				return servingClass, syncHealth, status.Error(codes.FailedPrecondition, errRootUnavailable)
			case coordpb.SyncHealth_SYNC_HEALTH_BOOTSTRAP_REQUIRED:
				return servingClass, syncHealth, status.Error(codes.FailedPrecondition, errBootstrapRequiredBeforeBounded)
			}
		}
	}

	return servingClass, syncHealth, nil
}
