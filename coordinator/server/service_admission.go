package server

import (
	"fmt"
	"strings"
	"time"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) requireExpectedClusterEpoch(expected uint64) error {
	if expected == 0 {
		return nil
	}
	current, err := s.currentClusterEpoch()
	if err != nil {
		return status.Error(codes.Internal, "load current cluster epoch: "+err.Error())
	}
	if current == expected {
		return nil
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("pd/meta cluster epoch mismatch (expected=%d current=%d)", expected, current))
}

func (s *Service) currentClusterEpoch() (uint64, error) {
	if s != nil && s.storage != nil {
		snapshot, err := s.storage.Load()
		if err != nil {
			return 0, err
		}
		return snapshot.ClusterEpoch, nil
	}
	var maxEpoch uint64
	if s != nil && s.cluster != nil {
		maxEpoch = s.cluster.MaxDescriptorRevision()
	}
	return maxEpoch, nil
}

func (s *Service) currentDescriptorRevision() uint64 {
	if s == nil || s.cluster == nil {
		return 0
	}
	return s.cluster.MaxDescriptorRevision()
}

type preActionKind uint8

const (
	preActionSealCurrentGeneration preActionKind = iota
	preActionLifecycleMutation
	preActionDutyAdmission
)

// preActionGate picks the lease-view source based on kind.
//
// Duty-admission (hot path, once per AllocID/TSO/GetRegionByKey) uses the
// cached mirror: it must be cheap. The cache is kept honest by the rooted
// refresh loop and by publish paths that overwrite it on every committed
// rooted event.
//
// Lifecycle mutations (seal, close, reattach) are infrequent and safety
// critical, so they re-read from storage to avoid a tiny window where
// the cached mirror has not yet absorbed a concurrent publish.
func (s *Service) preActionGate(kind preActionKind, dutyMask uint32) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	switch kind {
	case preActionDutyAdmission:
		return s.preActionGateCached(kind, dutyMask)
	default:
		return s.preActionGateStorage(kind, dutyMask)
	}
}

// preActionGateCached validates against the in-memory mirror. Cheap but
// can race with a just-landed rooted publish; only safe for read-path
// duty admission where a one-tick staleness is tolerable.
func (s *Service) preActionGateCached(kind preActionKind, dutyMask uint32) error {
	current, seal := s.currentCoordinatorLeaseView()
	return s.validatePreActionLease(kind, dutyMask, current, seal)
}

// preActionGateStorage validates against a freshly loaded rooted
// snapshot. Used for control-plane mutations where stale-read would
// violate closure completeness.
func (s *Service) preActionGateStorage(kind preActionKind, dutyMask uint32) error {
	current, seal, err := s.currentCoordinatorLeaseViewFromStorage()
	if err != nil {
		return status.Error(codes.Internal, "load rooted snapshot: "+err.Error())
	}
	return s.validatePreActionLease(kind, dutyMask, current, seal)
}

func (s *Service) currentCoordinatorLeaseViewFromStorage() (rootstate.CoordinatorLease, rootstate.CoordinatorSeal, error) {
	if s == nil || s.storage == nil {
		return rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}, nil
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return rootstate.CoordinatorLease{}, rootstate.CoordinatorSeal{}, err
	}
	s.refreshLeaseMirror(snapshot)
	return snapshot.CoordinatorLease, snapshot.CoordinatorSeal, nil
}

func (s *Service) validatePreActionLease(kind preActionKind, dutyMask uint32, current rootstate.CoordinatorLease, seal rootstate.CoordinatorSeal) error {
	if s == nil {
		return nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.leaseMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.leaseMu.RUnlock()
	if holderID == "" {
		return nil
	}

	if current.HolderID == "" {
		return statusCoordinatorLease(fmt.Errorf("%w: no rooted coordinator lease", rootstate.ErrCoordinatorLeaseHeld))
	}
	if current.HolderID != holderID {
		return statusCoordinatorLease(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrCoordinatorLeaseOwner, current.HolderID, holderID))
	}
	if !current.ActiveAt(nowUnixNano) {
		return statusCoordinatorLease(fmt.Errorf("%w: rooted lease expired generation=%d", rootstate.ErrInvalidCoordinatorLease, current.CertGeneration))
	}

	switch kind {
	case preActionSealCurrentGeneration:
		if rootstate.CoordinatorGenerationSealed(current, seal) {
			return statusCoordinatorLease(fmt.Errorf("%w: generation=%d already sealed", rootstate.ErrCoordinatorLeaseHeld, current.CertGeneration))
		}
	case preActionLifecycleMutation:
		if rootstate.CoordinatorGenerationSealed(current, seal) {
			return statusCoordinatorLease(fmt.Errorf("%w: generation=%d sealed_generation=%d", rootstate.ErrCoordinatorLeaseHeld, current.CertGeneration, seal.CertGeneration))
		}
	case preActionDutyAdmission:
		currentDutyMask := current.DutyMask
		if dutyMask != 0 && currentDutyMask&dutyMask != dutyMask {
			return statusCoordinatorLease(fmt.Errorf("%w: required_duty_mask=%d rooted_duty_mask=%d generation=%d", rootstate.ErrCoordinatorLeaseDuty, dutyMask, currentDutyMask, current.CertGeneration))
		}
		if rootstate.CoordinatorGenerationSealed(current, seal) {
			return statusCoordinatorLease(fmt.Errorf("%w: generation=%d sealed_generation=%d", rootstate.ErrCoordinatorLeaseHeld, current.CertGeneration, seal.CertGeneration))
		}
	}
	return nil
}
