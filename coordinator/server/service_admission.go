package server

import (
	"fmt"
	"strings"
	"time"

	rootfailpoints "github.com/feichai0017/NoKV/meta/root/failpoints"
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
		return status.Error(codes.Internal, "load current cluster era: "+err.Error())
	}
	if current == expected {
		return nil
	}
	return status.Error(codes.FailedPrecondition, fmt.Sprintf("pd/meta cluster era mismatch (expected=%d current=%d)", expected, current))
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

type gateKind uint8

const (
	gateLegacyFormation gateKind = iota
	gateHandoverMutation
	gateMandateAdmission
)

// eunomiaGate picks the tenure-view source based on kind.
//
// Mandate admission (hot path, once per AllocID/TSO/GetRegionByKey) uses the
// cached mirror: it must be cheap. The cache is kept honest by the rooted
// refresh loop and by publish paths that overwrite it on every committed
// rooted event.
//
// Lifecycle mutations (seal, close, reattach) are infrequent and safety
// critical, so they re-read from storage to avoid a tiny window where
// the cached mirror has not yet absorbed a concurrent publish.
func (s *Service) eunomiaGate(kind gateKind, mandate uint32) error {
	if s == nil || !s.coordinatorLeaseEnabled() || s.storage == nil {
		return nil
	}
	switch kind {
	case gateMandateAdmission:
		return s.eunomiaGateCached(kind, mandate)
	default:
		return s.eunomiaGateRooted(kind, mandate)
	}
}

// eunomiaGateCached validates against the in-memory mirror. Cheap but
// can race with a just-landed rooted publish; only safe for read-path
// mandate admission where a one-tick staleness is tolerable.
func (s *Service) eunomiaGateCached(kind gateKind, mandate uint32) error {
	current, seal := s.currentTenureView()
	return s.validateGateTenure(kind, mandate, current, seal)
}

// eunomiaGateRooted validates against a freshly loaded rooted
// snapshot. Used for control-plane mutations where stale-read would
// violate finality.
func (s *Service) eunomiaGateRooted(kind gateKind, mandate uint32) error {
	current, seal, err := s.currentTenureViewFromStorage()
	if err != nil {
		return status.Error(codes.Internal, "load rooted snapshot: "+err.Error())
	}
	return s.validateGateTenure(kind, mandate, current, seal)
}

func (s *Service) currentTenureViewFromStorage() (rootstate.Tenure, rootstate.Legacy, error) {
	if s == nil || s.storage == nil {
		return rootstate.Tenure{}, rootstate.Legacy{}, nil
	}
	if err := rootfailpoints.InjectBeforeTenureStorageRead(); err != nil {
		return rootstate.Tenure{}, rootstate.Legacy{}, err
	}
	snapshot, err := s.storage.Load()
	if err != nil {
		return rootstate.Tenure{}, rootstate.Legacy{}, err
	}
	s.refreshLeaseMirror(snapshot)
	return snapshot.Tenure, snapshot.Legacy, nil
}

func (s *Service) validateGateTenure(kind gateKind, mandate uint32, current rootstate.Tenure, seal rootstate.Legacy) error {
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
		s.eunomiaMetrics.recordGateRejection(kind)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteePrimacy)
		return statusTenure(fmt.Errorf("%w: no rooted tenure", rootstate.ErrPrimacy))
	}
	if current.HolderID != holderID {
		s.eunomiaMetrics.recordGateRejection(kind)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteePrimacy)
		return statusTenure(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrPrimacy, current.HolderID, holderID))
	}
	if !current.ActiveAt(nowUnixNano) {
		s.eunomiaMetrics.recordGateRejection(kind)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteePrimacy)
		return statusTenure(fmt.Errorf("%w: rooted lease expired era=%d", rootstate.ErrInvalidTenure, current.Era))
	}

	switch kind {
	case gateLegacyFormation:
		if rootstate.TenureSealed(current, seal) {
			s.eunomiaMetrics.recordGateRejection(kind)
			s.eunomiaMetrics.recordGuaranteeViolation(guaranteeFinality)
			return statusTenure(fmt.Errorf("%w: era=%d already sealed", rootstate.ErrFinality, current.Era))
		}
	case gateHandoverMutation:
		if rootstate.TenureSealed(current, seal) {
			s.eunomiaMetrics.recordGateRejection(kind)
			s.eunomiaMetrics.recordGuaranteeViolation(guaranteeSilence)
			return statusTenure(fmt.Errorf("%w: era=%d legacy_era=%d", rootstate.ErrSilence, current.Era, seal.Era))
		}
	case gateMandateAdmission:
		currentMandate := current.Mandate
		if mandate != 0 && currentMandate&mandate != mandate {
			s.eunomiaMetrics.recordGateRejection(kind)
			return statusTenure(fmt.Errorf("%w: required_mandate=%d rooted_mandate=%d era=%d", rootstate.ErrMandate, mandate, currentMandate, current.Era))
		}
		if rootstate.TenureSealed(current, seal) {
			s.eunomiaMetrics.recordGateRejection(kind)
			s.eunomiaMetrics.recordGuaranteeViolation(guaranteeSilence)
			return statusTenure(fmt.Errorf("%w: era=%d legacy_era=%d", rootstate.ErrSilence, current.Era, seal.Era))
		}
	}
	return nil
}
