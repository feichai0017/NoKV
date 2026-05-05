package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) resetAuthorityServing() {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	s.authorityState = authorityServing
	s.authorityInflight = 0
	s.authorityMu.Unlock()
}

func (s *Service) beginDutyAdmission(ctx context.Context, duty rootproto.DutyID) (func(), error) {
	if s == nil || !s.coordinatorGrantEnabled() {
		return func() {}, nil
	}
	if err := s.rejectIfAuthorityClosed(); err != nil {
		return nil, err
	}
	if err := s.ensureGrant(ctx); err != nil {
		return nil, translateGrantError(err)
	}
	if s.grantInheritanceCandidate() {
		if err := s.InheritRetiredGrants(ctx); err != nil {
			return nil, err
		}
	}
	if err := s.eunomiaGate(gateDutyAdmission, duty); err != nil {
		return nil, err
	}
	done, err := s.beginAuthorityServing(ctx, duty)
	if err != nil {
		return nil, err
	}
	return done, nil
}

func (s *Service) beginAuthorityServing(ctx context.Context, duty rootproto.DutyID) (func(), error) {
	if s == nil || !s.coordinatorGrantEnabled() {
		return func() {}, nil
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, status.FromContextError(err).Err()
		}
	}
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	switch s.authorityState {
	case authorityServing:
		s.authorityInflight++
		return func() { s.finishAuthorityServing() }, nil
	case authorityDraining:
		return nil, statusGrant(fmt.Errorf("%w: authority is draining", rootstate.ErrSilence))
	case authoritySealed:
		return nil, statusGrant(fmt.Errorf("%w: authority is sealed", rootstate.ErrSilence))
	default:
		return nil, statusGrant(fmt.Errorf("%w: unknown authority state=%d", rootstate.ErrPrimacy, s.authorityState))
	}
}

func (s *Service) finishAuthorityServing() {
	if s == nil || !s.coordinatorGrantEnabled() {
		return
	}
	s.authorityMu.Lock()
	if s.authorityInflight > 0 {
		s.authorityInflight--
	}
	s.authorityMu.Unlock()
}

func (s *Service) authorityServingSnapshot() (authorityServingState, uint64) {
	if s == nil {
		return authorityServing, 0
	}
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	return s.authorityState, s.authorityInflight
}

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
	gateDutyAdmission gateKind = iota
)

// eunomiaGate checks the cached grant mirror on authority-bearing hot paths.
// The mirror is kept current by rooted-tail refresh and by ApplyGrant responses.
func (s *Service) eunomiaGate(kind gateKind, duty rootproto.DutyID) error {
	if s == nil || !s.coordinatorGrantEnabled() || s.storage == nil {
		return nil
	}
	return s.eunomiaGateCached(kind, duty)
}

func (s *Service) rejectIfAuthorityClosed() error {
	if s == nil || !s.coordinatorGrantEnabled() {
		return nil
	}
	s.authorityMu.Lock()
	state := s.authorityState
	s.authorityMu.Unlock()
	switch state {
	case authorityServing:
		return nil
	case authorityDraining:
		return statusGrant(fmt.Errorf("%w: authority is draining", rootstate.ErrSilence))
	case authoritySealed:
		return statusGrant(fmt.Errorf("%w: authority is sealed", rootstate.ErrSilence))
	default:
		return statusGrant(fmt.Errorf("%w: unknown authority state=%d", rootstate.ErrPrimacy, state))
	}
}

func (s *Service) eunomiaGateCached(kind gateKind, duty rootproto.DutyID) error {
	return s.validateGateGrant(kind, duty, s.currentGrant())
}

func (s *Service) validateGateGrant(kind gateKind, duty rootproto.DutyID, grant rootproto.AuthorityGrant) error {
	if s == nil {
		return nil
	}
	nowFn := s.now
	if nowFn == nil {
		nowFn = time.Now
	}
	nowUnixNano := nowFn().UnixNano()

	s.grantMu.RLock()
	holderID := strings.TrimSpace(s.coordinatorID)
	s.grantMu.RUnlock()
	if holderID == "" {
		return nil
	}

	if !grant.Present() {
		s.eunomiaMetrics.recordGateRejection(kind)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteePrimacy)
		return statusGrant(fmt.Errorf("%w: no rooted grant", rootstate.ErrPrimacy))
	}
	if grant.HolderID != holderID {
		s.eunomiaMetrics.recordGateRejection(kind)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteePrimacy)
		return statusGrant(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrPrimacy, grant.HolderID, holderID))
	}
	if !grant.ActiveAt(nowUnixNano) {
		s.eunomiaMetrics.recordGateRejection(kind)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteePrimacy)
		return statusGrant(fmt.Errorf("%w: rooted grant expired era=%d", rootstate.ErrInvalidGrant, grant.Era))
	}
	if duty != "" {
		if _, ok := grant.Duty(duty); !ok {
			s.eunomiaMetrics.recordGateRejection(kind)
			return statusGrant(fmt.Errorf("%w: required_duty=%s era=%d", rootstate.ErrDuty, rootproto.DutyName(duty), grant.Era))
		}
	}
	return nil
}
