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

type dutyAdmission struct {
	grant           rootproto.AuthorityGrant
	certificate     rootproto.GrantCertificate
	retirements     []rootproto.GrantRetirement
	retiredEraFloor uint64
	duty            rootproto.DutyID
	servedUnixNano  int64
	done            func()
}

func (a dutyAdmission) Done() {
	if a.done != nil {
		a.done()
	}
}

func (s *Service) resetAuthorityServing() {
	if s == nil {
		return
	}
	s.authorityMu.Lock()
	s.authorityState = authorityServing
	s.authorityInflight = 0
	s.authorityMu.Unlock()
}

func (s *Service) beginDutyAdmission(ctx context.Context, duty rootproto.DutyID) (dutyAdmission, error) {
	if s == nil || !s.coordinatorGrantEnabled() {
		return dutyAdmission{done: func() {}}, nil
	}
	if err := s.rejectIfAuthorityClosed(); err != nil {
		return dutyAdmission{}, err
	}
	if err := s.ensureGrant(ctx); err != nil {
		return dutyAdmission{}, translateGrantError(err)
	}
	if s.grantInheritanceCandidate() {
		if err := s.InheritRetiredGrants(ctx); err != nil {
			return dutyAdmission{}, err
		}
	}
	admission, err := s.admitDutyFromCachedGrant(duty)
	if err != nil {
		return dutyAdmission{}, err
	}
	done, err := s.beginAuthorityServing(ctx, duty)
	if err != nil {
		return dutyAdmission{}, err
	}
	admission.done = done
	return admission, nil
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

func (s *Service) admitDutyFromCachedGrant(duty rootproto.DutyID) (dutyAdmission, error) {
	if s == nil {
		return dutyAdmission{}, nil
	}
	nowUnixNano := s.nowUnixNano()
	s.grantMu.RLock()
	view := s.grantView
	holderID := strings.TrimSpace(s.coordinatorID)
	clockSkew := s.grantClockSkew
	s.grantMu.RUnlock()
	if err := s.validateGateGrant(gateDutyAdmission, duty, view.grant); err != nil {
		return dutyAdmission{}, err
	}
	if view.grant.ExpiresUnixNano <= nowUnixNano+clockSkew.Nanoseconds() {
		s.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: grant inside clock-skew window era=%d", rootstate.ErrInvalidGrant, view.grant.Era))
	}
	if holderID == "" || view.grant.HolderID != holderID {
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrPrimacy, view.grant.HolderID, holderID))
	}
	if !grantCertificateMatches(view.certificate, view.grant) {
		s.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: missing root-issued grant certificate grant_id=%s era=%d", rootstate.ErrInvalidGrant, view.grant.GrantID, view.grant.Era))
	}
	return dutyAdmission{
		grant:           view.grant,
		certificate:     view.certificate,
		retirements:     append([]rootproto.GrantRetirement(nil), view.retirements...),
		retiredEraFloor: view.retiredEraFloor,
		duty:            duty,
		servedUnixNano:  nowUnixNano,
	}, nil
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
