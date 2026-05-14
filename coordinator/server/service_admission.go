// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
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
	s.authorityDuties = nil
	s.authorityMu.Unlock()
}

func (s *Service) beginDutyAdmission(ctx context.Context, duty rootproto.DutyID) (dutyAdmission, error) {
	if s == nil || !s.coordinatorGrantEnabled() {
		return dutyAdmission{done: func() {}}, nil
	}
	if err := s.rejectIfAuthorityClosed(duty); err != nil {
		return dutyAdmission{}, err
	}
	if err := s.ensureGrant(ctx, duty); err != nil {
		return dutyAdmission{}, translateGrantError(err)
	}
	if s.grantInheritanceCandidate() {
		s.eunomiaMetrics.recordGrantInheritanceSubmitted()
		if err := s.InheritRetiredGrants(ctx); err != nil {
			return dutyAdmission{}, err
		}
	} else {
		s.eunomiaMetrics.recordGrantInheritanceSkipped()
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
			return nil, statusContext(err)
		}
	}
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	slot := s.authorityDuties[duty]
	switch slot.state {
	case authorityServing:
		slot.inflight++
		s.setAuthorityDutyLocked(duty, slot)
		return func() { s.finishAuthorityServing(duty) }, nil
	case authorityDraining:
		return nil, statusGrant(fmt.Errorf("%w: authority is draining", rootstate.ErrSilence))
	case authoritySealed:
		return nil, statusGrant(fmt.Errorf("%w: authority is sealed", rootstate.ErrSilence))
	default:
		return nil, statusGrant(fmt.Errorf("%w: unknown authority state=%d", rootstate.ErrPrimacy, slot.state))
	}
}

func (s *Service) finishAuthorityServing(duty rootproto.DutyID) {
	if s == nil || !s.coordinatorGrantEnabled() {
		return
	}
	s.authorityMu.Lock()
	slot := s.authorityDuties[duty]
	if slot.inflight > 0 {
		slot.inflight--
	}
	s.setAuthorityDutyLocked(duty, slot)
	s.authorityMu.Unlock()
}

func (s *Service) authorityServingSnapshot() (authorityServingState, uint64) {
	if s == nil {
		return authorityServing, 0
	}
	s.authorityMu.Lock()
	defer s.authorityMu.Unlock()
	if len(s.authorityDuties) == 0 {
		return authorityServing, 0
	}
	state := authorityServing
	var inflight uint64
	for _, slot := range s.authorityDuties {
		inflight += slot.inflight
		if slot.state == authorityDraining {
			state = authorityDraining
			continue
		}
		if slot.state == authoritySealed && state == authorityServing {
			state = authoritySealed
		}
	}
	return state, inflight
}

func (s *Service) requireExpectedClusterEpoch(expected uint64) error {
	if expected == 0 {
		return nil
	}
	current, err := s.currentClusterEpoch()
	if err != nil {
		return statusInternalf("load current cluster era: %v", err)
	}
	if current == expected {
		return nil
	}
	return statusProtocol(fmt.Sprintf("pd/meta cluster era mismatch (expected=%d current=%d)", expected, current), reasonClusterEraMismatch)
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

func (s *Service) rejectIfAuthorityClosed(duty rootproto.DutyID) error {
	if s == nil || !s.coordinatorGrantEnabled() {
		return nil
	}
	s.authorityMu.Lock()
	state := s.authorityDuties[duty].state
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

func (s *Service) setAuthorityDutyLocked(duty rootproto.DutyID, slot authorityDutyServing) {
	if s.authorityDuties == nil {
		s.authorityDuties = make(map[rootproto.DutyID]authorityDutyServing)
	}
	if slot.state == authorityServing && slot.inflight == 0 {
		delete(s.authorityDuties, duty)
		return
	}
	s.authorityDuties[duty] = slot
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
	grant, ok := view.GrantFor(duty, rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal})
	if !ok {
		s.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: required_duty=%s", rootstate.ErrDuty, rootproto.DutyName(duty)))
	}
	scope := rootproto.DutyScope{Kind: rootproto.DutyScopeGlobal}
	retiredEraFloor := view.RetiredEraFloorFor(duty, scope)
	if authorityGrantRetiredAtFloor(grant, retiredEraFloor) {
		s.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
		s.eunomiaMetrics.recordGuaranteeViolation(guaranteeSilence)
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: rooted grant retired era=%d retired_floor=%d", rootstate.ErrSilence, grant.Era, retiredEraFloor))
	}
	if err := s.validateGateGrant(gateDutyAdmission, duty, grant); err != nil {
		return dutyAdmission{}, err
	}
	if grant.ExpiresUnixNano <= nowUnixNano+clockSkew.Nanoseconds() {
		s.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: grant inside clock-skew window era=%d", rootstate.ErrInvalidGrant, grant.Era))
	}
	if holderID == "" || grant.HolderID != holderID {
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: rooted holder=%s local_holder=%s", rootstate.ErrPrimacy, grant.HolderID, holderID))
	}
	cert := view.CertificateFor(grant)
	if !grantCertificateMatches(cert, grant) {
		s.eunomiaMetrics.recordGateRejection(gateDutyAdmission)
		return dutyAdmission{}, statusGrant(fmt.Errorf("%w: missing root-issued grant certificate grant_id=%s era=%d", rootstate.ErrInvalidGrant, grant.GrantID, grant.Era))
	}
	return dutyAdmission{
		grant:           grant,
		certificate:     cert,
		retirements:     authorityEvidenceRetirementsForDuty(view.retirements, duty, scope, retiredEraFloor),
		retiredEraFloor: retiredEraFloor,
		duty:            duty,
		servedUnixNano:  nowUnixNano,
	}, nil
}

// authorityEvidenceRetirementsForDuty keeps reply evidence scoped to the duty
// being served. Retirements for other duties are intentionally omitted so an
// alloc_id handoff cannot raise the apparent finality floor of a TSO response.
func authorityEvidenceRetirementsForDuty(retirements []rootproto.GrantRetirement, duty rootproto.DutyID, scope rootproto.DutyScope, retiredEraFloor uint64) []rootproto.GrantRetirement {
	if len(retirements) == 0 {
		return nil
	}
	out := make([]rootproto.GrantRetirement, 0, len(retirements))
	for _, retirement := range retirements {
		if !retirementCoversDutyScope(retirement, duty, scope) {
			continue
		}
		// The retired-era floor is the stable verifier contract for inherited
		// history. Keeping those records in every hot TSO/AllocID reply only
		// grows the evidence payload without adding a stronger fence.
		if retirement.InheritedByGrantID != "" && retirement.Era <= retiredEraFloor {
			continue
		}
		out = append(out, retirement)
	}
	return out
}

// retirementCoversDutyScope checks whether a retirement record is relevant to
// one duty/scope verifier key. Evidence filtering must use this exact key, not
// just the grant era, because client verifier state is persisted per duty.
func retirementCoversDutyScope(retirement rootproto.GrantRetirement, duty rootproto.DutyID, scope rootproto.DutyScope) bool {
	for _, bound := range retirement.Bounds {
		if bound.DutyID == duty && rootproto.ScopeEqual(bound.Scope, scope) {
			return true
		}
	}
	return false
}

// authorityGrantRetiredAtFloor is the Silence gate for one served duty. A grant
// at or below the duty's compact floor must not produce a detached reply.
func authorityGrantRetiredAtFloor(grant rootproto.AuthorityGrant, retiredFloor uint64) bool {
	return retiredFloor != 0 && grant.Era <= retiredFloor
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
