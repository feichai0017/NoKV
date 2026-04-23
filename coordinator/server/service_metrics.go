package server

import (
	"errors"
	"sync/atomic"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

type coordinatorALIViolation uint8

const (
	aliAuthorityUniqueness coordinatorALIViolation = iota + 1
	aliSuccessorCoverage
	aliPostSealInadmissibility
	aliClosureCompleteness
)

type coordinatorCCCMetrics struct {
	leaseGenerationTransitionsTotal atomic.Uint64

	closureStageConfirmedTotal  atomic.Uint64
	closureStageClosedTotal     atomic.Uint64
	closureStageReattachedTotal atomic.Uint64

	preActionGateSealCurrentGenerationRejectedTotal atomic.Uint64
	preActionGateLifecycleMutationRejectedTotal     atomic.Uint64
	preActionGateDutyAdmissionRejectedTotal         atomic.Uint64

	aliAuthorityUniquenessTotal     atomic.Uint64
	aliSuccessorCoverageTotal       atomic.Uint64
	aliPostSealInadmissibilityTotal atomic.Uint64
	aliClosureCompletenessTotal     atomic.Uint64
}

func (m *coordinatorCCCMetrics) snapshot() map[string]any {
	return map[string]any{
		"lease_generation_transitions_total": m.leaseGenerationTransitionsTotal.Load(),
		"closure_stage_transitions_total": map[string]any{
			"confirmed":  m.closureStageConfirmedTotal.Load(),
			"closed":     m.closureStageClosedTotal.Load(),
			"reattached": m.closureStageReattachedTotal.Load(),
		},
		"pre_action_gate_rejections_total": map[string]any{
			"seal_current_generation": m.preActionGateSealCurrentGenerationRejectedTotal.Load(),
			"lifecycle_mutation":      m.preActionGateLifecycleMutationRejectedTotal.Load(),
			"duty_admission":          m.preActionGateDutyAdmissionRejectedTotal.Load(),
		},
		"ali_violations_total": map[string]any{
			"authority_uniqueness":      m.aliAuthorityUniquenessTotal.Load(),
			"successor_coverage":        m.aliSuccessorCoverageTotal.Load(),
			"post_seal_inadmissibility": m.aliPostSealInadmissibilityTotal.Load(),
			"closure_completeness":      m.aliClosureCompletenessTotal.Load(),
		},
	}
}

func (m *coordinatorCCCMetrics) recordLeaseGenerationTransition(before, after uint64) {
	if after == 0 || before == after {
		return
	}
	m.leaseGenerationTransitionsTotal.Add(1)
}

func (m *coordinatorCCCMetrics) recordClosureStageTransition(before, after rootproto.CoordinatorClosureStage) {
	if after == before {
		return
	}
	switch after {
	case rootproto.CoordinatorClosureStageConfirmed:
		m.closureStageConfirmedTotal.Add(1)
	case rootproto.CoordinatorClosureStageClosed:
		m.closureStageClosedTotal.Add(1)
	case rootproto.CoordinatorClosureStageReattached:
		m.closureStageReattachedTotal.Add(1)
	}
}

func (m *coordinatorCCCMetrics) recordPreActionGateRejection(kind preActionKind) {
	switch kind {
	case preActionSealCurrentGeneration:
		m.preActionGateSealCurrentGenerationRejectedTotal.Add(1)
	case preActionLifecycleMutation:
		m.preActionGateLifecycleMutationRejectedTotal.Add(1)
	case preActionDutyAdmission:
		m.preActionGateDutyAdmissionRejectedTotal.Add(1)
	}
}

func (m *coordinatorCCCMetrics) recordALIViolation(kind coordinatorALIViolation) {
	switch kind {
	case aliAuthorityUniqueness:
		m.aliAuthorityUniquenessTotal.Add(1)
	case aliSuccessorCoverage:
		m.aliSuccessorCoverageTotal.Add(1)
	case aliPostSealInadmissibility:
		m.aliPostSealInadmissibilityTotal.Add(1)
	case aliClosureCompleteness:
		m.aliClosureCompletenessTotal.Add(1)
	}
}

func (m *coordinatorCCCMetrics) recordALIViolationForError(err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, rootstate.ErrCoordinatorLeaseCoverage),
		errors.Is(err, rootstate.ErrCoordinatorLeaseLineage):
		m.recordALIViolation(aliSuccessorCoverage)
	case errors.Is(err, rootstate.ErrCoordinatorLeaseAudit),
		errors.Is(err, rootstate.ErrCoordinatorLeaseClose),
		errors.Is(err, rootstate.ErrCoordinatorLeaseReattach):
		m.recordALIViolation(aliClosureCompleteness)
	case errors.Is(err, rootstate.ErrCoordinatorLeaseOwner),
		errors.Is(err, rootstate.ErrCoordinatorLeaseHeld):
		m.recordALIViolation(aliAuthorityUniqueness)
	}
}
