package server

import (
	"errors"
	"sync/atomic"

	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

type coordinatorGuaranteeViolation uint8

const (
	guaranteePrimacy coordinatorGuaranteeViolation = iota + 1
	guaranteeInheritance
	guaranteeSilence
	guaranteeClosure
)

type successionMetrics struct {
	tenureEpochTransitionsTotal atomic.Uint64

	transitStageConfirmedTotal  atomic.Uint64
	transitStageClosedTotal     atomic.Uint64
	transitStageReattachedTotal atomic.Uint64

	gateLegacyFormationRejectedTotal  atomic.Uint64
	gateTransitMutationRejectedTotal  atomic.Uint64
	gateMandateAdmissionRejectedTotal atomic.Uint64

	guaranteePrimacyTotal     atomic.Uint64
	guaranteeInheritanceTotal atomic.Uint64
	guaranteeSilenceTotal     atomic.Uint64
	guaranteeClosureTotal     atomic.Uint64
}

func (m *successionMetrics) snapshot() map[string]any {
	return map[string]any{
		"tenure_epoch_transitions_total": m.tenureEpochTransitionsTotal.Load(),
		"transit_stage_transitions_total": map[string]any{
			"confirmed":  m.transitStageConfirmedTotal.Load(),
			"closed":     m.transitStageClosedTotal.Load(),
			"reattached": m.transitStageReattachedTotal.Load(),
		},
		"gate_rejections_total": map[string]any{
			"legacy_formation":  m.gateLegacyFormationRejectedTotal.Load(),
			"transit_mutation":  m.gateTransitMutationRejectedTotal.Load(),
			"mandate_admission": m.gateMandateAdmissionRejectedTotal.Load(),
		},
		"guarantee_violations_total": map[string]any{
			"primacy":     m.guaranteePrimacyTotal.Load(),
			"inheritance": m.guaranteeInheritanceTotal.Load(),
			"silence":     m.guaranteeSilenceTotal.Load(),
			"closure":     m.guaranteeClosureTotal.Load(),
		},
	}
}

func (m *successionMetrics) recordTenureEpochTransition(before, after uint64) {
	if after == 0 || before == after {
		return
	}
	m.tenureEpochTransitionsTotal.Add(1)
}

func (m *successionMetrics) recordTransitStageTransition(before, after rootproto.TransitStage) {
	if after == before {
		return
	}
	switch after {
	case rootproto.TransitStageConfirmed:
		m.transitStageConfirmedTotal.Add(1)
	case rootproto.TransitStageClosed:
		m.transitStageClosedTotal.Add(1)
	case rootproto.TransitStageReattached:
		m.transitStageReattachedTotal.Add(1)
	}
}

func (m *successionMetrics) recordGateRejection(kind gateKind) {
	switch kind {
	case gateLegacyFormation:
		m.gateLegacyFormationRejectedTotal.Add(1)
	case gateTransitMutation:
		m.gateTransitMutationRejectedTotal.Add(1)
	case gateMandateAdmission:
		m.gateMandateAdmissionRejectedTotal.Add(1)
	}
}

func (m *successionMetrics) recordGuaranteeViolation(kind coordinatorGuaranteeViolation) {
	switch kind {
	case guaranteePrimacy:
		m.guaranteePrimacyTotal.Add(1)
	case guaranteeInheritance:
		m.guaranteeInheritanceTotal.Add(1)
	case guaranteeSilence:
		m.guaranteeSilenceTotal.Add(1)
	case guaranteeClosure:
		m.guaranteeClosureTotal.Add(1)
	}
}

func (m *successionMetrics) recordGuaranteeViolationForError(err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, rootstate.ErrInheritance):
		m.recordGuaranteeViolation(guaranteeInheritance)
	case errors.Is(err, rootstate.ErrClosure):
		m.recordGuaranteeViolation(guaranteeClosure)
	case errors.Is(err, rootstate.ErrSilence):
		m.recordGuaranteeViolation(guaranteeSilence)
	case errors.Is(err, rootstate.ErrPrimacy):
		m.recordGuaranteeViolation(guaranteePrimacy)
	}
}
