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
	guaranteeFinality
)

type successionMetrics struct {
	tenureEraTransitionsTotal atomic.Uint64

	handoverStageConfirmedTotal  atomic.Uint64
	handoverStageClosedTotal     atomic.Uint64
	handoverStageReattachedTotal atomic.Uint64

	gateLegacyFormationRejectedTotal  atomic.Uint64
	gateHandoverMutationRejectedTotal atomic.Uint64
	gateMandateAdmissionRejectedTotal atomic.Uint64

	guaranteePrimacyTotal     atomic.Uint64
	guaranteeInheritanceTotal atomic.Uint64
	guaranteeSilenceTotal     atomic.Uint64
	guaranteeFinalityTotal    atomic.Uint64
}

func (m *successionMetrics) snapshot() map[string]any {
	return map[string]any{
		"tenure_era_transitions_total": m.tenureEraTransitionsTotal.Load(),
		"handover_stage_transitions_total": map[string]any{
			"confirmed":  m.handoverStageConfirmedTotal.Load(),
			"closed":     m.handoverStageClosedTotal.Load(),
			"reattached": m.handoverStageReattachedTotal.Load(),
		},
		"gate_rejections_total": map[string]any{
			"legacy_formation":  m.gateLegacyFormationRejectedTotal.Load(),
			"handover_mutation": m.gateHandoverMutationRejectedTotal.Load(),
			"mandate_admission": m.gateMandateAdmissionRejectedTotal.Load(),
		},
		"guarantee_violations_total": map[string]any{
			"primacy":     m.guaranteePrimacyTotal.Load(),
			"inheritance": m.guaranteeInheritanceTotal.Load(),
			"silence":     m.guaranteeSilenceTotal.Load(),
			"finality":    m.guaranteeFinalityTotal.Load(),
		},
	}
}

func (m *successionMetrics) recordTenureEraTransition(before, after uint64) {
	if after == 0 || before == after {
		return
	}
	m.tenureEraTransitionsTotal.Add(1)
}

func (m *successionMetrics) recordHandoverStageTransition(before, after rootproto.HandoverStage) {
	if after == before {
		return
	}
	switch after {
	case rootproto.HandoverStageConfirmed:
		m.handoverStageConfirmedTotal.Add(1)
	case rootproto.HandoverStageClosed:
		m.handoverStageClosedTotal.Add(1)
	case rootproto.HandoverStageReattached:
		m.handoverStageReattachedTotal.Add(1)
	}
}

func (m *successionMetrics) recordGateRejection(kind gateKind) {
	switch kind {
	case gateLegacyFormation:
		m.gateLegacyFormationRejectedTotal.Add(1)
	case gateHandoverMutation:
		m.gateHandoverMutationRejectedTotal.Add(1)
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
	case guaranteeFinality:
		m.guaranteeFinalityTotal.Add(1)
	}
}

func (m *successionMetrics) recordGuaranteeViolationForError(err error) {
	switch {
	case err == nil:
		return
	case errors.Is(err, rootstate.ErrInheritance):
		m.recordGuaranteeViolation(guaranteeInheritance)
	case errors.Is(err, rootstate.ErrFinality):
		m.recordGuaranteeViolation(guaranteeFinality)
	case errors.Is(err, rootstate.ErrSilence):
		m.recordGuaranteeViolation(guaranteeSilence)
	case errors.Is(err, rootstate.ErrPrimacy):
		m.recordGuaranteeViolation(guaranteePrimacy)
	}
}
