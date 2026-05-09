package server

import (
	"errors"
	"sync/atomic"

	rootstate "github.com/feichai0017/NoKV/meta/root/state"
)

type coordinatorGuaranteeViolation uint8

const (
	guaranteePrimacy coordinatorGuaranteeViolation = iota + 1
	guaranteeInheritance
	guaranteeSilence
	guaranteeFinality
)

type eunomiaMetrics struct {
	grantEraTransitionsTotal atomic.Uint64

	gateDutyAdmissionRejectedTotal atomic.Uint64

	grantInheritanceSkippedTotal   atomic.Uint64
	grantInheritanceSubmittedTotal atomic.Uint64

	guaranteePrimacyTotal     atomic.Uint64
	guaranteeInheritanceTotal atomic.Uint64
	guaranteeSilenceTotal     atomic.Uint64
	guaranteeFinalityTotal    atomic.Uint64
}

func (m *eunomiaMetrics) snapshot() map[string]any {
	return map[string]any{
		"grant_era_transitions_total": m.grantEraTransitionsTotal.Load(),
		"gate_rejections_total": map[string]any{
			"duty_admission": m.gateDutyAdmissionRejectedTotal.Load(),
		},
		"grant_inheritance_total": map[string]any{
			"skipped":   m.grantInheritanceSkippedTotal.Load(),
			"submitted": m.grantInheritanceSubmittedTotal.Load(),
		},
		"guarantee_violations_total": map[string]any{
			"primacy":     m.guaranteePrimacyTotal.Load(),
			"inheritance": m.guaranteeInheritanceTotal.Load(),
			"silence":     m.guaranteeSilenceTotal.Load(),
			"finality":    m.guaranteeFinalityTotal.Load(),
		},
	}
}

func (m *eunomiaMetrics) recordGrantInheritanceSkipped() {
	m.grantInheritanceSkippedTotal.Add(1)
}

func (m *eunomiaMetrics) recordGrantInheritanceSubmitted() {
	m.grantInheritanceSubmittedTotal.Add(1)
}

func (m *eunomiaMetrics) recordGrantEraTransition(before, after uint64) {
	if after == 0 || before == after {
		return
	}
	m.grantEraTransitionsTotal.Add(1)
}

func (m *eunomiaMetrics) recordGateRejection(kind gateKind) {
	switch kind {
	case gateDutyAdmission:
		m.gateDutyAdmissionRejectedTotal.Add(1)
	}
}

func (m *eunomiaMetrics) recordGuaranteeViolation(kind coordinatorGuaranteeViolation) {
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

func (m *eunomiaMetrics) recordGuaranteeViolationForError(err error) {
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
