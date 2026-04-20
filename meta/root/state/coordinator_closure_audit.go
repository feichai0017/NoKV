package state

import rootproto "github.com/feichai0017/NoKV/meta/root/protocol"

// CoordinatorClosureAudit captures the current rooted closure relationship
// between a predecessor seal and the currently installed lease state.
type CoordinatorClosureAudit struct {
	SealGeneration            uint64
	SealDigest                string
	SuccessorPresent          bool
	SuccessorCoverage         CoordinatorSuccessorCoverageStatus
	SuccessorLineageSatisfied bool
	SealedGenerationRetired   bool
}

type CoordinatorClosureStage = rootproto.CoordinatorClosureStage

const (
	CoordinatorClosureStagePendingConfirm = rootproto.CoordinatorClosureStagePendingConfirm
	CoordinatorClosureStageConfirmed      = rootproto.CoordinatorClosureStageConfirmed
	CoordinatorClosureStageClosed         = rootproto.CoordinatorClosureStageClosed
	CoordinatorClosureStageReattached     = rootproto.CoordinatorClosureStageReattached
)

// CoordinatorClosureStatus is the implementation-neutral projection of the
// rooted closure lifecycle.
type CoordinatorClosureStatus = rootproto.CoordinatorClosureStatus

func (a CoordinatorClosureAudit) ClosureSatisfied() bool {
	return a.AsClosureWitness(CoordinatorClosureStagePendingConfirm).ClosureSatisfied()
}

func (a CoordinatorClosureAudit) SuccessorMonotoneCovered() bool {
	return a.AsClosureWitness(CoordinatorClosureStagePendingConfirm).SuccessorMonotoneCovered()
}

func (a CoordinatorClosureAudit) SuccessorDescriptorCovered() bool {
	return a.AsClosureWitness(CoordinatorClosureStagePendingConfirm).SuccessorDescriptorCovered()
}

func (a CoordinatorClosureAudit) ReplyGenerationLegal(certGeneration uint64) bool {
	return a.AsClosureWitness(CoordinatorClosureStagePendingConfirm).ReplyGenerationLegal(certGeneration)
}

func (a CoordinatorClosureAudit) AsClosureWitness(stage CoordinatorClosureStage) ClosureWitness {
	return ClosureWitness{
		SealGeneration:            a.SealGeneration,
		SealDigest:                a.SealDigest,
		SuccessorPresent:          a.SuccessorPresent,
		SuccessorCoverage:         a.SuccessorCoverage,
		SuccessorLineageSatisfied: a.SuccessorLineageSatisfied,
		SealedGenerationRetired:   a.SealedGenerationRetired,
		Stage:                     stage,
	}
}
