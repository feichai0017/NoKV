package controlplane_test

import (
	"testing"

	controlplane "github.com/feichai0017/NoKV/coordinator/protocol/controlplane"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestHandoffRecordProjectsLeaseFrontiers(t *testing.T) {
	lease := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		IssuedCursor:      rootstate.Cursor{Term: 2, Index: 9},
		DutyMask:          rootproto.CoordinatorDutyMaskDefault,
		PredecessorDigest: "seal-digest",
	}

	handoff := controlplane.HandoffRecord(lease, controlplane.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65))
	require.Equal(t, "c1", handoff.HolderID)
	require.Equal(t, uint64(8), handoff.CertGeneration)
	require.Equal(t, uint64(30), handoff.Frontiers.Frontier(rootproto.CoordinatorDutyAllocID))
	require.Equal(t, uint64(50), handoff.Frontiers.Frontier(rootproto.CoordinatorDutyTSO))
	require.Equal(t, uint64(65), handoff.Frontiers.Frontier(rootproto.CoordinatorDutyGetRegionByKey))
}

func TestBuildClosureWitness(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		PredecessorDigest: "seal-digest",
	}
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 9},
	}
	expectedDigest := rootstate.CoordinatorSealDigest(seal)
	current.PredecessorDigest = expectedDigest

	witness := controlplane.BuildClosureWitness(current, controlplane.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65), seal, 1_000)
	require.Equal(t, uint64(7), witness.SealGeneration)
	require.Equal(t, expectedDigest, witness.SealDigest)
	require.True(t, witness.SuccessorPresent)
	require.Len(t, witness.SuccessorCoverage.Checks, 3)
	require.True(t, witness.SuccessorCoverage.Covered())
	require.True(t, witness.SuccessorLineageSatisfied)
	require.True(t, witness.SuccessorMonotoneCovered())
	require.True(t, witness.SuccessorDescriptorCovered())
	require.True(t, witness.SealedGenerationRetired)
	require.True(t, witness.ClosureSatisfied())
	require.False(t, witness.ReplyGenerationLegal(7))
	require.True(t, witness.ReplyGenerationLegal(8))
	require.False(t, witness.ReplyGenerationLegal(rootproto.ContinuationWitnessGenerationSuppressed))

	currentSameGen := rootstate.CoordinatorLease{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		CertGeneration:  7,
	}
	witness = controlplane.BuildClosureWitness(currentSameGen, controlplane.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60), seal, 1_000)
	require.False(t, witness.SuccessorPresent)
	require.False(t, witness.ClosureSatisfied())
	require.False(t, witness.ReplyGenerationLegal(7))
}

func TestBuildClosureWitnessForClosure(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		PredecessorDigest: "seal-digest",
	}
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	current.PredecessorDigest = sealDigest
	closure := rootstate.CoordinatorClosure{
		HolderID:            "c1",
		SealGeneration:      7,
		SuccessorGeneration: 8,
		SealDigest:          sealDigest,
		Stage:               rootproto.CoordinatorClosureStageReattached,
	}

	witness := controlplane.BuildClosureWitnessForClosure(current, controlplane.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65), seal, closure, "c1", 1_000)
	require.Equal(t, rootproto.CoordinatorClosureStageReattached, witness.Stage)
	require.True(t, witness.SuccessorCoverage.Covered())
	require.True(t, witness.SuccessorCoverage.CoveredDutyMask(rootproto.CoordinatorDutyAllocID|rootproto.CoordinatorDutyTSO))
	require.True(t, witness.SuccessorCoverage.CoveredDutyMask(rootproto.CoordinatorDutyGetRegionByKey))
	require.True(t, witness.ClosureSatisfied())
}

func TestValidateClosureTransitions(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		PredecessorDigest: "seal-digest",
	}
	confirmed := rootstate.CoordinatorClosure{
		HolderID:            "c1",
		SealGeneration:      7,
		SuccessorGeneration: 8,
		SealDigest:          "seal-digest",
		Stage:               rootproto.CoordinatorClosureStageConfirmed,
	}
	closed := confirmed
	closed.Stage = rootproto.CoordinatorClosureStageClosed

	require.NoError(t, controlplane.ValidateClosureClose(current, confirmed, "c1", 1_000))
	require.ErrorIs(t, controlplane.ValidateClosureClose(current, rootstate.CoordinatorClosure{}, "c1", 1_000), rootstate.ErrCoordinatorLeaseClose)
	require.NoError(t, controlplane.ValidateClosureReattach(current, closed, "c1", 1_000))
	require.ErrorIs(t, controlplane.ValidateClosureReattach(current, rootstate.CoordinatorClosure{}, "c1", 1_000), rootstate.ErrCoordinatorLeaseReattach)
	require.ErrorIs(t, controlplane.ValidateClosureReattach(current, confirmed, "c1", 1_000), rootstate.ErrCoordinatorLeaseReattach)
	require.ErrorIs(t, controlplane.ValidateClosureReattach(rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		PredecessorDigest: "other-digest",
	}, closed, "c1", 1_000), rootstate.ErrCoordinatorLeaseReattach)
	require.ErrorIs(t, controlplane.ValidateClosureReattach(current, rootstate.CoordinatorClosure{
		HolderID:            "c1",
		SealGeneration:      8,
		SuccessorGeneration: 8,
		SealDigest:          "seal-digest",
		Stage:               rootproto.CoordinatorClosureStageClosed,
	}, "c1", 1_000), rootstate.ErrCoordinatorLeaseReattach)
	require.ErrorIs(t, controlplane.ValidateClosureReattach(current, closed, "c2", 1_000), rootstate.ErrCoordinatorLeaseOwner)
	require.ErrorIs(t, controlplane.ValidateClosureReattach(rootstate.CoordinatorLease{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		CertGeneration:  8,
	}, closed, "c1", 1_000), rootstate.ErrInvalidCoordinatorLease)
}

func TestEvaluateClosureStage(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		PredecessorDigest: "seal-digest",
	}
	confirmed := rootstate.CoordinatorClosure{
		HolderID:            "c1",
		SealGeneration:      7,
		SuccessorGeneration: 8,
		SealDigest:          "seal-digest",
		Stage:               rootproto.CoordinatorClosureStageConfirmed,
	}
	status := controlplane.EvaluateClosureStage(current, confirmed, "c1", 1_000)
	require.Equal(t, rootproto.CoordinatorClosureStageConfirmed, status.Stage)

	closed := confirmed
	closed.Stage = rootproto.CoordinatorClosureStageClosed
	status = controlplane.EvaluateClosureStage(current, closed, "c1", 1_000)
	require.Equal(t, rootproto.CoordinatorClosureStageClosed, status.Stage)

	reattached := closed
	reattached.Stage = rootproto.CoordinatorClosureStageReattached
	status = controlplane.EvaluateClosureStage(current, reattached, "c1", 1_000)
	require.Equal(t, rootproto.CoordinatorClosureStageReattached, status.Stage)

	status = controlplane.EvaluateClosureStage(current, rootstate.CoordinatorClosure{}, "c1", 1_000)
	require.Equal(t, rootproto.CoordinatorClosureStageUnspecified, status.Stage)
}

func TestValidateClosureConfirmationAndAdvanceClosure(t *testing.T) {
	current := rootstate.CoordinatorLease{
		HolderID:          "c1",
		ExpiresUnixNano:   2_000,
		CertGeneration:    8,
		PredecessorDigest: "seal-digest",
	}
	seal := rootstate.CoordinatorSeal{
		HolderID:       "c1",
		CertGeneration: 7,
		DutyMask:       rootproto.CoordinatorDutyMaskDefault,
		Frontiers:      controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
		SealedAtCursor: rootstate.Cursor{Term: 1, Index: 5},
	}
	sealDigest := rootstate.CoordinatorSealDigest(seal)
	current.PredecessorDigest = sealDigest

	witness, err := controlplane.ValidateClosureConfirmation(
		current,
		controlplane.Frontiers(rootstate.State{IDFence: 11, TSOFence: 21}, 31),
		seal,
		1_000,
	)
	require.NoError(t, err)
	require.True(t, witness.ClosureSatisfied())

	_, err = controlplane.ValidateClosureConfirmation(
		rootstate.CoordinatorLease{
			HolderID:        "c1",
			ExpiresUnixNano: 2_000,
			CertGeneration:  7,
		},
		controlplane.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
		seal,
		1_000,
	)
	require.ErrorIs(t, err, rootstate.ErrCoordinatorLeaseAudit)

	confirmed := controlplane.AdvanceClosure(
		current,
		rootstate.CoordinatorClosure{},
		rootproto.CoordinatorClosureStageConfirmed,
		"c1",
		7,
		sealDigest,
		rootstate.Cursor{Term: 2, Index: 9},
	)
	require.Equal(t, rootproto.CoordinatorClosureStageConfirmed, confirmed.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 9}, confirmed.ConfirmedAtCursor)
	require.Equal(t, uint64(8), confirmed.SuccessorGeneration)

	closed := controlplane.AdvanceClosure(
		current,
		confirmed,
		rootproto.CoordinatorClosureStageClosed,
		"c1",
		7,
		sealDigest,
		rootstate.Cursor{Term: 2, Index: 10},
	)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, closed.ClosedAtCursor)
	require.Equal(t, rootstate.Cursor{}, closed.ReattachedAtCursor)

	reattached := controlplane.AdvanceClosure(
		current,
		closed,
		rootproto.CoordinatorClosureStageReattached,
		"c1",
		7,
		sealDigest,
		rootstate.Cursor{Term: 2, Index: 11},
	)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 11}, reattached.ReattachedAtCursor)
}
