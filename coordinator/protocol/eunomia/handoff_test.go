package eunomia_test

import (
	"testing"

	eunomia "github.com/feichai0017/NoKV/coordinator/protocol/eunomia"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestHandoffRecordProjectsLeaseFrontiers(t *testing.T) {
	lease := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		IssuedAt:        rootstate.Cursor{Term: 2, Index: 9},
		Mandate:         rootproto.MandateDefault,
		LineageDigest:   "seal-digest",
	}

	handoff := eunomia.HandoffRecord(lease, eunomia.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65))
	require.Equal(t, "c1", handoff.HolderID)
	require.Equal(t, uint64(8), handoff.Era)
	require.Equal(t, uint64(30), handoff.Frontiers.Frontier(rootproto.MandateAllocID))
	require.Equal(t, uint64(50), handoff.Frontiers.Frontier(rootproto.MandateTSO))
	require.Equal(t, uint64(65), handoff.Frontiers.Frontier(rootproto.MandateGetRegionByKey))
}

func TestBuildHandoverWitness(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		LineageDigest:   "seal-digest",
	}
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Era:       7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: eunomia.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
		SealedAt:  rootstate.Cursor{Term: 1, Index: 9},
	}
	expectedDigest := rootstate.DigestOfLegacy(seal)
	current.LineageDigest = expectedDigest

	witness := eunomia.BuildHandoverWitness(current, eunomia.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65), seal, 1_000)
	require.Equal(t, uint64(7), witness.LegacyEra)
	require.Equal(t, expectedDigest, witness.LegacyDigest)
	require.True(t, witness.SuccessorPresent)
	require.Len(t, witness.Inheritance.Checks, 3)
	require.True(t, witness.Inheritance.Covered())
	require.True(t, witness.SuccessorLineageSatisfied)
	require.True(t, witness.SuccessorMonotoneCovered())
	require.True(t, witness.SuccessorDescriptorCovered())
	require.True(t, witness.SealedEraRetired)
	require.True(t, witness.FinalitySatisfied())
	require.False(t, witness.ReplyEraLegal(7))
	require.True(t, witness.ReplyEraLegal(8))
	require.False(t, witness.ReplyEraLegal(rootproto.MandateWitnessEraSuppressed))

	currentSameGen := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             7,
	}
	witness = eunomia.BuildHandoverWitness(currentSameGen, eunomia.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60), seal, 1_000)
	require.False(t, witness.SuccessorPresent)
	require.False(t, witness.FinalitySatisfied())
	require.False(t, witness.ReplyEraLegal(7))
}

func TestBuildHandoverWitnessForStage(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		LineageDigest:   "seal-digest",
	}
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Era:       7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: eunomia.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
	}
	legacyDigest := rootstate.DigestOfLegacy(seal)
	current.LineageDigest = legacyDigest
	handover := rootstate.Handover{
		HolderID:     "c1",
		LegacyEra:    7,
		SuccessorEra: 8,
		LegacyDigest: legacyDigest,
		Stage:        rootproto.HandoverStageReattached,
	}

	witness := eunomia.BuildHandoverWitnessForStage(current, eunomia.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65), seal, handover, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageReattached, witness.Stage)
	require.True(t, witness.Inheritance.Covered())
	require.True(t, witness.Inheritance.CoveredMandate(rootproto.MandateAllocID|rootproto.MandateTSO))
	require.True(t, witness.Inheritance.CoveredMandate(rootproto.MandateGetRegionByKey))
	require.True(t, witness.FinalitySatisfied())
}

func TestValidateClosureTransitions(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		LineageDigest:   "seal-digest",
	}
	confirmed := rootstate.Handover{
		HolderID:     "c1",
		LegacyEra:    7,
		SuccessorEra: 8,
		LegacyDigest: "seal-digest",
		Stage:        rootproto.HandoverStageConfirmed,
	}
	closed := confirmed
	closed.Stage = rootproto.HandoverStageClosed

	require.NoError(t, eunomia.ValidateHandoverFinality(current, confirmed, "c1", 1_000))
	require.ErrorIs(t, eunomia.ValidateHandoverFinality(current, rootstate.Handover{}, "c1", 1_000), rootstate.ErrFinality)
	require.NoError(t, eunomia.ValidateHandoverReattach(current, closed, "c1", 1_000))
	require.ErrorIs(t, eunomia.ValidateHandoverReattach(current, rootstate.Handover{}, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, eunomia.ValidateHandoverReattach(current, confirmed, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, eunomia.ValidateHandoverReattach(rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		LineageDigest:   "other-digest",
	}, closed, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, eunomia.ValidateHandoverReattach(current, rootstate.Handover{
		HolderID:     "c1",
		LegacyEra:    8,
		SuccessorEra: 8,
		LegacyDigest: "seal-digest",
		Stage:        rootproto.HandoverStageClosed,
	}, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, eunomia.ValidateHandoverReattach(current, closed, "c2", 1_000), rootstate.ErrPrimacy)
	require.ErrorIs(t, eunomia.ValidateHandoverReattach(rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		Era:             8,
	}, closed, "c1", 1_000), rootstate.ErrInvalidTenure)
}

func TestEvaluateHandoverStage(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		LineageDigest:   "seal-digest",
	}
	confirmed := rootstate.Handover{
		HolderID:     "c1",
		LegacyEra:    7,
		SuccessorEra: 8,
		LegacyDigest: "seal-digest",
		Stage:        rootproto.HandoverStageConfirmed,
	}
	status := eunomia.EvaluateHandoverStage(current, confirmed, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageConfirmed, status.Stage)

	closed := confirmed
	closed.Stage = rootproto.HandoverStageClosed
	status = eunomia.EvaluateHandoverStage(current, closed, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageClosed, status.Stage)

	reattached := closed
	reattached.Stage = rootproto.HandoverStageReattached
	status = eunomia.EvaluateHandoverStage(current, reattached, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageReattached, status.Stage)

	status = eunomia.EvaluateHandoverStage(current, rootstate.Handover{}, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageUnspecified, status.Stage)
}

func TestValidateHandoverConfirmationAndAdvanceHandover(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Era:             8,
		LineageDigest:   "seal-digest",
	}
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Era:       7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: eunomia.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
		SealedAt:  rootstate.Cursor{Term: 1, Index: 5},
	}
	legacyDigest := rootstate.DigestOfLegacy(seal)
	current.LineageDigest = legacyDigest

	witness, err := eunomia.ValidateHandoverConfirmation(
		current,
		eunomia.Frontiers(rootstate.State{IDFence: 11, TSOFence: 21}, 31),
		seal,
		1_000,
	)
	require.NoError(t, err)
	require.True(t, witness.FinalitySatisfied())

	_, err = eunomia.ValidateHandoverConfirmation(
		rootstate.Tenure{
			HolderID:        "c1",
			ExpiresUnixNano: 2_000,
			Era:             7,
		},
		eunomia.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
		seal,
		1_000,
	)
	require.ErrorIs(t, err, rootstate.ErrFinality)

	confirmed := eunomia.AdvanceHandover(
		current,
		rootstate.Handover{},
		rootproto.HandoverStageConfirmed,
		"c1",
		7,
		legacyDigest,
		rootstate.Cursor{Term: 2, Index: 9},
	)
	require.Equal(t, rootproto.HandoverStageConfirmed, confirmed.Stage)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 9}, confirmed.ConfirmedAt)
	require.Equal(t, uint64(8), confirmed.SuccessorEra)

	closed := eunomia.AdvanceHandover(
		current,
		confirmed,
		rootproto.HandoverStageClosed,
		"c1",
		7,
		legacyDigest,
		rootstate.Cursor{Term: 2, Index: 10},
	)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 10}, closed.ClosedAt)
	require.Equal(t, rootstate.Cursor{}, closed.ReattachedAt)

	reattached := eunomia.AdvanceHandover(
		current,
		closed,
		rootproto.HandoverStageReattached,
		"c1",
		7,
		legacyDigest,
		rootstate.Cursor{Term: 2, Index: 11},
	)
	require.Equal(t, rootstate.Cursor{Term: 2, Index: 11}, reattached.ReattachedAt)
}
