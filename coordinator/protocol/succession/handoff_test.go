package succession_test

import (
	"testing"

	succession "github.com/feichai0017/NoKV/coordinator/protocol/succession"
	rootproto "github.com/feichai0017/NoKV/meta/root/protocol"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
)

func TestHandoffRecordProjectsLeaseFrontiers(t *testing.T) {
	lease := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           8,
		IssuedAt:        rootstate.Cursor{Term: 2, Index: 9},
		Mandate:         rootproto.MandateDefault,
		LineageDigest:   "seal-digest",
	}

	handoff := succession.HandoffRecord(lease, succession.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65))
	require.Equal(t, "c1", handoff.HolderID)
	require.Equal(t, uint64(8), handoff.Epoch)
	require.Equal(t, uint64(30), handoff.Frontiers.Frontier(rootproto.MandateAllocID))
	require.Equal(t, uint64(50), handoff.Frontiers.Frontier(rootproto.MandateTSO))
	require.Equal(t, uint64(65), handoff.Frontiers.Frontier(rootproto.MandateGetRegionByKey))
}

func TestBuildHandoverWitness(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           8,
		LineageDigest:   "seal-digest",
	}
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Epoch:     7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
		SealedAt:  rootstate.Cursor{Term: 1, Index: 9},
	}
	expectedDigest := rootstate.DigestOfLegacy(seal)
	current.LineageDigest = expectedDigest

	witness := succession.BuildHandoverWitness(current, succession.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65), seal, 1_000)
	require.Equal(t, uint64(7), witness.LegacyEpoch)
	require.Equal(t, expectedDigest, witness.LegacyDigest)
	require.True(t, witness.SuccessorPresent)
	require.Len(t, witness.Inheritance.Checks, 3)
	require.True(t, witness.Inheritance.Covered())
	require.True(t, witness.SuccessorLineageSatisfied)
	require.True(t, witness.SuccessorMonotoneCovered())
	require.True(t, witness.SuccessorDescriptorCovered())
	require.True(t, witness.SealedGenerationRetired)
	require.True(t, witness.FinalitySatisfied())
	require.False(t, witness.ReplyGenerationLegal(7))
	require.True(t, witness.ReplyGenerationLegal(8))
	require.False(t, witness.ReplyGenerationLegal(rootproto.ContinuationWitnessGenerationSuppressed))

	currentSameGen := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           7,
	}
	witness = succession.BuildHandoverWitness(currentSameGen, succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60), seal, 1_000)
	require.False(t, witness.SuccessorPresent)
	require.False(t, witness.FinalitySatisfied())
	require.False(t, witness.ReplyGenerationLegal(7))
}

func TestBuildHandoverWitnessForStage(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           8,
		LineageDigest:   "seal-digest",
	}
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Epoch:     7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: succession.Frontiers(rootstate.State{IDFence: 20, TSOFence: 40}, 60),
	}
	legacyDigest := rootstate.DigestOfLegacy(seal)
	current.LineageDigest = legacyDigest
	handover := rootstate.Handover{
		HolderID:       "c1",
		LegacyEpoch:    7,
		SuccessorEpoch: 8,
		LegacyDigest:   legacyDigest,
		Stage:          rootproto.HandoverStageReattached,
	}

	witness := succession.BuildHandoverWitnessForStage(current, succession.Frontiers(rootstate.State{IDFence: 30, TSOFence: 50}, 65), seal, handover, "c1", 1_000)
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
		Epoch:           8,
		LineageDigest:   "seal-digest",
	}
	confirmed := rootstate.Handover{
		HolderID:       "c1",
		LegacyEpoch:    7,
		SuccessorEpoch: 8,
		LegacyDigest:   "seal-digest",
		Stage:          rootproto.HandoverStageConfirmed,
	}
	closed := confirmed
	closed.Stage = rootproto.HandoverStageClosed

	require.NoError(t, succession.ValidateHandoverFinality(current, confirmed, "c1", 1_000))
	require.ErrorIs(t, succession.ValidateHandoverFinality(current, rootstate.Handover{}, "c1", 1_000), rootstate.ErrFinality)
	require.NoError(t, succession.ValidateHandoverReattach(current, closed, "c1", 1_000))
	require.ErrorIs(t, succession.ValidateHandoverReattach(current, rootstate.Handover{}, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, succession.ValidateHandoverReattach(current, confirmed, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, succession.ValidateHandoverReattach(rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           8,
		LineageDigest:   "other-digest",
	}, closed, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, succession.ValidateHandoverReattach(current, rootstate.Handover{
		HolderID:       "c1",
		LegacyEpoch:    8,
		SuccessorEpoch: 8,
		LegacyDigest:   "seal-digest",
		Stage:          rootproto.HandoverStageClosed,
	}, "c1", 1_000), rootstate.ErrFinality)
	require.ErrorIs(t, succession.ValidateHandoverReattach(current, closed, "c2", 1_000), rootstate.ErrPrimacy)
	require.ErrorIs(t, succession.ValidateHandoverReattach(rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 1_000,
		Epoch:           8,
	}, closed, "c1", 1_000), rootstate.ErrInvalidTenure)
}

func TestEvaluateHandoverStage(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           8,
		LineageDigest:   "seal-digest",
	}
	confirmed := rootstate.Handover{
		HolderID:       "c1",
		LegacyEpoch:    7,
		SuccessorEpoch: 8,
		LegacyDigest:   "seal-digest",
		Stage:          rootproto.HandoverStageConfirmed,
	}
	status := succession.EvaluateHandoverStage(current, confirmed, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageConfirmed, status.Stage)

	closed := confirmed
	closed.Stage = rootproto.HandoverStageClosed
	status = succession.EvaluateHandoverStage(current, closed, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageClosed, status.Stage)

	reattached := closed
	reattached.Stage = rootproto.HandoverStageReattached
	status = succession.EvaluateHandoverStage(current, reattached, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageReattached, status.Stage)

	status = succession.EvaluateHandoverStage(current, rootstate.Handover{}, "c1", 1_000)
	require.Equal(t, rootproto.HandoverStageUnspecified, status.Stage)
}

func TestValidateHandoverConfirmationAndAdvanceHandover(t *testing.T) {
	current := rootstate.Tenure{
		HolderID:        "c1",
		ExpiresUnixNano: 2_000,
		Epoch:           8,
		LineageDigest:   "seal-digest",
	}
	seal := rootstate.Legacy{
		HolderID:  "c1",
		Epoch:     7,
		Mandate:   rootproto.MandateDefault,
		Frontiers: succession.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
		SealedAt:  rootstate.Cursor{Term: 1, Index: 5},
	}
	legacyDigest := rootstate.DigestOfLegacy(seal)
	current.LineageDigest = legacyDigest

	witness, err := succession.ValidateHandoverConfirmation(
		current,
		succession.Frontiers(rootstate.State{IDFence: 11, TSOFence: 21}, 31),
		seal,
		1_000,
	)
	require.NoError(t, err)
	require.True(t, witness.FinalitySatisfied())

	_, err = succession.ValidateHandoverConfirmation(
		rootstate.Tenure{
			HolderID:        "c1",
			ExpiresUnixNano: 2_000,
			Epoch:           7,
		},
		succession.Frontiers(rootstate.State{IDFence: 10, TSOFence: 20}, 30),
		seal,
		1_000,
	)
	require.ErrorIs(t, err, rootstate.ErrFinality)

	confirmed := succession.AdvanceHandover(
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
	require.Equal(t, uint64(8), confirmed.SuccessorEpoch)

	closed := succession.AdvanceHandover(
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

	reattached := succession.AdvanceHandover(
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
