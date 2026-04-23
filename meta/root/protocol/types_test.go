package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMandateFrontiersHelpers(t *testing.T) {
	frontiers := NewMandateFrontiers(
		MandateFrontier{Mandate: MandateAllocID, Frontier: 10},
		MandateFrontier{Mandate: MandateGetRegionByKey, Frontier: 30},
	)

	require.Equal(t, uint64(10), frontiers.Frontier(MandateAllocID))
	require.Zero(t, frontiers.Frontier(MandateTSO))
	require.Equal(t, 2, frontiers.Len())
	require.Equal(t,
		[]MandateFrontier{
			{Mandate: MandateAllocID, Frontier: 10},
			{Mandate: MandateGetRegionByKey, Frontier: 30},
		},
		frontiers.Entries(),
	)
	require.Equal(t, map[uint32]uint64{
		MandateAllocID:        10,
		MandateGetRegionByKey: 30,
	}, frontiers.AsMap())

	require.Equal(
		t,
		[]uint32{MandateAllocID, MandateTSO, MandateGetRegionByKey},
		OrderedMandateMasks(MandateTSO, frontiers),
	)

	unchanged := frontiers.WithFrontier(1<<30, 99)
	require.Equal(t, frontiers, unchanged)

	fromMap := MandateFrontiersFromMap(map[uint32]uint64{
		MandateTSO:            20,
		MandateGetRegionByKey: 30,
		MandateLeaseStart:     40,
	})
	require.Equal(t, uint64(20), fromMap.Frontier(MandateTSO))
	require.Equal(t, 3, fromMap.Len())
	require.Equal(t, "alloc_id", MandateName(MandateAllocID))
	require.Equal(t, "mandate_999", MandateName(999))

	idx, ok := mandateIndex(MandateLeaseStart)
	require.True(t, ok)
	require.Equal(t, 3, idx)
	_, ok = mandateIndex(1 << 30)
	require.False(t, ok)
}

func TestAuthorityHandoffRecordValidation(t *testing.T) {
	empty, err := NewAuthorityHandoffRecord("", 0, 0, Cursor{}, 0, "", MandateFrontiers{})
	require.NoError(t, err)
	require.False(t, empty.Present())

	_, err = NewAuthorityHandoffRecord("", 1, 1, Cursor{Term: 1, Index: 1}, MandateAllocID, "", MandateFrontiers{})
	require.ErrorContains(t, err, "holder id is required")

	_, err = NewAuthorityHandoffRecord("holder", 1, 0, Cursor{}, MandateAllocID, "", NewMandateFrontiers(MandateFrontier{
		Mandate:  MandateAllocID,
		Frontier: 10,
	}))
	require.ErrorContains(t, err, "cert generation is required")

	_, err = NewAuthorityHandoffRecord("holder", 1, 1, Cursor{}, 0, "", MandateFrontiers{})
	require.ErrorContains(t, err, "duty mask is required")

	_, err = NewAuthorityHandoffRecord(
		"holder",
		1,
		1,
		Cursor{Term: 1, Index: 2},
		MandateAllocID|MandateTSO,
		"pred",
		NewMandateFrontiers(MandateFrontier{Mandate: MandateAllocID, Frontier: 10}),
	)
	require.ErrorContains(t, err, "frontiers must cover all duty mask bits")

	record, err := NewAuthorityHandoffRecord(
		" holder ",
		100,
		7,
		Cursor{Term: 2, Index: 5},
		MandateAllocID|MandateTSO|(1<<30),
		" pred ",
		NewMandateFrontiers(
			MandateFrontier{Mandate: MandateAllocID, Frontier: 10},
			MandateFrontier{Mandate: MandateTSO, Frontier: 20},
		),
	)
	require.NoError(t, err)
	require.True(t, record.Present())
	require.Equal(t, "holder", record.HolderID)
	require.Equal(t, "pred", record.LineageDigest)
	require.Equal(t, MandateAllocID|MandateTSO, record.Mandate)

	require.Panics(t, func() {
		_ = MustNewAuthorityHandoffRecord("holder", 0, 0, Cursor{}, MandateAllocID, "", MandateFrontiers{})
	})
}

func TestCoverageAndHandoverWitnessHelpers(t *testing.T) {
	coverage := InheritanceStatus{
		Checks: []InheritanceCoverage{
			{
				Mandate:          MandateAllocID,
				RequiredFrontier: 10,
				ActualFrontier:   12,
				Covered:          true,
			},
			{
				Mandate:          MandateTSO,
				RequiredFrontier: 20,
				ActualFrontier:   19,
				Covered:          false,
			},
			{
				Mandate:          MandateGetRegionByKey,
				RequiredFrontier: 30,
				ActualFrontier:   30,
				Covered:          true,
			},
		},
	}

	require.False(t, coverage.Covered())
	require.False(t, coverage.CoveredMandate(MandateAllocID|MandateTSO))
	require.True(t, coverage.CoveredMandate(MandateGetRegionByKey))
	gap, ok := coverage.FirstGap()
	require.True(t, ok)
	require.Equal(t, MandateTSO, gap.Mandate)

	allCovered := InheritanceStatus{
		Checks: []InheritanceCoverage{
			{Mandate: MandateAllocID, Covered: true},
			{Mandate: MandateTSO, Covered: true},
			{Mandate: MandateGetRegionByKey, Covered: true},
		},
	}
	witness := HandoverWitness{
		LegacyEpoch:               9,
		LegacyDigest:              "seal",
		SuccessorPresent:          true,
		Inheritance:               allCovered,
		SuccessorLineageSatisfied: true,
		SealedGenerationRetired:   true,
	}
	require.True(t, witness.FinalitySatisfied())
	require.True(t, witness.SuccessorMonotoneCovered())
	require.True(t, witness.SuccessorDescriptorCovered())
	require.True(t, witness.ReplyGenerationLegal(0))
	require.False(t, witness.ReplyGenerationLegal(ContinuationWitnessGenerationSuppressed))
	require.False(t, witness.ReplyGenerationLegal(witness.LegacyEpoch))
	require.True(t, witness.ReplyGenerationLegal(witness.LegacyEpoch+1))
	require.Equal(t, HandoverStageClosed, witness.WithStage(HandoverStageClosed).Stage)

	witness.SuccessorPresent = false
	require.False(t, witness.FinalitySatisfied())
	require.False(t, witness.SuccessorMonotoneCovered())
	require.False(t, witness.SuccessorDescriptorCovered())

	attached := NewContinuationWitness(MandateAllocID, 3, 99)
	require.Equal(t, uint64(3), attached.Epoch)
	suppressed := NewSuppressedContinuationWitness(MandateTSO)
	require.Equal(t, ContinuationWitnessGenerationSuppressed, suppressed.Epoch)

	require.Equal(t, "pending_confirm", HandoverStagePendingConfirm.String())
	require.Equal(t, "unknown", HandoverStage(99).String())
	require.True(t, HandoverStageAtLeast(HandoverStageClosed, HandoverStageConfirmed))
	require.False(t, HandoverStageAtLeast(HandoverStageConfirmed, HandoverStageClosed))
}
