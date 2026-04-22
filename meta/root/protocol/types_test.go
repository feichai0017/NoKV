package protocol

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCoordinatorDutyFrontiersHelpers(t *testing.T) {
	frontiers := NewCoordinatorDutyFrontiers(
		CoordinatorDutyFrontier{DutyMask: CoordinatorDutyAllocID, Frontier: 10},
		CoordinatorDutyFrontier{DutyMask: CoordinatorDutyGetRegionByKey, Frontier: 30},
	)

	require.Equal(t, uint64(10), frontiers.Frontier(CoordinatorDutyAllocID))
	require.Zero(t, frontiers.Frontier(CoordinatorDutyTSO))
	require.Equal(t, 2, frontiers.Len())
	require.Equal(t,
		[]CoordinatorDutyFrontier{
			{DutyMask: CoordinatorDutyAllocID, Frontier: 10},
			{DutyMask: CoordinatorDutyGetRegionByKey, Frontier: 30},
		},
		frontiers.Entries(),
	)
	require.Equal(t, map[uint32]uint64{
		CoordinatorDutyAllocID:        10,
		CoordinatorDutyGetRegionByKey: 30,
	}, frontiers.AsMap())

	require.Equal(
		t,
		[]uint32{CoordinatorDutyAllocID, CoordinatorDutyTSO, CoordinatorDutyGetRegionByKey},
		OrderedCoordinatorDutyMasks(CoordinatorDutyTSO, frontiers),
	)

	unchanged := frontiers.WithFrontier(1<<30, 99)
	require.Equal(t, frontiers, unchanged)

	fromMap := CoordinatorDutyFrontiersFromMap(map[uint32]uint64{
		CoordinatorDutyTSO:            20,
		CoordinatorDutyGetRegionByKey: 30,
		CoordinatorDutyLeaseStart:     40,
	})
	require.Equal(t, uint64(20), fromMap.Frontier(CoordinatorDutyTSO))
	require.Equal(t, 3, fromMap.Len())
	require.Equal(t, "alloc_id", CoordinatorDutyName(CoordinatorDutyAllocID))
	require.Equal(t, "duty_999", CoordinatorDutyName(999))

	idx, ok := coordinatorDutyFrontierIndex(CoordinatorDutyLeaseStart)
	require.True(t, ok)
	require.Equal(t, 3, idx)
	_, ok = coordinatorDutyFrontierIndex(1 << 30)
	require.False(t, ok)
}

func TestAuthorityHandoffRecordValidation(t *testing.T) {
	empty, err := NewAuthorityHandoffRecord("", 0, 0, Cursor{}, 0, "", CoordinatorDutyFrontiers{})
	require.NoError(t, err)
	require.False(t, empty.Present())

	_, err = NewAuthorityHandoffRecord("", 1, 1, Cursor{Term: 1, Index: 1}, CoordinatorDutyAllocID, "", CoordinatorDutyFrontiers{})
	require.ErrorContains(t, err, "holder id is required")

	_, err = NewAuthorityHandoffRecord("holder", 1, 0, Cursor{}, CoordinatorDutyAllocID, "", NewCoordinatorDutyFrontiers(CoordinatorDutyFrontier{
		DutyMask: CoordinatorDutyAllocID,
		Frontier: 10,
	}))
	require.ErrorContains(t, err, "cert generation is required")

	_, err = NewAuthorityHandoffRecord("holder", 1, 1, Cursor{}, 0, "", CoordinatorDutyFrontiers{})
	require.ErrorContains(t, err, "duty mask is required")

	_, err = NewAuthorityHandoffRecord(
		"holder",
		1,
		1,
		Cursor{Term: 1, Index: 2},
		CoordinatorDutyAllocID|CoordinatorDutyTSO,
		"pred",
		NewCoordinatorDutyFrontiers(CoordinatorDutyFrontier{DutyMask: CoordinatorDutyAllocID, Frontier: 10}),
	)
	require.ErrorContains(t, err, "frontiers must cover all duty mask bits")

	record, err := NewAuthorityHandoffRecord(
		" holder ",
		100,
		7,
		Cursor{Term: 2, Index: 5},
		CoordinatorDutyAllocID|CoordinatorDutyTSO|(1<<30),
		" pred ",
		NewCoordinatorDutyFrontiers(
			CoordinatorDutyFrontier{DutyMask: CoordinatorDutyAllocID, Frontier: 10},
			CoordinatorDutyFrontier{DutyMask: CoordinatorDutyTSO, Frontier: 20},
		),
	)
	require.NoError(t, err)
	require.True(t, record.Present())
	require.Equal(t, "holder", record.HolderID)
	require.Equal(t, "pred", record.PredecessorDigest)
	require.Equal(t, CoordinatorDutyAllocID|CoordinatorDutyTSO, record.DutyMask)

	require.Panics(t, func() {
		_ = MustNewAuthorityHandoffRecord("holder", 0, 0, Cursor{}, CoordinatorDutyAllocID, "", CoordinatorDutyFrontiers{})
	})
}

func TestCoverageAndClosureWitnessHelpers(t *testing.T) {
	coverage := CoordinatorSuccessorCoverageStatus{
		Checks: []CoordinatorFrontierCoverage{
			{
				DutyMask:         CoordinatorDutyAllocID,
				RequiredFrontier: 10,
				ActualFrontier:   12,
				Covered:          true,
			},
			{
				DutyMask:         CoordinatorDutyTSO,
				RequiredFrontier: 20,
				ActualFrontier:   19,
				Covered:          false,
			},
			{
				DutyMask:         CoordinatorDutyGetRegionByKey,
				RequiredFrontier: 30,
				ActualFrontier:   30,
				Covered:          true,
			},
		},
	}

	require.False(t, coverage.Covered())
	require.False(t, coverage.CoveredDutyMask(CoordinatorDutyAllocID|CoordinatorDutyTSO))
	require.True(t, coverage.CoveredDutyMask(CoordinatorDutyGetRegionByKey))
	gap, ok := coverage.FirstGap()
	require.True(t, ok)
	require.Equal(t, CoordinatorDutyTSO, gap.DutyMask)

	allCovered := CoordinatorSuccessorCoverageStatus{
		Checks: []CoordinatorFrontierCoverage{
			{DutyMask: CoordinatorDutyAllocID, Covered: true},
			{DutyMask: CoordinatorDutyTSO, Covered: true},
			{DutyMask: CoordinatorDutyGetRegionByKey, Covered: true},
		},
	}
	witness := ClosureWitness{
		SealGeneration:            9,
		SealDigest:                "seal",
		SuccessorPresent:          true,
		SuccessorCoverage:         allCovered,
		SuccessorLineageSatisfied: true,
		SealedGenerationRetired:   true,
	}
	require.True(t, witness.ClosureSatisfied())
	require.True(t, witness.SuccessorMonotoneCovered())
	require.True(t, witness.SuccessorDescriptorCovered())
	require.True(t, witness.ReplyGenerationLegal(0))
	require.False(t, witness.ReplyGenerationLegal(ContinuationWitnessGenerationSuppressed))
	require.False(t, witness.ReplyGenerationLegal(witness.SealGeneration))
	require.True(t, witness.ReplyGenerationLegal(witness.SealGeneration+1))
	require.Equal(t, CoordinatorClosureStageClosed, witness.WithStage(CoordinatorClosureStageClosed).Stage)

	witness.SuccessorPresent = false
	require.False(t, witness.ClosureSatisfied())
	require.False(t, witness.SuccessorMonotoneCovered())
	require.False(t, witness.SuccessorDescriptorCovered())

	attached := NewContinuationWitness(CoordinatorDutyAllocID, 3, 99)
	require.Equal(t, uint64(3), attached.CertGeneration)
	suppressed := NewSuppressedContinuationWitness(CoordinatorDutyTSO)
	require.Equal(t, ContinuationWitnessGenerationSuppressed, suppressed.CertGeneration)

	require.Equal(t, "pending_confirm", CoordinatorClosureStagePendingConfirm.String())
	require.Equal(t, "unknown", CoordinatorClosureStage(99).String())
	require.True(t, ClosureStageAtLeast(CoordinatorClosureStageClosed, CoordinatorClosureStageConfirmed))
	require.False(t, ClosureStageAtLeast(CoordinatorClosureStageConfirmed, CoordinatorClosureStageClosed))
}
