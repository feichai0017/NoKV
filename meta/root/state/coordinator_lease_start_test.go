package state

import (
	"errors"
	"testing"
)

func TestValidateCoordinatorLeaseStartCoverageRejectsBelowFrontier(t *testing.T) {
	seal := CoordinatorSeal{
		HolderID:       "n1",
		CertGeneration: 1,
		DutyMask:       CoordinatorDutyMaskDefault,
		Frontiers: NewCoordinatorDutyFrontiers(
			CoordinatorDutyFrontier{DutyMask: CoordinatorDutyLeaseStart, Frontier: 15},
		),
	}

	if err := ValidateCoordinatorLeaseStartCoverage(seal, 15); !errors.Is(err, ErrCoordinatorLeaseCoverage) {
		t.Fatalf("lease_start=15 must be rejected (not strictly greater than frontier), got err=%v", err)
	}
	if err := ValidateCoordinatorLeaseStartCoverage(seal, 10); !errors.Is(err, ErrCoordinatorLeaseCoverage) {
		t.Fatalf("lease_start=10 must be rejected, got err=%v", err)
	}
	if err := ValidateCoordinatorLeaseStartCoverage(seal, 16); err != nil {
		t.Fatalf("lease_start=16 must be accepted, got err=%v", err)
	}
}

func TestValidateCoordinatorLeaseStartCoverageNoOpWhenSealAbsent(t *testing.T) {
	if err := ValidateCoordinatorLeaseStartCoverage(CoordinatorSeal{}, 0); err != nil {
		t.Fatalf("empty seal must be a no-op, got err=%v", err)
	}
}

func TestValidateCoordinatorLeaseStartCoverageNoOpWhenFrontierZero(t *testing.T) {
	seal := CoordinatorSeal{HolderID: "n1", CertGeneration: 1}
	if err := ValidateCoordinatorLeaseStartCoverage(seal, 0); err != nil {
		t.Fatalf("zero frontier must be a no-op, got err=%v", err)
	}
}

func TestCoordinatorSealWithServedFrontierMonotonic(t *testing.T) {
	seal := CoordinatorSeal{HolderID: "n1", CertGeneration: 1}

	updated := CoordinatorSealWithServedFrontier(seal, 10)
	if got := updated.Frontiers.Frontier(CoordinatorDutyLeaseStart); got != 10 {
		t.Fatalf("frontier must be 10, got %d", got)
	}

	bumped := CoordinatorSealWithServedFrontier(updated, 15)
	if got := bumped.Frontiers.Frontier(CoordinatorDutyLeaseStart); got != 15 {
		t.Fatalf("frontier must be 15, got %d", got)
	}

	lowered := CoordinatorSealWithServedFrontier(bumped, 5)
	if got := lowered.Frontiers.Frontier(CoordinatorDutyLeaseStart); got != 15 {
		t.Fatalf("frontier must stay at 15 after attempting to lower, got %d", got)
	}
}
