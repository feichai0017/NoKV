package state

import (
	"fmt"
)

// ValidateCoordinatorLeaseStartCoverage is the rooted gate for any ordered
// served-summary frontier carried under CoordinatorDutyLeaseStart. It rejects
// a successor candidate whose lease_start does not strictly exceed the sealed
// predecessor's persisted served frontier.
func ValidateCoordinatorLeaseStartCoverage(seal CoordinatorSeal, successorLeaseStart uint64) error {
	if !seal.Present() {
		return nil
	}
	frontier := seal.Frontiers.Frontier(CoordinatorDutyLeaseStart)
	if frontier == 0 {
		return nil
	}
	if successorLeaseStart > frontier {
		return nil
	}
	return fmt.Errorf(
		"%w: duty=%s successor_lease_start=%d served_frontier=%d",
		ErrCoordinatorLeaseCoverage,
		CoordinatorDutyName(CoordinatorDutyLeaseStart),
		successorLeaseStart,
		frontier,
	)
}

// CoordinatorSealWithServedFrontier returns seal with the lease-start frontier
// bumped to at least servedTimestamp. Existing frontier values are never
// lowered.
func CoordinatorSealWithServedFrontier(seal CoordinatorSeal, servedTimestamp uint64) CoordinatorSeal {
	existing := seal.Frontiers.Frontier(CoordinatorDutyLeaseStart)
	if servedTimestamp <= existing {
		return seal
	}
	seal.Frontiers = seal.Frontiers.WithFrontier(CoordinatorDutyLeaseStart, servedTimestamp)
	return seal
}
