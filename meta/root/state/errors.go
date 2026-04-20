package state

import "errors"

var (
	// ErrCoordinatorLeaseHeld indicates that another active coordinator currently owns the lease.
	ErrCoordinatorLeaseHeld = errors.New("meta/root/state: coordinator lease held")
	// ErrInvalidCoordinatorLease indicates malformed or impossible lease parameters.
	ErrInvalidCoordinatorLease = errors.New("meta/root/state: invalid coordinator lease")
	// ErrCoordinatorLeaseOwner indicates that a lease release or overwrite was attempted by a different holder.
	ErrCoordinatorLeaseOwner = errors.New("meta/root/state: coordinator lease owner mismatch")
	// ErrCoordinatorLeaseCoverage indicates that a successor campaign does not
	// cover the sealed predecessor frontier.
	ErrCoordinatorLeaseCoverage = errors.New("meta/root/state: coordinator lease coverage mismatch")
	// ErrCoordinatorLeaseDuty indicates that the current rooted lease does not
	// admit one requested coordinator duty.
	ErrCoordinatorLeaseDuty = errors.New("meta/root/state: coordinator lease duty mismatch")
	// ErrCoordinatorLeaseLineage indicates that a successor campaign did not
	// explicitly reference the sealed predecessor authority.
	ErrCoordinatorLeaseLineage = errors.New("meta/root/state: coordinator lease lineage mismatch")
	// ErrCoordinatorLeaseAudit indicates that rooted closure confirmation was
	// attempted before the current seal/successor relationship was complete.
	ErrCoordinatorLeaseAudit = errors.New("meta/root/state: coordinator lease audit incomplete")
	// ErrCoordinatorLeaseClose indicates that rooted closure close was attempted
	// before the current successor generation had a matching confirmation.
	ErrCoordinatorLeaseClose = errors.New("meta/root/state: coordinator lease close incomplete")
	// ErrCoordinatorLeaseReattach indicates that rooted reattachment was
	// attempted before the current successor generation had a matching closure
	// confirmation.
	ErrCoordinatorLeaseReattach = errors.New("meta/root/state: coordinator lease reattach incomplete")
)
