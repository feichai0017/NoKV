package state

import "errors"

var (
	// ErrCoordinatorLeaseHeld indicates that another active coordinator currently owns the lease.
	ErrCoordinatorLeaseHeld = errors.New("meta/root/state: coordinator lease held")
	// ErrInvalidCoordinatorLease indicates malformed or impossible lease parameters.
	ErrInvalidCoordinatorLease = errors.New("meta/root/state: invalid coordinator lease")
	// ErrCoordinatorLeaseOwner indicates that a lease release or overwrite was attempted by a different holder.
	ErrCoordinatorLeaseOwner = errors.New("meta/root/state: coordinator lease owner mismatch")
)
