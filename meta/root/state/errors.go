package state

import "errors"

var (
	// ErrPrimacy indicates a primacy violation: another active holder already owns the current tenure.
	ErrPrimacy = errors.New("meta/root/state: primacy violated")
	// ErrInvalidTenure indicates malformed or impossible lease parameters.
	ErrInvalidTenure = errors.New("meta/root/state: invalid tenure")
	// ErrInheritance indicates an inheritance violation: the successor did not cover or acknowledge the sealed legacy.
	ErrInheritance = errors.New("meta/root/state: inheritance violated")
	// ErrMandate indicates that the current rooted lease does not
	// admit one requested coordinator mandate.
	ErrMandate = errors.New("meta/root/state: mandate mismatch")
	// ErrSilence indicates that a sealed predecessor generation was still admitted on a live path.
	ErrSilence = errors.New("meta/root/state: silence violated")
	// ErrFinality indicates that rooted handover was not yet complete for the requested operation.
	ErrFinality = errors.New("meta/root/state: finality violated")
)
