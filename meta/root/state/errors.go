package state

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	// ErrPrimacy indicates a primacy violation: another active holder already owns the current tenure.
	ErrPrimacy = nokverrors.New(nokverrors.KindConflict, "meta/root/state: primacy violated")
	// ErrInvalidTenure indicates malformed or impossible lease parameters.
	ErrInvalidTenure = nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/state: invalid tenure")
	// ErrInheritance indicates an inheritance violation: the successor did not cover or acknowledge the sealed legacy.
	ErrInheritance = nokverrors.New(nokverrors.KindProtocolViolation, "meta/root/state: inheritance violated")
	// ErrMandate indicates that the current rooted lease does not
	// admit one requested coordinator mandate.
	ErrMandate = nokverrors.New(nokverrors.KindProtocolViolation, "meta/root/state: mandate mismatch")
	// ErrSilence indicates that a sealed predecessor era was still admitted on a live path.
	ErrSilence = nokverrors.New(nokverrors.KindProtocolViolation, "meta/root/state: silence violated")
	// ErrFinality indicates that rooted handover was not yet complete for the requested operation.
	ErrFinality = nokverrors.New(nokverrors.KindConflict, "meta/root/state: finality violated")
)
