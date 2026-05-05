package state

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	// ErrPrimacy indicates a primacy violation: another active holder already owns the current grant.
	ErrPrimacy = nokverrors.New(nokverrors.KindConflict, "meta/root/state: primacy violated")
	// ErrInvalidGrant indicates malformed or impossible grant parameters.
	ErrInvalidGrant = nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/state: invalid grant")
	// ErrInheritance indicates an inheritance violation: the successor did not cover or acknowledge a retired grant.
	ErrInheritance = nokverrors.New(nokverrors.KindProtocolViolation, "meta/root/state: inheritance violated")
	// ErrDuty indicates that the current rooted grant does not admit one requested duty.
	ErrDuty = nokverrors.New(nokverrors.KindProtocolViolation, "meta/root/state: duty mismatch")
	// ErrSilence indicates that a retired predecessor era was still admitted on a live path.
	ErrSilence = nokverrors.New(nokverrors.KindProtocolViolation, "meta/root/state: silence violated")
	// ErrFinality indicates that rooted grant retirement/inheritance was not yet complete for the requested operation.
	ErrFinality = nokverrors.New(nokverrors.KindConflict, "meta/root/state: finality violated")
)
