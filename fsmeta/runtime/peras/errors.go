package peras

import (
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
)

var (
	ErrClientRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority client required")
	ErrTableRequired   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority table required")
	ErrHolderRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: holder id required")
	ErrTTLInvalid      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority ttl must be non-negative")
	ErrInvalidResponse = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/peras: invalid authority response")
	ErrNotHeld         = nokverrors.New(nokverrors.KindNotLeader, "fsmeta/runtime/peras: authority is held by another holder")
)

func IsNotHeld(err error) bool {
	return errors.Is(err, ErrNotHeld)
}
