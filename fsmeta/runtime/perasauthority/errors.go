package perasauthority

import (
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
)

var (
	ErrClientRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/perasauthority: authority client required")
	ErrTableRequired   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/perasauthority: authority table required")
	ErrHolderRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/perasauthority: holder id required")
	ErrTTLInvalid      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/perasauthority: authority ttl must be non-negative")
	ErrInvalidResponse = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/perasauthority: invalid authority response")
	ErrNotHeld         = nokverrors.New(nokverrors.KindNotLeader, "fsmeta/runtime/perasauthority: authority is held by another holder")
)

func IsNotHeld(err error) bool {
	return errors.Is(err, ErrNotHeld)
}
