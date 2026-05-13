package peras

import (
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
)

var (
	ErrClientRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority client required")
	ErrTableRequired   = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority table required")
	ErrHolderRequired  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: holder id required")
	ErrTTLInvalid      = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: authority ttl must be non-negative")
	ErrInvalidResponse = nokverrors.New(nokverrors.KindProtocolViolation, "fsmeta/runtime/peras: invalid authority response")
	ErrNotHeld         = nokverrors.New(nokverrors.KindNotLeader, "fsmeta/runtime/peras: authority is held by another holder")
	ErrRuntimeInvalid  = nokverrors.New(nokverrors.KindInvalidArgument, "fsmeta/runtime/peras: runtime config is invalid")
	ErrRuntimeClosed   = nokverrors.New(nokverrors.KindUnavailable, "fsmeta/runtime/peras: runtime is closed")
)

func IsNotHeld(err error) bool {
	return errors.Is(err, ErrNotHeld)
}

func (c *Runtime) recordErrorf(format string, args ...any) error {
	return c.recordError(fmt.Errorf(format, args...))
}

func (c *Runtime) recordError(err error) error {
	if c == nil || err == nil {
		return err
	}
	c.metrics.errorTotal.Add(1)
	c.metrics.statsMu.Lock()
	c.metrics.lastError = err.Error()
	c.metrics.statsMu.Unlock()
	return err
}

func isAdmissionTerminalError(err error) bool {
	return errors.Is(err, fsmeta.ErrExists) ||
		errors.Is(err, fsmeta.ErrNotFound) ||
		errors.Is(err, fsmeta.ErrInvalidRequest) ||
		errors.Is(err, fsmeta.ErrInvalidValue)
}
