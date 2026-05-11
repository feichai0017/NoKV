package peras

import "errors"

var (
	ErrInvalidPerasSegment = errors.New("fsmeta peras: invalid peras segment")
	ErrAdmissionRejected   = errors.New("fsmeta peras: admission rejected")
)
