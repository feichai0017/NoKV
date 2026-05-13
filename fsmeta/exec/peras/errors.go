package peras

import "errors"

var (
	ErrInvalidPerasSegment             = errors.New("fsmeta peras: invalid peras segment")
	ErrAdmissionRejected               = errors.New("fsmeta peras: admission rejected")
	ErrHolderConfigInvalid             = errors.New("fsmeta peras: invalid holder config")
	ErrIneligibleOperation             = errors.New("fsmeta peras: ineligible operation")
	ErrInvalidOperationID              = errors.New("fsmeta peras: invalid operation id")
	ErrDuplicateOperation              = errors.New("fsmeta peras: duplicate operation id")
	ErrSegmentCatalogStoreRequired     = errors.New("fsmeta peras: segment catalog store required")
	ErrReplayVersionRequired           = errors.New("fsmeta peras: replay version required")
	ErrInvalidWitnessRecord            = errors.New("fsmeta peras: invalid witness record")
	ErrWitnessLogRequired              = errors.New("fsmeta peras: witness log required")
	ErrWitnessReplicaInvalid           = errors.New("fsmeta peras: invalid witness replica")
	ErrSegmentWitnessQuorumUnavailable = errors.New("fsmeta peras: segment witness quorum unavailable")
)
