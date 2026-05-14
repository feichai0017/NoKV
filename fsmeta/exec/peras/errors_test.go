package peras

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestPerasErrorsCarryStableKinds(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind nokverrors.Kind
	}{
		{name: "invalid segment", err: ErrInvalidPerasSegment, kind: nokverrors.KindProtocolViolation},
		{name: "admission rejected", err: ErrAdmissionRejected, kind: nokverrors.KindConflict},
		{name: "holder config", err: ErrHolderConfigInvalid, kind: nokverrors.KindInvalidArgument},
		{name: "ineligible operation", err: ErrIneligibleOperation, kind: nokverrors.KindProtocolViolation},
		{name: "invalid operation id", err: ErrInvalidOperationID, kind: nokverrors.KindInvalidArgument},
		{name: "duplicate operation", err: ErrDuplicateOperation, kind: nokverrors.KindConflict},
		{name: "catalog store", err: ErrSegmentCatalogStoreRequired, kind: nokverrors.KindInvalidArgument},
		{name: "replay version", err: ErrReplayVersionRequired, kind: nokverrors.KindProtocolViolation},
		{name: "witness record", err: ErrInvalidWitnessRecord, kind: nokverrors.KindProtocolViolation},
		{name: "witness log", err: ErrWitnessLogRequired, kind: nokverrors.KindInvalidArgument},
		{name: "witness replica", err: ErrWitnessReplicaInvalid, kind: nokverrors.KindInvalidArgument},
		{name: "witness quorum", err: ErrSegmentWitnessQuorumUnavailable, kind: nokverrors.KindUnavailable},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.kind, nokverrors.KindOf(tt.err))
			require.Equal(t, tt.kind, nokverrors.KindOf(fmt.Errorf("wrapped: %w", tt.err)))
		})
	}
}

func TestSegmentWitnessQuorumUnavailableIsRetryable(t *testing.T) {
	require.True(t, nokverrors.Retryable(ErrSegmentWitnessQuorumUnavailable))
}
