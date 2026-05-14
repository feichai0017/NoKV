package peras

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestAuthorityErrorsCarryStableKinds(t *testing.T) {
	tests := []struct {
		name string
		err  error
		kind nokverrors.Kind
	}{
		{name: "invalid grant", err: ErrInvalidGrant, kind: nokverrors.KindInvalidArgument},
		{name: "ambiguous authority", err: ErrAmbiguousAuthority, kind: nokverrors.KindConflict},
		{name: "conflicting grant", err: ErrConflictingGrant, kind: nokverrors.KindConflict},
		{name: "stale view", err: ErrAuthorityViewStale, kind: nokverrors.KindStaleEpoch},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.kind, nokverrors.KindOf(tt.err))
			require.Equal(t, tt.kind, nokverrors.KindOf(fmt.Errorf("wrapped: %w", tt.err)))
		})
	}
}

func TestAuthorityViewStaleIsRetryable(t *testing.T) {
	require.True(t, nokverrors.Retryable(ErrAuthorityViewStale))
}
