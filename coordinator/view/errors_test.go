package view

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestViewErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(ErrInvalidStoreID))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(ErrInvalidRegionID))
	require.Equal(t, nokverrors.KindStaleEpoch, nokverrors.KindOf(ErrRegionHeartbeatStale))
	require.Equal(t, nokverrors.KindConflict, nokverrors.KindOf(ErrRegionRangeOverlap))
	require.True(t, nokverrors.Retryable(ErrRegionHeartbeatStale))
	require.False(t, nokverrors.Retryable(ErrRegionRangeOverlap))
}
