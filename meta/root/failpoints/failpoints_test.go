package failpoints

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailpointsToggleIndependently(t *testing.T) {
	Set(None)
	t.Cleanup(func() { Set(None) })

	require.Equal(t, None, Current())
	require.NoError(t, InjectBeforeApplyTenure())
	require.NoError(t, InjectBeforeApplyHandover())
	require.NoError(t, InjectBeforeTenureStorageRead())
	require.NoError(t, InjectAfterAppendCommittedBeforeCheckpoint())

	Set(BeforeApplyTenure | BeforeTenureStorageRead)
	require.Equal(t, BeforeApplyTenure|BeforeTenureStorageRead, Current())
	require.ErrorIs(t, InjectBeforeApplyTenure(), ErrBeforeApplyTenure)
	require.NoError(t, InjectBeforeApplyHandover())
	require.ErrorIs(t, InjectBeforeTenureStorageRead(), ErrBeforeTenureStorageRead)
	require.NoError(t, InjectAfterAppendCommittedBeforeCheckpoint())

	Set(BeforeApplyHandover | AfterAppendCommittedBeforeCheckpoint)
	require.NoError(t, InjectBeforeApplyTenure())
	require.ErrorIs(t, InjectBeforeApplyHandover(), ErrBeforeApplyHandover)
	require.NoError(t, InjectBeforeTenureStorageRead())
	require.ErrorIs(t, InjectAfterAppendCommittedBeforeCheckpoint(), ErrAfterAppendCommittedBeforeCheckpoint)
}
