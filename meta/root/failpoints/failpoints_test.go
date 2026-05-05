package failpoints

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFailpointsToggleIndependently(t *testing.T) {
	Set(None)
	t.Cleanup(func() { Set(None) })

	require.Equal(t, None, Current())
	require.NoError(t, InjectBeforeApplyGrantIssue())
	require.NoError(t, InjectBeforeApplyGrantRetirement())
	require.NoError(t, InjectBeforeGrantStorageRead())
	require.NoError(t, InjectAfterAppendCommittedBeforeCheckpoint())

	Set(BeforeApplyGrantIssue | BeforeGrantStorageRead)
	require.Equal(t, BeforeApplyGrantIssue|BeforeGrantStorageRead, Current())
	require.ErrorIs(t, InjectBeforeApplyGrantIssue(), ErrBeforeApplyGrantIssue)
	require.NoError(t, InjectBeforeApplyGrantRetirement())
	require.ErrorIs(t, InjectBeforeGrantStorageRead(), ErrBeforeGrantStorageRead)
	require.NoError(t, InjectAfterAppendCommittedBeforeCheckpoint())

	Set(BeforeApplyGrantRetirement | AfterAppendCommittedBeforeCheckpoint)
	require.NoError(t, InjectBeforeApplyGrantIssue())
	require.ErrorIs(t, InjectBeforeApplyGrantRetirement(), ErrBeforeApplyGrantRetirement)
	require.NoError(t, InjectBeforeGrantStorageRead())
	require.ErrorIs(t, InjectAfterAppendCommittedBeforeCheckpoint(), ErrAfterAppendCommittedBeforeCheckpoint)
}
