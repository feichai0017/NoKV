package failpoints

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModesAndPredicates(t *testing.T) {
	Set(None)
	require.Equal(t, None, Current())
	require.False(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())
	require.False(t, ShouldFailAfterSnapshotApplyBeforePublish())

	Set(BeforeStorage)
	require.True(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())

	Set(SkipLocalMeta)
	require.False(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
	require.False(t, ShouldFailAfterSnapshotApplyBeforePublish())

	Set(AfterSnapshotApplyBeforePublish)
	require.False(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())
	require.True(t, ShouldFailAfterSnapshotApplyBeforePublish())

	Set(BeforeStorage | SkipLocalMeta | AfterSnapshotApplyBeforePublish)
	require.True(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
	require.True(t, ShouldFailAfterSnapshotApplyBeforePublish())
}
