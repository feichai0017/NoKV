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

	Set(BeforeStorage)
	require.True(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())

	Set(SkipLocalMeta)
	require.False(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())

	Set(BeforeStorage | SkipLocalMeta)
	require.True(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
}
