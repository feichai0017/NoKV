package failpoints

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestModesAndPredicates(t *testing.T) {
	Set(None)
	require.Equal(t, None, Current())
	require.False(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipManifestUpdate())

	Set(BeforeStorage)
	require.True(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipManifestUpdate())

	Set(SkipManifest)
	require.False(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipManifestUpdate())

	Set(BeforeStorage | SkipManifest)
	require.True(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipManifestUpdate())
}
