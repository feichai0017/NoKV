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
	require.False(t, ShouldFailAfterInitModePreparing())
	require.False(t, ShouldFailAfterInitCatalogPersist())
	require.False(t, ShouldFailAfterInitSeedSnapshot())

	Set(BeforeStorage)
	require.True(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())
	require.False(t, ShouldFailAfterInitModePreparing())

	Set(SkipLocalMeta)
	require.False(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
	require.False(t, ShouldFailAfterSnapshotApplyBeforePublish())
	require.False(t, ShouldFailAfterInitCatalogPersist())

	Set(AfterSnapshotApplyBeforePublish)
	require.False(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())
	require.True(t, ShouldFailAfterSnapshotApplyBeforePublish())
	require.False(t, ShouldFailAfterInitSeedSnapshot())

	Set(AfterInitModePreparing)
	require.True(t, ShouldFailAfterInitModePreparing())
	require.False(t, ShouldFailAfterInitCatalogPersist())
	require.False(t, ShouldFailAfterInitSeedSnapshot())

	Set(AfterInitCatalogPersist)
	require.False(t, ShouldFailAfterInitModePreparing())
	require.True(t, ShouldFailAfterInitCatalogPersist())
	require.False(t, ShouldFailAfterInitSeedSnapshot())

	Set(AfterInitSeedSnapshot)
	require.False(t, ShouldFailAfterInitModePreparing())
	require.False(t, ShouldFailAfterInitCatalogPersist())
	require.True(t, ShouldFailAfterInitSeedSnapshot())

	Set(BeforeStorage | SkipLocalMeta | AfterSnapshotApplyBeforePublish | AfterInitModePreparing | AfterInitCatalogPersist | AfterInitSeedSnapshot)
	require.True(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
	require.True(t, ShouldFailAfterSnapshotApplyBeforePublish())
	require.True(t, ShouldFailAfterInitModePreparing())
	require.True(t, ShouldFailAfterInitCatalogPersist())
	require.True(t, ShouldFailAfterInitSeedSnapshot())
}
