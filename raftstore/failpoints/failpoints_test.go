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
	require.False(t, ShouldFailAfterReadyAdvanceBeforeSend())
	require.False(t, ShouldFailBeforeTransportSendRPC())

	Set(BeforeStorage)
	require.True(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())
	require.False(t, ShouldFailAfterInitModePreparing())
	require.False(t, ShouldFailAfterReadyAdvanceBeforeSend())

	Set(SkipLocalMeta)
	require.False(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
	require.False(t, ShouldFailAfterSnapshotApplyBeforePublish())
	require.False(t, ShouldFailAfterInitCatalogPersist())
	require.False(t, ShouldFailBeforeTransportSendRPC())

	Set(AfterSnapshotApplyBeforePublish)
	require.False(t, ShouldFailBeforeStorage())
	require.False(t, ShouldSkipLocalMetaUpdate())
	require.True(t, ShouldFailAfterSnapshotApplyBeforePublish())
	require.False(t, ShouldFailAfterInitSeedSnapshot())
	require.False(t, ShouldFailAfterReadyAdvanceBeforeSend())

	Set(AfterInitModePreparing)
	require.True(t, ShouldFailAfterInitModePreparing())
	require.False(t, ShouldFailAfterInitCatalogPersist())
	require.False(t, ShouldFailAfterInitSeedSnapshot())
	require.False(t, ShouldFailBeforeTransportSendRPC())

	Set(AfterInitCatalogPersist)
	require.False(t, ShouldFailAfterInitModePreparing())
	require.True(t, ShouldFailAfterInitCatalogPersist())
	require.False(t, ShouldFailAfterInitSeedSnapshot())
	require.False(t, ShouldFailAfterReadyAdvanceBeforeSend())

	Set(AfterInitSeedSnapshot)
	require.False(t, ShouldFailAfterInitModePreparing())
	require.False(t, ShouldFailAfterInitCatalogPersist())
	require.True(t, ShouldFailAfterInitSeedSnapshot())
	require.False(t, ShouldFailBeforeTransportSendRPC())

	Set(AfterReadyAdvanceBeforeSend)
	require.True(t, ShouldFailAfterReadyAdvanceBeforeSend())
	require.False(t, ShouldFailBeforeTransportSendRPC())

	Set(BeforeTransportSendRPC)
	require.False(t, ShouldFailAfterReadyAdvanceBeforeSend())
	require.True(t, ShouldFailBeforeTransportSendRPC())

	Set(BeforeStorage | SkipLocalMeta | AfterSnapshotApplyBeforePublish | AfterInitModePreparing | AfterInitCatalogPersist | AfterInitSeedSnapshot | AfterReadyAdvanceBeforeSend | BeforeTransportSendRPC)
	require.True(t, ShouldFailBeforeStorage())
	require.True(t, ShouldSkipLocalMetaUpdate())
	require.True(t, ShouldFailAfterSnapshotApplyBeforePublish())
	require.True(t, ShouldFailAfterInitModePreparing())
	require.True(t, ShouldFailAfterInitCatalogPersist())
	require.True(t, ShouldFailAfterInitSeedSnapshot())
	require.True(t, ShouldFailAfterReadyAdvanceBeforeSend())
	require.True(t, ShouldFailBeforeTransportSendRPC())
}
