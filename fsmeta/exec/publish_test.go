package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestRootPublisherRequiresCoordinatorClient(t *testing.T) {
	publisher := rootPublisher{}
	token := fsmeta.SnapshotSubtreeToken{Mount: "vol", RootInode: 1, ReadVersion: 10}

	require.ErrorContains(t, publisher.PublishSnapshotSubtree(context.Background(), token), "not configured")
	require.ErrorContains(t, publisher.RetireSnapshotSubtree(context.Background(), token), "not configured")
	require.ErrorContains(t, publisher.StartSubtreeHandoff(context.Background(), "vol", 1, 10), "not configured")
	require.ErrorContains(t, publisher.CompleteSubtreeHandoff(context.Background(), "vol", 1, 10), "not configured")
}
