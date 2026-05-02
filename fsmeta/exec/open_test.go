package exec

import (
	"context"
	"testing"

	"github.com/feichai0017/NoKV/fsmeta"
	"github.com/stretchr/testify/require"
)

func TestOpenWithRaftstoreRejectsInvalidSessionCleanupLimit(t *testing.T) {
	_, err := OpenWithRaftstore(context.Background(), Options{
		CoordinatorAddr:     "127.0.0.1:1",
		SessionCleanupLimit: fsmeta.MaxSessionExpireLimit + 1,
	})
	require.ErrorContains(t, err, "session cleanup limit exceeds maximum")
}
