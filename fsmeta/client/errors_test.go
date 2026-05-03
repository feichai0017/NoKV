package client

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
)

func TestClientErrorsExposeStableKinds(t *testing.T) {
	invalid := []error{
		errAddressRequired,
		errDirectoryReaderRequired,
		errWatchClientRequired,
		errStagedPublishClientRequired,
		errWatchStreamNotConfigured,
		errWatchSessionNotConfigured,
		errRPCClientNotConfigured,
	}
	for _, err := range invalid {
		require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(err))
	}

	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errWatchEventBeforeReady))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errConnectionNotReady))
	require.True(t, nokverrors.Retryable(errConnectionNotReady))
}
