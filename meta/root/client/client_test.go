package client

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRetryableRemoteErrorConnectionClosing(t *testing.T) {
	err := status.Error(codes.Canceled, errClientConnectionClosing)
	require.True(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorWrappedConnectionClosing(t *testing.T) {
	inner := status.Error(codes.Canceled, errClientConnectionClosing)
	err := status.Error(codes.Internal, inner.Error())
	require.True(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorMetadataRootNotLeaderIsWriteOnly(t *testing.T) {
	err := status.Error(codes.FailedPrecondition, errMetadataRootNotLeader+" (leader_id=2)")
	require.False(t, retryableRemoteError(err, false))
	require.True(t, retryableRemoteError(err, true))
}

func TestRetryableRemoteErrorLeavesGenericInternalFatal(t *testing.T) {
	err := status.Error(codes.Internal, "boom")
	require.False(t, retryableRemoteError(err, false))
	require.False(t, retryableRemoteError(err, true))
}
