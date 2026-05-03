package client

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetadataRootClientErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errEmptyTarget))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errEmptyTargetSet))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errNilClient))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errNoEndpoints))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errNoReachableEndpoint))
	require.True(t, nokverrors.Retryable(errNoReachableEndpoint))

	notLeader := status.Error(codes.FailedPrecondition, errMetadataRootNotLeader+" (leader_id=2)")
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(notLeader))
	require.True(t, nokverrors.Retryable(notLeader))
}
