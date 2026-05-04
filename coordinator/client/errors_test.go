package client

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCoordinatorClientErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errEmptyAddress))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errNoReachableAddress))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errConnectionShutdown))
	require.Equal(t, nokverrors.KindStaleEpoch, nokverrors.KindOf(errStaleWitnessEra))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errInvalidWitness))
	require.True(t, nokverrors.Retryable(errStaleWitnessEra))
	require.False(t, nokverrors.Retryable(errInvalidWitness))

	notLeader := status.Error(codes.FailedPrecondition, errNotLeaderPrefix+" (leader_id=2)")
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(notLeader))
	require.True(t, nokverrors.Retryable(notLeader))

	leaseNotHeld := status.Error(codes.FailedPrecondition, errLeaseNotHeldPrefix)
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(leaseNotHeld))
	require.True(t, nokverrors.Retryable(leaseNotHeld))

	leaseExpired := status.Error(codes.FailedPrecondition, errLeaseNotHeldPrefix+": "+nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/state: invalid tenure: rooted lease expired era=7").Error())
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(leaseExpired))
	require.True(t, nokverrors.Retryable(leaseExpired))
}
