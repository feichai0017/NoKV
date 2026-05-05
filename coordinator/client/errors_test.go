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

	grantNotHeld := status.Error(codes.FailedPrecondition, errGrantNotHeldPrefix)
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(grantNotHeld))
	require.True(t, nokverrors.Retryable(grantNotHeld))

	grantExpired := status.Error(codes.FailedPrecondition, errGrantNotHeldPrefix+": "+nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/state: invalid grant: rooted grant expired era=7").Error())
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(grantExpired))
	require.True(t, nokverrors.Retryable(grantExpired))
}
