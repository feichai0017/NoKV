package client

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
)

func TestCoordinatorClientErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errEmptyAddress))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errNoReachableAddress))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errConnectionShutdown))
	require.Equal(t, nokverrors.KindStaleEpoch, nokverrors.KindOf(errStaleWitnessEra))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errInvalidWitness))
	require.True(t, nokverrors.Retryable(errStaleWitnessEra))
	require.False(t, nokverrors.Retryable(errInvalidWitness))

	notLeader := nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, "coordinator not leader", map[string]string{
		coordinatorReasonMetadata: reasonNotLeader,
		leaderIDMetadata:          "2",
	})
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(notLeader))
	require.True(t, nokverrors.Retryable(notLeader))
	require.True(t, IsNotLeader(notLeader))
	leaderID, ok := LeaderHint(notLeader)
	require.True(t, ok)
	require.Equal(t, uint64(2), leaderID)

	grantNotHeld := nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, "coordinator grant not held", map[string]string{
		coordinatorReasonMetadata: reasonGrantNotHeld,
	})
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(grantNotHeld))
	require.True(t, nokverrors.Retryable(grantNotHeld))
	require.True(t, IsGrantNotHeld(grantNotHeld))

	grantExpired := nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, nokverrors.New(nokverrors.KindInvalidArgument, "meta/root/state: invalid grant: rooted grant expired era=7").Error(), map[string]string{
		coordinatorReasonMetadata: reasonGrantNotHeld,
	})
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(grantExpired))
	require.True(t, nokverrors.Retryable(grantExpired))
}
