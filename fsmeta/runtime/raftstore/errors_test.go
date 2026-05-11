package raftstore

import (
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestRuntimeErrorsExposeStableKinds(t *testing.T) {
	invalid := []error{
		errCoordinatorAddrRequired,
		errSessionCleanupLimitExceeded,
		errPerasAuthorityClientRequired,
		errPerasAuthorityTableRequired,
		errPerasAuthorityHolderRequired,
		errPerasAuthorityTTLInvalid,
		errMountCacheNotConfigured,
		errRootPublisherNotConfigured,
		errStoreListerRequired,
		errWatchRouterRequired,
		errKVClientRequired,
		errTSOClientRequired,
		errTimestampCountRequired,
	}
	for _, err := range invalid {
		require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(err))
	}

	protocol := []error{
		errRootEventNotAccepted,
		errNilTSOResponse,
		errZeroTSOTimestamp,
		errTSOCountMismatch(1, 2),
		errPerasAuthorityInvalidResponse,
	}
	for _, err := range protocol {
		require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(err))
	}

	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(errPerasAuthorityNotHeld))
}

func TestRunnerKeyErrorPreservesStableKind(t *testing.T) {
	err := runnerKeyError("kv get", &kvrpcpb.KeyError{
		Locked: &kvrpcpb.Locked{Key: []byte("k")},
	})
	require.Error(t, err)
	require.True(t, nokverrors.IsKind(err, nokverrors.KindLockConflict))
	require.True(t, nokverrors.Retryable(err))
}
