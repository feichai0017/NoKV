package store

import (
	"errors"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestStoreTypedErrors(t *testing.T) {
	routing := &RegionRoutingError{
		Operation: "txn heartbeat",
		RegionID:  7,
		Key:       []byte("primary"),
		Detail:    "no region covers key",
		Err:       errors.New("resolver failed"),
	}
	require.Contains(t, routing.Error(), "region routing error")
	require.Contains(t, routing.Error(), "txn heartbeat")
	require.Contains(t, routing.Error(), "no region covers key")
	require.EqualError(t, routing.Unwrap(), "resolver failed")
	require.True(t, IsRegionRoutingError(routing))
	require.False(t, IsRegionRoutingError(errors.New("other")))
	require.Equal(t, nokverrors.KindRegionRouting, nokverrors.KindOf(routing))
	require.True(t, nokverrors.Retryable(routing))

	protocol := &ProtocolError{Operation: "resolve locks", Detail: "invalid response"}
	require.Contains(t, protocol.Error(), "resolve locks protocol error")
	require.True(t, IsProtocolError(protocol))
	require.False(t, IsProtocolError(errors.New("other")))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(protocol))
	require.False(t, nokverrors.Retryable(protocol))

	keyErr := errRegionKeyError("resolve locks", 8, &kvrpcpb.KeyError{
		CommitTsExpired: &kvrpcpb.CommitTsExpired{Key: []byte("k"), CommitTs: 10, MinCommitTs: 11},
	})
	require.Error(t, keyErr)
	require.True(t, nokverrors.IsKind(keyErr, nokverrors.KindCommitTsExpired))
	require.True(t, nokverrors.Retryable(keyErr))
}
