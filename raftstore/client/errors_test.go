package client

import (
	"errors"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestClientSentinelHelpers(t *testing.T) {
	require.True(t, IsMissingRegionResolver(errMissingRegionResolver))
	require.True(t, IsMissingStoreResolver(errMissingStoreResolver))
	require.True(t, IsStoreIDNotSet(errStoreIDNotSet))
	require.True(t, IsStoreUnavailable(errStoreUnavailable))
	require.True(t, IsResolvedRegionIDMissing(errResolvedRegionIDMissing))
	require.True(t, IsRegionMetaMissing(errRegionMetaMissing))
	require.True(t, IsLeaderUnknown(errLeaderUnknown))
	require.True(t, IsInvalidScanLimit(errInvalidScanLimit))

	require.False(t, IsMissingRegionResolver(errors.New("other")))
	require.False(t, IsMissingStoreResolver(errors.New("other")))
}

func TestClientTypedErrors(t *testing.T) {
	routeErr := &RouteUnavailableError{Key: []byte("route-key"), Err: errors.New("dial failed")}
	require.Contains(t, routeErr.Error(), "route unavailable")
	require.EqualError(t, routeErr.Unwrap(), "dial failed")
	require.Equal(t, "client: route unavailable", (*RouteUnavailableError)(nil).Error())
	require.Nil(t, (*RouteUnavailableError)(nil).Unwrap())
	require.True(t, IsRouteUnavailable(routeErr))
	gotRouteErr, ok := AsRouteUnavailable(routeErr)
	require.True(t, ok)
	require.Same(t, routeErr, gotRouteErr)
	_, ok = AsRouteUnavailable(errors.New("other"))
	require.False(t, ok)

	notFound := &RegionNotFoundError{Key: []byte("missing")}
	require.Contains(t, notFound.Error(), "region not found")
	require.Equal(t, "client: region not found", (*RegionNotFoundError)(nil).Error())
	require.True(t, IsRegionNotFound(notFound))
	gotNotFound, ok := AsRegionNotFound(notFound)
	require.True(t, ok)
	require.Same(t, notFound, gotNotFound)
	_, ok = AsRegionNotFound(errors.New("other"))
	require.False(t, ok)

	retry := &RetryExhaustedError{Operation: "commit", RegionID: 7}
	require.Contains(t, retry.Error(), "commit retries exhausted")
	require.True(t, IsRetryExhausted(retry))
	require.False(t, IsRetryExhausted(errors.New("other")))

	protocol := &ProtocolError{Operation: "mutate", Detail: "primary key required"}
	require.Contains(t, protocol.Error(), "mutate protocol error")
	require.True(t, IsProtocolError(protocol))
	require.False(t, IsProtocolError(errors.New("other")))

	routing := &RegionRoutingError{
		Operation: "resolve region",
		RegionID:  11,
		Key:       []byte("key"),
		Detail:    "missing peers",
		Err:       errors.New("resolver failed"),
	}
	require.Contains(t, routing.Error(), "region routing error")
	require.Contains(t, routing.Error(), "missing peers")
	require.EqualError(t, routing.Unwrap(), "resolver failed")
	require.True(t, IsRegionRoutingError(routing))
	require.False(t, IsRegionRoutingError(errors.New("other")))
}

func TestClientTypedErrorsExposeStableKinds(t *testing.T) {
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errMissingRegionResolver))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errMissingStoreResolver))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errStoreIDNotSet))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(errStoreUnavailable))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errResolvedRegionIDMissing))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(errRegionMetaMissing))
	require.Equal(t, nokverrors.KindRouteUnavailable, nokverrors.KindOf(errLeaderUnknown))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(errInvalidScanLimit))
	require.Equal(t, nokverrors.KindRetryable, nokverrors.KindOf(errReadLockStillLive))

	require.Equal(t, nokverrors.KindRetryExhausted, nokverrors.KindOf(&RetryExhaustedError{Operation: "scan"}))
	require.Equal(t, nokverrors.KindProtocolViolation, nokverrors.KindOf(&ProtocolError{Operation: "commit"}))
	require.Equal(t, nokverrors.KindRegionRouting, nokverrors.KindOf(&RegionRoutingError{Operation: "route"}))
	require.Equal(t, nokverrors.KindRouteUnavailable, nokverrors.KindOf(&RouteUnavailableError{Key: []byte("k")}))
	require.Equal(t, nokverrors.KindNotFound, nokverrors.KindOf(&RegionNotFoundError{Key: []byte("k")}))
	require.Equal(t, nokverrors.KindLockConflict, nokverrors.KindOf(nokverrors.NewTxnKeyError(
		&kvrpcpb.KeyError{Locked: &kvrpcpb.Locked{Key: []byte("k")}},
	)))
	require.True(t, nokverrors.Retryable(&RegionRoutingError{Operation: "route"}))
	require.True(t, nokverrors.Retryable(&RouteUnavailableError{Key: []byte("k")}))
	require.False(t, nokverrors.Retryable(&RetryExhaustedError{Operation: "scan"}))
}
