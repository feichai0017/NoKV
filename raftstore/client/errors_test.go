package client

import (
	"errors"
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestClientSentinelHelpers(t *testing.T) {
	require.True(t, IsMissingRegionResolver(errMissingRegionResolver))
	require.True(t, IsMissingStoreResolver(errMissingStoreResolver))
	require.True(t, IsStoreIDNotSet(errStoreIDNotSet))
	require.True(t, IsResolvedRegionIDMissing(errResolvedRegionIDMissing))
	require.True(t, IsRegionMetaMissing(errRegionMetaMissing))
	require.True(t, IsLeaderUnknown(errLeaderUnknown))
	require.True(t, IsInvalidScanLimit(errInvalidScanLimit))

	require.False(t, IsMissingRegionResolver(errors.New("other")))
	require.False(t, IsMissingStoreResolver(errors.New("other")))
}

func TestClientTypedErrors(t *testing.T) {
	keyConflict := &KeyConflictError{
		Errors: []*kvrpcpb.KeyError{{Locked: &kvrpcpb.Locked{PrimaryLock: []byte("pk")}}},
	}
	require.Contains(t, keyConflict.Error(), "prewrite key errors")
	gotKeyConflict, ok := AsKeyConflict(keyConflict)
	require.True(t, ok)
	require.Same(t, keyConflict, gotKeyConflict)
	_, ok = AsKeyConflict(errors.New("other"))
	require.False(t, ok)

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
}
