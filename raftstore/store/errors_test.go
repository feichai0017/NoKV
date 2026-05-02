package store

import (
	"errors"
	"testing"

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

	protocol := &ProtocolError{Operation: "resolve locks", Detail: "invalid response"}
	require.Contains(t, protocol.Error(), "resolve locks protocol error")
	require.True(t, IsProtocolError(protocol))
	require.False(t, IsProtocolError(errors.New("other")))
}
