package transport

import (
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
)

const (
	transportReasonMetadata = "raftstore_transport_reason"
	reasonTransportInternal = "internal"
	reasonTransportInjected = "failpoint_injected"
)

var (
	// errPeerBlocked indicates that transport delivery to the target peer is currently blocked.
	errPeerBlocked = errors.New("raftstore: peer blocked")
	// errPeerUnknown indicates that no address is known for the target peer.
	errPeerUnknown = errors.New("raftstore: peer address unknown")
	// errInvalidLocalID indicates that the transport requires a non-zero local node id.
	errInvalidLocalID = errors.New("raftstore: gRPC transport requires non-zero local ID")
	// errTransportClosed indicates that the transport is already shut down.
	errTransportClosed = errors.New("raftstore: transport closed")
	// errRegisterAfterStart indicates that service registration happened after the transport started serving.
	errRegisterAfterStart = errors.New("raftstore: cannot register service after transport start")
	// errNilTransport indicates that a nil transport was used.
	errNilTransport = errors.New("raftstore: transport is nil")
)

func rpcTransportInternal(err error) error {
	kind := nokverrors.KindOf(err)
	if kind == nokverrors.KindUnknown {
		kind = nokverrors.KindProtocolViolation
	}
	return nokverrors.RPCStatusError(kind, codes.Internal, err.Error(), map[string]string{
		transportReasonMetadata: reasonTransportInternal,
	})
}

func rpcTransportUnavailable(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindUnavailable, codes.Unavailable, message, map[string]string{
		transportReasonMetadata: reasonTransportInjected,
	})
}
