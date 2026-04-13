package transport

import "errors"

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
