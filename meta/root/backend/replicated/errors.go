package replicated

import (
	"errors"
	"fmt"
)

var (
	errTransportRequiresLocalID       = errors.New("meta/root/backend/replicated: gRPC transport requires non-zero local id")
	errTransportClosed                = errors.New("meta/root/backend/replicated: transport closed")
	errTransportConnectionShutdown    = errors.New("meta/root/backend/replicated: transport connection shutdown")
	errNetworkDriverRequiresID        = errors.New("meta/root/backend/replicated: network driver id must be non-zero")
	errNetworkDriverRequiresWorkDir   = errors.New("meta/root/backend/replicated: network driver workdir is required")
	errNetworkDriverRequiresTransport = errors.New("meta/root/backend/replicated: network driver transport is required")
	errNetworkDriverClosed            = errors.New("meta/root/backend/replicated: network driver is closed")
	errDriverRequired                 = errors.New("meta/root/backend/replicated: driver is required")
	errCommittedEventPayloadTooLarge  = errors.New("meta/root/backend/replicated: committed event payload too large")
	errInvalidCommittedEntryPayload   = errors.New("meta/root/backend/replicated: invalid committed entry payload")
)

func errPeerAddressUnknown(id uint64) error {
	return fmt.Errorf("meta/root/backend/replicated: peer %d address unknown", id)
}

func errLocalNodeMissingFromPeerSet(id uint64, peerIDs []uint64) error {
	return fmt.Errorf("meta/root/backend/replicated: local node %d missing from peer set %v", id, peerIDs)
}

func errUnknownAllocatorKind(kind uint32) error {
	return fmt.Errorf("meta/root/backend/replicated: unknown allocator kind %d", kind)
}

func errNodeNotLeader(id uint64) error {
	return fmt.Errorf("meta/root/backend/replicated: node %d is not leader", id)
}

func errAppendWaitTimedOut(target any) error {
	return fmt.Errorf("meta/root/backend/replicated: append wait timed out before committed cursor %v", target)
}
