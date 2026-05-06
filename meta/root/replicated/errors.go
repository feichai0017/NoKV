package replicated

import (
	"context"
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errTransportRequiresLocalID       = errors.New("meta/root/replicated: gRPC transport requires non-zero local id")
	errTransportClosed                = errors.New("meta/root/replicated: transport closed")
	errTransportConnectionShutdown    = errors.New("meta/root/replicated: transport connection shutdown")
	errNetworkDriverRequiresID        = errors.New("meta/root/replicated: network driver id must be non-zero")
	errNetworkDriverRequiresWorkDir   = errors.New("meta/root/replicated: network driver workdir is required")
	errNetworkDriverRequiresTransport = errors.New("meta/root/replicated: network driver transport is required")
	errNetworkDriverClosed            = errors.New("meta/root/replicated: network driver is closed")
	errDriverRequired                 = errors.New("meta/root/replicated: driver is required")
	errCommittedEventPayloadTooLarge  = errors.New("meta/root/replicated: committed event payload too large")
	errInvalidCommittedEntryPayload   = errors.New("meta/root/replicated: invalid committed entry payload")
)

const (
	rootTransportReasonMetadata = "meta_root_transport_reason"

	reasonContextCanceled = "context_canceled"
	reasonContextDeadline = "context_deadline"
	reasonInternal        = "internal"
)

func rpcTransportStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return rpcTransportError(nokverrors.KindAborted, codes.Canceled, err.Error(), reasonContextCanceled)
	case errors.Is(err, context.DeadlineExceeded):
		return rpcTransportError(nokverrors.KindUnavailable, codes.DeadlineExceeded, err.Error(), reasonContextDeadline)
	default:
		return rpcTransportError(nokverrors.KindUnavailable, codes.Internal, err.Error(), reasonInternal)
	}
}

func rpcTransportError(kind nokverrors.Kind, code codes.Code, message, reason string) error {
	return nokverrors.RPCStatusError(kind, code, message, map[string]string{
		rootTransportReasonMetadata: reason,
	})
}

func errPeerAddressUnknown(id uint64) error {
	return fmt.Errorf("meta/root/replicated: peer %d address unknown", id)
}

func errLocalNodeMissingFromPeerSet(id uint64, peerIDs []uint64) error {
	return fmt.Errorf("meta/root/replicated: local node %d missing from peer set %v", id, peerIDs)
}

func errUnknownAllocatorKind(kind uint32) error {
	return fmt.Errorf("meta/root/replicated: unknown allocator kind %d", kind)
}

func errNodeNotLeader(id uint64) error {
	return fmt.Errorf("meta/root/replicated: node %d is not leader", id)
}

func errAppendWaitTimedOut(target any) error {
	return fmt.Errorf("meta/root/replicated: append wait timed out before committed cursor %v", target)
}
