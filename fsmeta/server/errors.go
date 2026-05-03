package server

import (
	"context"
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func rpcError(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(rpcCodeForKind(nokverrors.KindOf(err)), err.Error())
	}
}

func rpcCodeForKind(kind nokverrors.Kind) codes.Code {
	switch kind {
	case nokverrors.KindInvalidArgument:
		return codes.InvalidArgument
	case nokverrors.KindNotFound:
		return codes.NotFound
	case nokverrors.KindAlreadyExists:
		return codes.AlreadyExists
	case nokverrors.KindResourceExhausted:
		return codes.ResourceExhausted
	case nokverrors.KindStaleEpoch:
		return codes.OutOfRange
	case nokverrors.KindConflict,
		nokverrors.KindWriteConflict,
		nokverrors.KindLockConflict,
		nokverrors.KindCommitTsExpired:
		return codes.Aborted
	case nokverrors.KindAborted,
		nokverrors.KindProtocolViolation:
		return codes.FailedPrecondition
	case nokverrors.KindRetryable,
		nokverrors.KindUnavailable,
		nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindNotLeader:
		return codes.Unavailable
	case nokverrors.KindCorruption:
		return codes.DataLoss
	default:
		return codes.Internal
	}
}
