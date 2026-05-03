package server

import (
	"context"
	stderrors "errors"
	"fmt"

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
	case stderrors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case stderrors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Error(rpcCodeForKind(nokverrors.KindOf(err)), err.Error())
	}
}

func rpcCodeForKind(kind nokverrors.Kind) codes.Code {
	switch kind {
	case nokverrors.KindInvalidArgument:
		return codes.InvalidArgument
	case nokverrors.KindConflict,
		nokverrors.KindWriteConflict,
		nokverrors.KindLockConflict,
		nokverrors.KindCommitTsExpired,
		nokverrors.KindAborted,
		nokverrors.KindProtocolViolation,
		nokverrors.KindNotLeader:
		return codes.FailedPrecondition
	case nokverrors.KindRetryable,
		nokverrors.KindUnavailable,
		nokverrors.KindRouteUnavailable,
		nokverrors.KindRegionRouting,
		nokverrors.KindStaleEpoch:
		return codes.Unavailable
	case nokverrors.KindResourceExhausted:
		return codes.ResourceExhausted
	case nokverrors.KindCorruption:
		return codes.DataLoss
	default:
		return codes.Internal
	}
}

func statusInvalidArgument(message string) error {
	return status.Error(codes.InvalidArgument, nokverrors.New(nokverrors.KindInvalidArgument, message).Error())
}

func statusFailedPrecondition(err error) error {
	if err == nil {
		return nil
	}
	return status.Error(codes.FailedPrecondition, err.Error())
}

func statusUnimplemented(message string) error {
	return status.Error(codes.Unimplemented, message)
}

func statusNotLeader(leaderID uint64) error {
	message := "metadata root not leader"
	if leaderID != 0 {
		message = fmt.Sprintf("%s (leader_id=%d)", message, leaderID)
	}
	return status.Error(codes.FailedPrecondition, nokverrors.New(nokverrors.KindNotLeader, message).Error())
}
