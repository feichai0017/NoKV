// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	stderrors "errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	metaRootReasonMetadata = "meta_root_reason"
	leaderIDMetadata       = "leader_id"
	reasonNotLeader        = "not_leader"
	reasonInvalidArgument  = "invalid_argument"
	reasonPrecondition     = "failed_precondition"
	reasonInternal         = "internal"
	reasonContextCanceled  = "context_canceled"
	reasonContextDeadline  = "context_deadline"
	reasonUnimplemented    = "unimplemented"
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
		return nokverrors.RPCStatusError(nokverrors.KindAborted, codes.Canceled, err.Error(), map[string]string{
			metaRootReasonMetadata: reasonContextCanceled,
		})
	case stderrors.Is(err, context.DeadlineExceeded):
		return nokverrors.RPCStatusError(nokverrors.KindUnavailable, codes.DeadlineExceeded, err.Error(), map[string]string{
			metaRootReasonMetadata: reasonContextDeadline,
		})
	default:
		kind := nokverrors.KindOf(err)
		return nokverrors.RPCStatusError(kind, rpcCodeForKind(kind), err.Error(), map[string]string{
			metaRootReasonMetadata: reasonInternal,
		})
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
	return nokverrors.RPCStatusError(nokverrors.KindInvalidArgument, codes.InvalidArgument, message, map[string]string{
		metaRootReasonMetadata: reasonInvalidArgument,
	})
}

func statusFailedPrecondition(err error) error {
	if err == nil {
		return nil
	}
	kind := nokverrors.KindOf(err)
	if kind == nokverrors.KindUnknown {
		kind = nokverrors.KindProtocolViolation
	}
	return nokverrors.RPCStatusError(kind, codes.FailedPrecondition, err.Error(), map[string]string{
		metaRootReasonMetadata: reasonPrecondition,
	})
}

func statusUnimplemented(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindProtocolViolation, codes.Unimplemented, message, map[string]string{
		metaRootReasonMetadata: reasonUnimplemented,
	})
}

func statusNotLeader(leaderID uint64) error {
	message := "metadata root not leader"
	metadata := map[string]string{metaRootReasonMetadata: reasonNotLeader}
	if leaderID != 0 {
		message = fmt.Sprintf("%s (leader_id=%d)", message, leaderID)
		metadata[leaderIDMetadata] = fmt.Sprintf("%d", leaderID)
	}
	return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, message, metadata)
}
