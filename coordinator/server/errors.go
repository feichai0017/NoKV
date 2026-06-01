// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	stderrors "errors"
	"fmt"
	"strconv"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
)

const (
	diagnosticNotLeader                = "coordinator not leader"
	diagnosticRootUnavailable          = "coordinator root unavailable"
	diagnosticGrantNotHeld             = "coordinator grant not held"
	errRootLagExceedsStrongFreshness   = "root lag exceeds strong freshness"
	errBootstrapRequiredBeforeBounded  = "bootstrap required before bounded freshness"
	errRequiredRootedTokenNotSatisfied = "required rooted token not satisfied"
	errRequiredDescriptorNotSatisfied  = "required descriptor revision not satisfied"
	errRootLagExceedsBound             = "root lag exceeds bound"
)

const (
	coordinatorReasonMetadata = "coordinator_reason"
	leaderIDMetadata          = "leader_id"

	reasonNotLeader              = "not_leader"
	reasonGrantNotHeld           = "grant_not_held"
	reasonRootUnavailable        = "root_unavailable"
	reasonRootLagExceeded        = "root_lag_exceeded"
	reasonRequiredRootedToken    = "required_rooted_token"
	reasonRequiredDescriptor     = "required_descriptor"
	reasonBootstrapRequired      = "bootstrap_required"
	reasonCatalogInvalid         = "catalog_invalid"
	reasonCatalogPrecondition    = "catalog_precondition"
	reasonCatalogInternal        = "catalog_internal"
	reasonClusterEraMismatch     = "cluster_era_mismatch"
	reasonRootStorageUnavailable = "root_storage_unavailable"
	reasonInvalidRequest         = "invalid_request"
	reasonContextCanceled        = "context_canceled"
	reasonContextDeadline        = "context_deadline"
	reasonInternal               = "internal"
)

func statusContext(err error) error {
	reason := reasonContextCanceled
	code := codes.Canceled
	kind := nokverrors.KindAborted
	if stderrors.Is(err, context.DeadlineExceeded) {
		reason = reasonContextDeadline
		code = codes.DeadlineExceeded
		kind = nokverrors.KindUnavailable
	}
	return nokverrors.RPCStatusError(kind, code, err.Error(), map[string]string{
		coordinatorReasonMetadata: reason,
	})
}

func statusInvalidArgument(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindInvalidArgument, codes.InvalidArgument, message, map[string]string{
		coordinatorReasonMetadata: reasonInvalidRequest,
	})
}

func statusInternal(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindUnavailable, codes.Internal, message, map[string]string{
		coordinatorReasonMetadata: reasonInternal,
	})
}

func statusInternalf(format string, args ...any) error {
	return statusInternal(fmt.Sprintf(format, args...))
}

func statusNotLeader(leaderID uint64) error {
	metadata := map[string]string{coordinatorReasonMetadata: reasonNotLeader}
	if leaderID == 0 {
		return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, diagnosticNotLeader, metadata)
	}
	metadata[leaderIDMetadata] = strconv.FormatUint(leaderID, 10)
	return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, fmt.Sprintf("%s (leader_id=%d)", diagnosticNotLeader, leaderID), metadata)
}

func statusGrant(err error) error {
	return nokverrors.RPCStatusError(nokverrors.KindNotLeader, codes.FailedPrecondition, fmt.Sprintf("%s: %v", diagnosticGrantNotHeld, err), map[string]string{
		coordinatorReasonMetadata: reasonGrantNotHeld,
	})
}

func statusRootUnavailable() error {
	return nokverrors.RPCStatusError(nokverrors.KindUnavailable, codes.FailedPrecondition, diagnosticRootUnavailable, map[string]string{
		coordinatorReasonMetadata: reasonRootUnavailable,
	})
}

func statusStaleEpoch(message, reason string) error {
	return nokverrors.RPCStatusError(nokverrors.KindStaleEpoch, codes.FailedPrecondition, message, map[string]string{
		coordinatorReasonMetadata: reason,
	})
}

func statusProtocol(message, reason string) error {
	return nokverrors.RPCStatusError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, message, map[string]string{
		coordinatorReasonMetadata: reason,
	})
}

func statusCatalog(kind nokverrors.Kind, code codes.Code, err error, reason string) error {
	if kind == nokverrors.KindUnknown {
		kind = nokverrors.KindProtocolViolation
	}
	return nokverrors.RPCStatusError(kind, code, err.Error(), map[string]string{
		coordinatorReasonMetadata: reason,
	})
}
