package admin

import (
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
)

const (
	adminReasonMetadata = "raft_admin_reason"

	reasonCanceled              = "canceled"
	reasonInvalidRequest        = "invalid_request"
	reasonServiceNotConfigured  = "service_not_configured"
	reasonSnapshotNotConfigured = "snapshot_not_configured"
	reasonRegionNotHosted       = "region_not_hosted"
	reasonRegionNotLeader       = "region_not_leader"
	reasonPrecondition          = "failed_precondition"
	reasonInternal              = "internal"
)

func rpcCanceled(err error) error {
	return rpcAdminError(nokverrors.KindAborted, codes.Canceled, err.Error(), reasonCanceled)
}

func rpcInvalidArgument(message string) error {
	return rpcAdminError(nokverrors.KindInvalidArgument, codes.InvalidArgument, message, reasonInvalidRequest)
}

func rpcInvalidArgumentf(format string, args ...any) error {
	return rpcInvalidArgument(fmt.Sprintf(format, args...))
}

func rpcServiceNotConfigured(message string) error {
	return rpcAdminError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, message, reasonServiceNotConfigured)
}

func rpcSnapshotNotConfigured(message string) error {
	return rpcAdminError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, message, reasonSnapshotNotConfigured)
}

func rpcPrecondition(err error) error {
	kind := nokverrors.KindOf(err)
	if kind == nokverrors.KindUnknown {
		kind = nokverrors.KindProtocolViolation
	}
	return rpcAdminError(kind, codes.FailedPrecondition, err.Error(), reasonPrecondition)
}

func rpcPreconditionf(format string, args ...any) error {
	return rpcAdminError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, fmt.Sprintf(format, args...), reasonPrecondition)
}

func rpcNotFoundf(format string, args ...any) error {
	return rpcAdminError(nokverrors.KindNotFound, codes.NotFound, fmt.Sprintf(format, args...), reasonRegionNotHosted)
}

func rpcNotLeaderf(format string, args ...any) error {
	return rpcAdminError(nokverrors.KindNotLeader, codes.FailedPrecondition, fmt.Sprintf(format, args...), reasonRegionNotLeader)
}

func rpcInternalf(format string, args ...any) error {
	return rpcAdminError(nokverrors.KindProtocolViolation, codes.Internal, fmt.Sprintf(format, args...), reasonInternal)
}

func rpcAdminError(kind nokverrors.Kind, code codes.Code, message, reason string) error {
	return nokverrors.RPCStatusError(kind, code, message, map[string]string{
		adminReasonMetadata: reason,
	})
}
