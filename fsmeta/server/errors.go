package server

import (
	"context"
	"errors"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/feichai0017/NoKV/fsmeta"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	fsmetaReasonMetadata       = "fsmeta_reason"
	reasonQuotaExceeded        = "quota_exceeded"
	reasonWatchOverflow        = "watch_overflow"
	reasonWatchCursorExpired   = "watch_cursor_expired"
	reasonMountNotRegistered   = "mount_not_registered"
	reasonMountRetired         = "mount_retired"
	reasonCrossAuthorityRename = "cross_authority_rename"
	reasonNamespaceExists      = "entry_exists"
	reasonNamespaceNotFound    = "entry_not_found"
	reasonInvalidFSMetaInput   = "invalid_fsmeta_input"
	reasonInvalidMountID       = "invalid_mount_id"
	reasonInvalidInodeID       = "invalid_inode_id"
	reasonInvalidName          = "invalid_name"
	reasonInvalidSession       = "invalid_session"
	reasonInvalidRequest       = "invalid_request"
	reasonInvalidKey           = "invalid_key"
	reasonInvalidKeyKind       = "invalid_key_kind"
	reasonInvalidValue         = "invalid_value"
	reasonInvalidValueKind     = "invalid_value_kind"
	reasonInvalidPageSize      = "invalid_page_size"
	reasonServiceUnavailable   = "service_unavailable"
	reasonContextCanceled      = "context_canceled"
	reasonContextDeadline      = "context_deadline"
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
		return nokverrors.RPCStatusError(nokverrors.KindAborted, codes.Canceled, err.Error(), map[string]string{
			fsmetaReasonMetadata: reasonContextCanceled,
		})
	case errors.Is(err, context.DeadlineExceeded):
		return nokverrors.RPCStatusError(nokverrors.KindUnavailable, codes.DeadlineExceeded, err.Error(), map[string]string{
			fsmetaReasonMetadata: reasonContextDeadline,
		})
	default:
		kind := nokverrors.KindOf(err)
		return nokverrors.RPCStatusError(kind, rpcCodeForKind(kind), err.Error(), fsmetaErrorMetadata(err))
	}
}

func rpcInvalidArgument(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindInvalidArgument, codes.InvalidArgument, message, map[string]string{
		fsmetaReasonMetadata: reasonInvalidFSMetaInput,
	})
}

func rpcServiceUnavailable(message string) error {
	return nokverrors.RPCStatusError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, message, map[string]string{
		fsmetaReasonMetadata: reasonServiceUnavailable,
	})
}

func fsmetaErrorMetadata(err error) map[string]string {
	reason := ""
	switch {
	case errors.Is(err, fsmeta.ErrQuotaExceeded):
		reason = reasonQuotaExceeded
	case errors.Is(err, fsmeta.ErrWatchOverflow):
		reason = reasonWatchOverflow
	case errors.Is(err, fsmeta.ErrWatchCursorExpired):
		reason = reasonWatchCursorExpired
	case errors.Is(err, fsmeta.ErrMountNotRegistered):
		reason = reasonMountNotRegistered
	case errors.Is(err, fsmeta.ErrMountRetired):
		reason = reasonMountRetired
	case errors.Is(err, fsmeta.ErrCrossAuthorityRename):
		reason = reasonCrossAuthorityRename
	case errors.Is(err, fsmeta.ErrExists):
		reason = reasonNamespaceExists
	case errors.Is(err, fsmeta.ErrNotFound):
		reason = reasonNamespaceNotFound
	case errors.Is(err, fsmeta.ErrInvalidMountID):
		reason = reasonInvalidMountID
	case errors.Is(err, fsmeta.ErrInvalidInodeID):
		reason = reasonInvalidInodeID
	case errors.Is(err, fsmeta.ErrInvalidName):
		reason = reasonInvalidName
	case errors.Is(err, fsmeta.ErrInvalidSession):
		reason = reasonInvalidSession
	case errors.Is(err, fsmeta.ErrInvalidRequest):
		reason = reasonInvalidRequest
	case errors.Is(err, fsmeta.ErrInvalidKey):
		reason = reasonInvalidKey
	case errors.Is(err, fsmeta.ErrInvalidKeyKind):
		reason = reasonInvalidKeyKind
	case errors.Is(err, fsmeta.ErrInvalidValue):
		reason = reasonInvalidValue
	case errors.Is(err, fsmeta.ErrInvalidValueKind):
		reason = reasonInvalidValueKind
	case errors.Is(err, fsmeta.ErrInvalidPageSize):
		reason = reasonInvalidPageSize
	}
	if reason == "" {
		return nil
	}
	return map[string]string{fsmetaReasonMetadata: reason}
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
		nokverrors.KindRetryExhausted,
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
