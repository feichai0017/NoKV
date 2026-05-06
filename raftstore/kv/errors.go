package kv

import (
	"context"
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	errStoreNotInitialized = errors.New("raftstore: store not initialized")
)

const (
	kvReasonMetadata = "raftstore_kv_reason"

	reasonInvalidRequest        = "invalid_request"
	reasonServiceNotInitialized = "service_not_initialized"
	reasonUnsupportedOperation  = "unsupported_operation"
	reasonProtocolViolation     = "protocol_violation"
	reasonContextCanceled       = "context_canceled"
	reasonContextDeadline       = "context_deadline"
	reasonInternal              = "internal"
)

func IsStoreNotInitialized(err error) bool {
	return errors.Is(err, errStoreNotInitialized)
}

func rpcInvalidArgument(message string) error {
	return rpcKVError(nokverrors.KindInvalidArgument, codes.InvalidArgument, message, reasonInvalidRequest)
}

func rpcServiceNotInitialized() error {
	return rpcKVError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, errStoreNotInitialized.Error(), reasonServiceNotInitialized)
}

func rpcUnsupported(message string) error {
	return rpcKVError(nokverrors.KindProtocolViolation, codes.Unimplemented, message, reasonUnsupportedOperation)
}

func rpcProtocolPrecondition(message string) error {
	return rpcKVError(nokverrors.KindProtocolViolation, codes.FailedPrecondition, message, reasonProtocolViolation)
}

func raftPayloadError(op, detail string) error {
	msg := fmt.Sprintf("raftstore/kv: %s response protocol violation: %s", op, detail)
	return rpcProtocolPrecondition(msg)
}

func rpcStatus(err error) error {
	if err == nil {
		return nil
	}
	if _, ok := status.FromError(err); ok {
		return err
	}
	switch {
	case errors.Is(err, context.Canceled):
		return rpcKVError(nokverrors.KindAborted, codes.Canceled, err.Error(), reasonContextCanceled)
	case errors.Is(err, context.DeadlineExceeded):
		return rpcKVError(nokverrors.KindUnavailable, codes.DeadlineExceeded, err.Error(), reasonContextDeadline)
	default:
		return rpcKVError(nokverrors.KindUnavailable, codes.Internal, err.Error(), reasonInternal)
	}
}

func rpcKVError(kind nokverrors.Kind, code codes.Code, message, reason string) error {
	return nokverrors.RPCStatusError(kind, code, message, map[string]string{
		kvReasonMetadata: reason,
	})
}
