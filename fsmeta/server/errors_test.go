package server

import (
	"fmt"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCCodeForKind(t *testing.T) {
	require.Equal(t, codes.InvalidArgument, rpcCodeForKind(nokverrors.KindInvalidArgument))
	require.Equal(t, codes.NotFound, rpcCodeForKind(nokverrors.KindNotFound))
	require.Equal(t, codes.AlreadyExists, rpcCodeForKind(nokverrors.KindAlreadyExists))
	require.Equal(t, codes.ResourceExhausted, rpcCodeForKind(nokverrors.KindResourceExhausted))
	require.Equal(t, codes.OutOfRange, rpcCodeForKind(nokverrors.KindStaleEpoch))
	require.Equal(t, codes.Aborted, rpcCodeForKind(nokverrors.KindLockConflict))
	require.Equal(t, codes.FailedPrecondition, rpcCodeForKind(nokverrors.KindProtocolViolation))
	require.Equal(t, codes.Unavailable, rpcCodeForKind(nokverrors.KindNotLeader))
	require.Equal(t, codes.DataLoss, rpcCodeForKind(nokverrors.KindCorruption))
	require.Equal(t, codes.Internal, rpcCodeForKind(nokverrors.KindUnknown))
}

func TestRPCErrorPassesThroughStatusErrors(t *testing.T) {
	err := status.Error(codes.NotFound, "already mapped")
	require.Same(t, err, rpcError(err))
}

func TestRPCErrorPreservesOuterStableKindOverWrappedStatus(t *testing.T) {
	err := testKindWrap{
		kind: nokverrors.KindRouteUnavailable,
		err:  status.Error(codes.FailedPrecondition, "required descriptor revision not satisfied"),
	}

	got := rpcError(err)

	require.Equal(t, codes.Unavailable, status.Code(got))
	require.Equal(t, nokverrors.KindRouteUnavailable, nokverrors.KindOf(got))
}

type testKindWrap struct {
	kind nokverrors.Kind
	err  error
}

func (e testKindWrap) Error() string {
	return fmt.Sprintf("wrapped: %v", e.err)
}

func (e testKindWrap) Unwrap() error {
	return e.err
}

func (e testKindWrap) ErrorKind() nokverrors.Kind {
	return e.kind
}
