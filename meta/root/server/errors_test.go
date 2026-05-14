// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"errors"
	"testing"

	nokverrors "github.com/feichai0017/NoKV/errors"
	rootstate "github.com/feichai0017/NoKV/meta/root/state"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRPCErrorMapsStableKinds(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
		kind nokverrors.Kind
	}{
		{
			name: "invalid argument",
			err:  rootstate.ErrInvalidGrant,
			code: codes.InvalidArgument,
			kind: nokverrors.KindInvalidArgument,
		},
		{
			name: "protocol violation",
			err:  rootstate.ErrInheritance,
			code: codes.FailedPrecondition,
			kind: nokverrors.KindProtocolViolation,
		},
		{
			name: "conflict",
			err:  rootstate.ErrFinality,
			code: codes.FailedPrecondition,
			kind: nokverrors.KindConflict,
		},
		{
			name: "unknown",
			err:  errors.New("boom"),
			code: codes.Internal,
			kind: nokverrors.KindUnknown,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := rpcError(tt.err)
			require.Equal(t, tt.code, status.Code(err))
			require.Equal(t, tt.kind, nokverrors.KindOf(err))
		})
	}
}

func TestRPCErrorPreservesStatusAndContext(t *testing.T) {
	statusErr := status.Error(codes.Unavailable, "already mapped")
	require.Same(t, statusErr, rpcError(statusErr))

	require.Equal(t, codes.Canceled, status.Code(rpcError(context.Canceled)))
	require.Equal(t, nokverrors.KindAborted, nokverrors.KindOf(rpcError(context.Canceled)))

	require.Equal(t, codes.DeadlineExceeded, status.Code(rpcError(context.DeadlineExceeded)))
	require.Equal(t, nokverrors.KindUnavailable, nokverrors.KindOf(rpcError(context.DeadlineExceeded)))
}

func TestStatusHelpersExposeKinds(t *testing.T) {
	invalid := statusInvalidArgument("metadata root append requires known event kind")
	require.Equal(t, codes.InvalidArgument, status.Code(invalid))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(invalid))

	precondition := statusFailedPrecondition(rootstate.ErrInvalidGrant)
	require.Equal(t, codes.FailedPrecondition, status.Code(precondition))
	require.Equal(t, nokverrors.KindInvalidArgument, nokverrors.KindOf(precondition))

	notLeader := statusNotLeader(23)
	require.Equal(t, codes.FailedPrecondition, status.Code(notLeader))
	require.Equal(t, nokverrors.KindNotLeader, nokverrors.KindOf(notLeader))
	_, metadata, ok := nokverrors.RPCErrorInfo(notLeader)
	require.True(t, ok)
	require.Equal(t, reasonNotLeader, metadata[metaRootReasonMetadata])
	require.Equal(t, "23", metadata[leaderIDMetadata])
}
