// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"context"
	stderrors "errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestKindWrapPreservesCauseAndKind(t *testing.T) {
	cause := stderrors.New("disk is unavailable")
	err := Wrap(KindUnavailable, "append wal", cause)

	require.ErrorIs(t, err, cause)
	require.Equal(t, KindUnavailable, KindOf(err))
	require.True(t, IsKind(err, KindUnavailable))
	require.True(t, Retryable(err))
}

func TestTxnContentionRequiresOnlyRetryableTxnConflicts(t *testing.T) {
	err := NewMetadataKeyError(
		MetadataKeyIssue{Kind: KindLockConflict, Key: []byte("a")},
		MetadataKeyIssue{Kind: KindCommitTsExpired, Key: []byte("b"), CommitVersion: 2, MinCommitVersion: 3},
		MetadataKeyIssue{Kind: KindWriteConflict, Key: []byte("c"), ConflictVersion: 5, StartVersion: 3},
		MetadataKeyIssue{Kind: KindRetryable, Message: "transaction start timestamp was rolled back"},
	)

	require.True(t, IsMetadataContention(err))
	require.True(t, Retryable(err))
	require.True(t, HasKeyErrorKind(err, KindLockConflict))
	require.True(t, HasKeyErrorKind(err, KindCommitTsExpired))
	require.True(t, HasKeyErrorKind(err, KindWriteConflict))
	require.True(t, HasKeyErrorKind(err, KindRetryable))
	require.Equal(t, KindConflict, KindOf(err))

	txnErr, ok := AsMetadataKeyError(err)
	require.True(t, ok)
	require.Len(t, txnErr.Issues, 4)
}

func TestTxnContentionRejectsMixedSemanticFailure(t *testing.T) {
	err := NewMetadataKeyError(
		MetadataKeyIssue{Kind: KindCommitTsExpired, Key: []byte("a"), CommitVersion: 2, MinCommitVersion: 3},
		MetadataKeyIssue{Kind: KindAlreadyExists, Key: []byte("b")},
	)

	require.False(t, IsMetadataContention(err))
	require.False(t, Retryable(err))
	require.True(t, HasKeyErrorKind(err, KindAlreadyExists))
	require.Equal(t, KindConflict, KindOf(err))
}

func TestNewMetadataKeyErrorFiltersZeroIssues(t *testing.T) {
	require.NoError(t, NewMetadataKeyError(MetadataKeyIssue{}))
	err := NewMetadataKeyError(MetadataKeyIssue{}, MetadataKeyIssue{Kind: KindRetryable, Message: "temporary"})
	require.Error(t, err)
	require.Equal(t, KindRetryable, KindOf(err))
	require.True(t, Retryable(err))
}

func TestKeyErrorKindPriority(t *testing.T) {
	require.Equal(t, KindCommitTsExpired, KindOfMetadataKeyIssue(MetadataKeyIssue{
		Kind:             KindCommitTsExpired,
		Key:              []byte("a"),
		CommitVersion:    2,
		MinCommitVersion: 3,
	}))
	require.Equal(t, KindRetryable, KindOfMetadataKeyIssue(MetadataKeyIssue{Kind: KindRetryable, Message: "temporary"}))
	require.Equal(t, KindAborted, KindOfMetadataKeyIssue(MetadataKeyIssue{Kind: KindAborted, Message: "abort"}))
}

func TestContextAndGRPCStatusKinds(t *testing.T) {
	require.Equal(t, KindAborted, KindOf(context.Canceled))
	require.Equal(t, KindUnavailable, KindOf(context.DeadlineExceeded))

	require.Equal(t, KindInvalidArgument, KindOf(status.Error(codes.InvalidArgument, "bad request")))
	require.Equal(t, KindNotFound, KindOf(status.Error(codes.NotFound, "missing")))
	require.Equal(t, KindAlreadyExists, KindOf(status.Error(codes.AlreadyExists, "exists")))
	require.Equal(t, KindAborted, KindOf(status.Error(codes.Canceled, "client canceled")))
	require.Equal(t, KindResourceExhausted, KindOf(status.Error(codes.ResourceExhausted, "quota")))
	require.Equal(t, KindUnavailable, KindOf(status.Error(codes.Unavailable, "down")))
	require.Equal(t, KindProtocolViolation, KindOf(status.Error(codes.FailedPrecondition, "invalid protocol state")))
	require.Equal(t, KindAborted, KindOf(status.Error(codes.FailedPrecondition, New(KindAborted, "fsmeta: mount is retired").Error())))
	require.Equal(t, KindResourceExhausted, KindOf(status.Error(codes.ResourceExhausted, New(KindResourceExhausted, "fsmeta: quota exceeded").Error())))

	notLeader := RPCStatusError(KindNotLeader, codes.FailedPrecondition, "diagnostic not leader text", map[string]string{"leader_id": "2"})
	require.Equal(t, KindNotLeader, KindOf(notLeader))
	require.True(t, Retryable(notLeader))

	stale := RPCStatusError(KindStaleEpoch, codes.FailedPrecondition, "diagnostic stale root text", nil)
	require.Equal(t, KindStaleEpoch, KindOf(stale))
	require.True(t, Retryable(stale))

	lockConflict := RPCStatusError(KindLockConflict, codes.Aborted, "diagnostic live lock text", nil)
	require.Equal(t, KindLockConflict, KindOf(lockConflict))
	require.True(t, Retryable(lockConflict))

	writeConflict := RPCStatusError(KindWriteConflict, codes.Aborted, "diagnostic write conflict text", nil)
	require.Equal(t, KindWriteConflict, KindOf(writeConflict))
	require.True(t, Retryable(writeConflict))

	unavailable := RPCStatusError(KindUnavailable, codes.FailedPrecondition, "diagnostic root unavailable text", nil)
	require.Equal(t, KindUnavailable, KindOf(unavailable))
	require.True(t, Retryable(unavailable))

	require.Equal(t, KindProtocolViolation, KindOf(status.Error(codes.FailedPrecondition, "coordinator not leader")))
	require.False(t, Retryable(status.Error(codes.FailedPrecondition, "root lag exceeds bound")))
	require.False(t, Retryable(status.Error(codes.ResourceExhausted, "quota")))
}
