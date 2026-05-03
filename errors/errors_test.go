package errors

import (
	stderrors "errors"
	"testing"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"github.com/stretchr/testify/require"
)

func TestKindWrapPreservesCauseAndKind(t *testing.T) {
	cause := stderrors.New("disk is unavailable")
	err := Wrap(KindUnavailable, "append wal", cause)

	require.ErrorIs(t, err, cause)
	require.Equal(t, KindUnavailable, KindOf(err))
	require.True(t, IsKind(err, KindUnavailable))
	require.True(t, Retryable(err))
}

func TestTxnContentionRequiresOnlyLockOrCommitTsExpired(t *testing.T) {
	err := fakeKeyErrors{errs: []*kvrpcpb.KeyError{
		{Locked: &kvrpcpb.Locked{Key: []byte("a")}},
		{CommitTsExpired: &kvrpcpb.CommitTsExpired{Key: []byte("b"), CommitTs: 2, MinCommitTs: 3}},
	}}

	require.True(t, IsTxnContention(err))
	require.True(t, Retryable(err))
	require.True(t, HasKeyErrorKind(err, KindLockConflict))
	require.True(t, HasKeyErrorKind(err, KindCommitTsExpired))
	require.Equal(t, KindConflict, KindOf(err))
}

func TestTxnContentionRejectsMixedSemanticFailure(t *testing.T) {
	err := fakeKeyErrors{errs: []*kvrpcpb.KeyError{
		{CommitTsExpired: &kvrpcpb.CommitTsExpired{Key: []byte("a"), CommitTs: 2, MinCommitTs: 3}},
		{AlreadyExists: &kvrpcpb.KeyAlreadyExists{Key: []byte("b")}},
	}}

	require.False(t, IsTxnContention(err))
	require.False(t, Retryable(err))
	require.True(t, HasKeyErrorKind(err, KindAlreadyExists))
	require.Equal(t, KindConflict, KindOf(err))
}

func TestKeyErrorKindPriority(t *testing.T) {
	require.Equal(t, KindCommitTsExpired, KindOfKeyError(&kvrpcpb.KeyError{
		Locked:          &kvrpcpb.Locked{Key: []byte("a")},
		CommitTsExpired: &kvrpcpb.CommitTsExpired{Key: []byte("a"), CommitTs: 2, MinCommitTs: 3},
	}))
	require.Equal(t, KindRetryable, KindOfKeyError(&kvrpcpb.KeyError{Retryable: "temporary"}))
	require.Equal(t, KindAborted, KindOfKeyError(&kvrpcpb.KeyError{Abort: "abort"}))
}

type fakeKeyErrors struct {
	errs []*kvrpcpb.KeyError
}

func (e fakeKeyErrors) Error() string {
	return "fake key errors"
}

func (e fakeKeyErrors) KeyErrors() []*kvrpcpb.KeyError {
	return e.errs
}
