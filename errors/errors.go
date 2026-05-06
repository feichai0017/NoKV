package errors

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Kind is the stable, cross-package error class used by retry, routing,
// observability, and recovery code. Message text is diagnostic only; callers
// should branch on Kind.
type Kind uint8

const (
	KindUnknown Kind = iota
	KindInvalidArgument
	KindNotFound
	KindAlreadyExists
	KindConflict
	KindWriteConflict
	KindLockConflict
	KindCommitTsExpired
	KindRetryable
	KindResourceExhausted
	KindUnavailable
	KindRouteUnavailable
	KindRegionRouting
	KindStaleEpoch
	KindNotLeader
	KindRetryExhausted
	KindProtocolViolation
	KindCorruption
	KindAborted
)

func (k Kind) String() string {
	switch k {
	case KindInvalidArgument:
		return "invalid_argument"
	case KindNotFound:
		return "not_found"
	case KindAlreadyExists:
		return "already_exists"
	case KindConflict:
		return "conflict"
	case KindWriteConflict:
		return "write_conflict"
	case KindLockConflict:
		return "lock_conflict"
	case KindCommitTsExpired:
		return "commit_ts_expired"
	case KindRetryable:
		return "retryable"
	case KindResourceExhausted:
		return "resource_exhausted"
	case KindUnavailable:
		return "unavailable"
	case KindRouteUnavailable:
		return "route_unavailable"
	case KindRegionRouting:
		return "region_routing"
	case KindStaleEpoch:
		return "stale_epoch"
	case KindNotLeader:
		return "not_leader"
	case KindRetryExhausted:
		return "retry_exhausted"
	case KindProtocolViolation:
		return "protocol_violation"
	case KindCorruption:
		return "corruption"
	case KindAborted:
		return "aborted"
	default:
		return "unknown"
	}
}

func ParseKind(s string) Kind {
	switch s {
	case "invalid_argument":
		return KindInvalidArgument
	case "not_found":
		return KindNotFound
	case "already_exists":
		return KindAlreadyExists
	case "conflict":
		return KindConflict
	case "write_conflict":
		return KindWriteConflict
	case "lock_conflict":
		return KindLockConflict
	case "commit_ts_expired":
		return KindCommitTsExpired
	case "retryable":
		return KindRetryable
	case "resource_exhausted":
		return KindResourceExhausted
	case "unavailable":
		return KindUnavailable
	case "route_unavailable":
		return KindRouteUnavailable
	case "region_routing":
		return KindRegionRouting
	case "stale_epoch":
		return KindStaleEpoch
	case "not_leader":
		return KindNotLeader
	case "retry_exhausted":
		return KindRetryExhausted
	case "protocol_violation":
		return KindProtocolViolation
	case "corruption":
		return KindCorruption
	case "aborted":
		return KindAborted
	default:
		return KindUnknown
	}
}

// KindCarrier is implemented by errors with a stable classification.
type KindCarrier interface {
	ErrorKind() Kind
}

// Error attaches one stable Kind to an underlying error.
type Error struct {
	Kind Kind
	Op   string
	Err  error
}

func New(kind Kind, msg string) error {
	return &Error{Kind: kind, Err: stderrors.New(msg)}
}

func Wrap(kind Kind, op string, err error) error {
	if err == nil {
		return nil
	}
	return &Error{Kind: kind, Op: op, Err: err}
}

func Mark(kind Kind, err error) error {
	return Wrap(kind, "", err)
}

func (e *Error) Error() string {
	if e == nil {
		return "nokv: unknown error"
	}
	if e.Op == "" {
		return fmt.Sprintf("nokv: %s: %v", e.Kind, e.Err)
	}
	return fmt.Sprintf("nokv: %s: %s: %v", e.Kind, e.Op, e.Err)
}

func (e *Error) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func (e *Error) ErrorKind() Kind {
	if e == nil {
		return KindUnknown
	}
	return e.Kind
}

func KindOf(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	var carrier KindCarrier
	if stderrors.As(err, &carrier) {
		if kind := carrier.ErrorKind(); kind != KindUnknown {
			return kind
		}
	}
	if kind := KindOfContext(err); kind != KindUnknown {
		return kind
	}
	if kind := KindOfGRPCStatus(err); kind != KindUnknown {
		return kind
	}
	if kind := KindOfTxnKeyError(err); kind != KindUnknown {
		return kind
	}
	return KindUnknown
}

func IsKind(err error, kind Kind) bool {
	if kind == KindUnknown {
		return err == nil || KindOf(err) == KindUnknown
	}
	return KindOf(err) == kind || HasKeyErrorKind(err, kind)
}

func Retryable(err error) bool {
	if err == nil {
		return false
	}
	if IsTxnContention(err) {
		return true
	}
	switch KindOf(err) {
	case KindRetryable, KindUnavailable, KindRouteUnavailable, KindRegionRouting, KindStaleEpoch, KindNotLeader:
		return true
	default:
		return false
	}
}

func KindOfContext(err error) Kind {
	switch {
	case err == nil:
		return KindUnknown
	case stderrors.Is(err, context.Canceled):
		return KindAborted
	case stderrors.Is(err, context.DeadlineExceeded):
		return KindUnavailable
	default:
		return KindUnknown
	}
}

func KindOfGRPCStatus(err error) Kind {
	if err == nil {
		return KindUnknown
	}
	code := status.Code(err)
	if code == codes.OK || code == codes.Unknown {
		return KindUnknown
	}
	message := status.Convert(err).Message()
	if kind := kindFromMessage(message); kind != KindUnknown {
		return kind
	}
	if strings.Contains(message, "not leader") ||
		strings.Contains(message, "grant not held") ||
		strings.Contains(message, "lease not held") {
		return KindNotLeader
	}
	if strings.Contains(message, "root unavailable") {
		return KindUnavailable
	}
	if strings.Contains(message, "root lag") ||
		strings.Contains(message, "required rooted token") ||
		strings.Contains(message, "required descriptor") {
		return KindStaleEpoch
	}
	switch code {
	case codes.InvalidArgument, codes.OutOfRange:
		return KindInvalidArgument
	case codes.NotFound:
		return KindNotFound
	case codes.AlreadyExists:
		return KindAlreadyExists
	case codes.Canceled, codes.Aborted:
		return KindAborted
	case codes.ResourceExhausted:
		return KindResourceExhausted
	case codes.Unavailable, codes.DeadlineExceeded:
		return KindUnavailable
	case codes.FailedPrecondition:
		return KindProtocolViolation
	case codes.DataLoss:
		return KindCorruption
	default:
		return KindUnknown
	}
}

func kindFromMessage(message string) Kind {
	const prefix = "nokv: "
	if !strings.HasPrefix(message, prefix) {
		return KindUnknown
	}
	rest := strings.TrimPrefix(message, prefix)
	token, _, ok := strings.Cut(rest, ":")
	if !ok {
		return KindUnknown
	}
	return ParseKind(token)
}

// KeyErrorCarrier is the common surface for Percolator key errors carried
// through raftstore and higher-level runtimes.
type KeyErrorCarrier interface {
	KeyErrors() []*kvrpcpb.KeyError
}

// TxnKeyError carries Percolator key errors without flattening their semantic
// class into text. It is the common error boundary for transaction retries and
// conflict reporting.
type TxnKeyError struct {
	Errors []*kvrpcpb.KeyError
}

func NewTxnKeyError(errs ...*kvrpcpb.KeyError) error {
	filtered := make([]*kvrpcpb.KeyError, 0, len(errs))
	for _, err := range errs {
		if err != nil {
			filtered = append(filtered, err)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &TxnKeyError{Errors: filtered}
}

func (e *TxnKeyError) Error() string {
	if e == nil {
		return "nokv: transaction key errors"
	}
	return fmt.Sprintf("nokv: transaction key errors: %+v", e.Errors)
}

func (e *TxnKeyError) KeyErrors() []*kvrpcpb.KeyError {
	if e == nil {
		return nil
	}
	return e.Errors
}

func (e *TxnKeyError) ErrorKind() Kind {
	if e == nil {
		return KindUnknown
	}
	return KindOfTxnKeyErrors(e.Errors)
}

func AsTxnKeyError(err error) (*TxnKeyError, bool) {
	var target *TxnKeyError
	if !stderrors.As(err, &target) {
		return nil, false
	}
	return target, true
}

func KindOfTxnKeyError(err error) Kind {
	var carrier KeyErrorCarrier
	if !stderrors.As(err, &carrier) {
		return KindUnknown
	}
	return KindOfTxnKeyErrors(carrier.KeyErrors())
}

func KindOfTxnKeyErrors(errs []*kvrpcpb.KeyError) Kind {
	kind := KindUnknown
	for _, keyErr := range errs {
		next := KindOfKeyError(keyErr)
		if next == KindUnknown {
			continue
		}
		if kind == KindUnknown {
			kind = next
			continue
		}
		if kind != next {
			return KindConflict
		}
	}
	return kind
}

func KindOfKeyError(err *kvrpcpb.KeyError) Kind {
	switch {
	case err == nil:
		return KindUnknown
	case err.GetCommitTsExpired() != nil:
		return KindCommitTsExpired
	case err.GetLocked() != nil:
		return KindLockConflict
	case err.GetWriteConflict() != nil:
		return KindWriteConflict
	case err.GetAlreadyExists() != nil:
		return KindAlreadyExists
	case err.GetRetryable() != "":
		return KindRetryable
	case err.GetAbort() != "":
		return KindAborted
	default:
		return KindUnknown
	}
}

func HasKeyErrorKind(err error, kind Kind) bool {
	var carrier KeyErrorCarrier
	if !stderrors.As(err, &carrier) {
		return false
	}
	for _, keyErr := range carrier.KeyErrors() {
		if KindOfKeyError(keyErr) == kind {
			return true
		}
	}
	return false
}

// IsTxnContention reports whether every non-empty key error is retryable by
// obtaining a fresh transaction timestamp. WriteConflict means another
// transaction committed after this start_ts; Retryable means the transaction
// protocol could not safely finish this start_ts, but a higher layer that
// re-reads and re-plans may try again.
// Mixed semantic failures are not contention: retrying them would hide a
// protocol or user-visible error.
func IsTxnContention(err error) bool {
	var carrier KeyErrorCarrier
	if !stderrors.As(err, &carrier) {
		return false
	}
	seen := false
	for _, keyErr := range carrier.KeyErrors() {
		switch KindOfKeyError(keyErr) {
		case KindUnknown:
			continue
		case KindCommitTsExpired, KindLockConflict, KindWriteConflict, KindRetryable:
			seen = true
		default:
			return false
		}
	}
	return seen
}
