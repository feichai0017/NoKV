// Copyright 2024-2026 The NoKV Authors.
// SPDX-License-Identifier: Apache-2.0

package errors

import (
	"context"
	stderrors "errors"
	"fmt"
	"strings"

	errdetails "google.golang.org/genproto/googleapis/rpc/errdetails"
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

const (
	RPCErrorInfoDomain   = "nokv"
	RPCErrorInfoReason   = "nokv_error"
	RPCErrorKindMetadata = "nokv_kind"
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
	if kind := KindOfMetadataKeyError(err); kind != KindUnknown {
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
	if IsMetadataContention(err) {
		return true
	}
	// RPC boundaries preserve only the stable kind metadata, not the original
	// KeyErrorCarrier. Keep metadata-contention kinds retryable after gRPC
	// translation so clients do not need to parse diagnostic status text.
	switch KindOf(err) {
	case KindCommitTsExpired,
		KindLockConflict,
		KindNotLeader,
		KindRegionRouting,
		KindRetryable,
		KindRouteUnavailable,
		KindStaleEpoch,
		KindUnavailable,
		KindWriteConflict:
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
	if kind, _, ok := RPCErrorInfo(err); ok && kind != KindUnknown {
		return kind
	}
	message := status.Convert(err).Message()
	if kind := kindFromMessage(message); kind != KindUnknown {
		return kind
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

// RPCStatusError returns a gRPC status error with stable machine-readable NoKV
// error metadata. The status message remains diagnostic; clients must branch on
// the ErrorInfo metadata or KindOf rather than matching message text.
func RPCStatusError(kind Kind, code codes.Code, message string, metadata map[string]string) error {
	md := cloneStringMap(metadata)
	md[RPCErrorKindMetadata] = kind.String()
	st := status.New(code, message)
	withDetails, err := st.WithDetails(&errdetails.ErrorInfo{
		Reason:   RPCErrorInfoReason,
		Domain:   RPCErrorInfoDomain,
		Metadata: md,
	})
	if err != nil {
		return st.Err()
	}
	return withDetails.Err()
}

// RPCErrorInfo extracts NoKV's stable gRPC ErrorInfo metadata when present.
func RPCErrorInfo(err error) (Kind, map[string]string, bool) {
	st, ok := status.FromError(err)
	if !ok {
		return KindUnknown, nil, false
	}
	for _, detail := range st.Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if !ok || info.GetDomain() != RPCErrorInfoDomain || info.GetReason() != RPCErrorInfoReason {
			continue
		}
		md := cloneStringMap(info.GetMetadata())
		return ParseKind(md[RPCErrorKindMetadata]), md, true
	}
	return KindUnknown, nil, false
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k := range in {
		out[k] = in[k]
	}
	return out
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

// MetadataKeyIssue describes one structured metadata key failure. It is used at
// fsmeta execution boundaries instead of backend-specific protobuf errors.
type MetadataKeyIssue struct {
	Kind             Kind
	Key              []byte
	Primary          []byte
	StartVersion     uint64
	ConflictVersion  uint64
	CommitVersion    uint64
	MinCommitVersion uint64
	LockVersion      uint64
	LockTTL          uint64
	Message          string
}

// KeyErrorCarrier is the common surface for structured metadata key errors
// carried through metadata runtimes.
type KeyErrorCarrier interface {
	KeyErrors() []MetadataKeyIssue
}

// MetadataKeyError carries structured metadata key errors without flattening
// their semantic class into text. It is the common error boundary for metadata
// commit retries and conflict reporting.
type MetadataKeyError struct {
	Issues []MetadataKeyIssue
}

func NewMetadataKeyError(issues ...MetadataKeyIssue) error {
	filtered := make([]MetadataKeyIssue, 0, len(issues))
	for _, issue := range issues {
		if issue.Kind != KindUnknown {
			filtered = append(filtered, issue)
		}
	}
	if len(filtered) == 0 {
		return nil
	}
	return &MetadataKeyError{Issues: filtered}
}

func (e *MetadataKeyError) Error() string {
	if e == nil {
		return "nokv: metadata key errors"
	}
	return fmt.Sprintf("nokv: metadata key errors: %+v", e.Issues)
}

func (e *MetadataKeyError) KeyErrors() []MetadataKeyIssue {
	if e == nil {
		return nil
	}
	return e.Issues
}

func (e *MetadataKeyError) ErrorKind() Kind {
	if e == nil {
		return KindUnknown
	}
	return KindOfMetadataKeyIssues(e.Issues)
}

func AsMetadataKeyError(err error) (*MetadataKeyError, bool) {
	var target *MetadataKeyError
	if !stderrors.As(err, &target) {
		return nil, false
	}
	return target, true
}

func KindOfMetadataKeyError(err error) Kind {
	var carrier KeyErrorCarrier
	if !stderrors.As(err, &carrier) {
		return KindUnknown
	}
	return KindOfMetadataKeyIssues(carrier.KeyErrors())
}

func KindOfMetadataKeyIssues(issues []MetadataKeyIssue) Kind {
	kind := KindUnknown
	for _, issue := range issues {
		next := KindOfMetadataKeyIssue(issue)
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

func KindOfMetadataKeyIssue(issue MetadataKeyIssue) Kind {
	return issue.Kind
}

func HasKeyErrorKind(err error, kind Kind) bool {
	var carrier KeyErrorCarrier
	if !stderrors.As(err, &carrier) {
		return false
	}
	for _, issue := range carrier.KeyErrors() {
		if KindOfMetadataKeyIssue(issue) == kind {
			return true
		}
	}
	return false
}

// IsMetadataContention reports whether every non-empty key error is retryable by
// obtaining a fresh metadata timestamp. WriteConflict means another command
// committed after this read version; Retryable means the backend could not
// safely finish this attempt, but a higher layer that re-reads and re-plans may
// try again.
// Mixed semantic failures are not contention: retrying them would hide a
// protocol or user-visible error.
func IsMetadataContention(err error) bool {
	var carrier KeyErrorCarrier
	if !stderrors.As(err, &carrier) {
		return false
	}
	seen := false
	for _, issue := range carrier.KeyErrors() {
		switch KindOfMetadataKeyIssue(issue) {
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
