package client

import (
	"errors"
	"fmt"

	nokverrors "github.com/feichai0017/NoKV/errors"
	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	// errMissingRegionResolver indicates that the distributed client cannot route requests without a resolver.
	errMissingRegionResolver = nokverrors.New(nokverrors.KindInvalidArgument, "client: region resolver required")
	// errMissingStoreResolver indicates that the distributed client cannot dial stores without a resolver.
	errMissingStoreResolver = nokverrors.New(nokverrors.KindInvalidArgument, "client: store resolver required")
	// errStoreIDNotSet indicates that a request tried to use an empty store id.
	errStoreIDNotSet = nokverrors.New(nokverrors.KindInvalidArgument, "client: store id not set")
	// errStoreUnavailable indicates that Coordinator knows the store but marks its heartbeat stale.
	errStoreUnavailable = nokverrors.New(nokverrors.KindUnavailable, "client: store unavailable")
	// errResolvedRegionIDMissing indicates that routing returned a descriptor without a region id.
	errResolvedRegionIDMissing = nokverrors.New(nokverrors.KindProtocolViolation, "client: resolved region id missing")
	// errRegionMetaMissing indicates that a routed region snapshot had no metadata.
	errRegionMetaMissing = nokverrors.New(nokverrors.KindProtocolViolation, "client: region meta missing")
	// errLeaderUnknown indicates that a region has no known leader store.
	errLeaderUnknown = nokverrors.New(nokverrors.KindRouteUnavailable, "client: leader unknown")
	// errInvalidScanLimit indicates that scan was called with limit == 0.
	errInvalidScanLimit = nokverrors.New(nokverrors.KindInvalidArgument, "client: scan limit must be > 0")
	// errReadLockStillLive indicates that a read lock is valid and should be retried within the caller budget.
	errReadLockStillLive = nokverrors.New(nokverrors.KindRetryable, "client: read lock still live")
)

func IsMissingRegionResolver(err error) bool { return errors.Is(err, errMissingRegionResolver) }

func IsMissingStoreResolver(err error) bool { return errors.Is(err, errMissingStoreResolver) }

func IsStoreIDNotSet(err error) bool { return errors.Is(err, errStoreIDNotSet) }

func IsStoreUnavailable(err error) bool { return errors.Is(err, errStoreUnavailable) }

func IsResolvedRegionIDMissing(err error) bool { return errors.Is(err, errResolvedRegionIDMissing) }

func IsRegionMetaMissing(err error) bool { return errors.Is(err, errRegionMetaMissing) }

func IsLeaderUnknown(err error) bool { return errors.Is(err, errLeaderUnknown) }

func IsInvalidScanLimit(err error) bool { return errors.Is(err, errInvalidScanLimit) }

// RetryExhaustedError records a stable retry-budget failure for one client operation.
type RetryExhaustedError struct {
	Operation string
	RegionID  uint64
	Key       []byte
}

func (e *RetryExhaustedError) Error() string {
	if e == nil {
		return "client: retries exhausted"
	}
	switch {
	case e.RegionID != 0:
		return fmt.Sprintf("client: %s retries exhausted for region %d", e.Operation, e.RegionID)
	case len(e.Key) > 0:
		return fmt.Sprintf("client: %s retries exhausted for key %q", e.Operation, e.Key)
	default:
		return fmt.Sprintf("client: %s retries exhausted", e.Operation)
	}
}

func IsRetryExhausted(err error) bool {
	var target *RetryExhaustedError
	return errors.As(err, &target)
}

func (e *RetryExhaustedError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindRetryExhausted
}

// ProtocolError records local transaction/client contract violations.
type ProtocolError struct {
	Operation string
	Detail    string
}

func (e *ProtocolError) Error() string {
	if e == nil {
		return "client: protocol error"
	}
	if e.Operation == "" {
		return fmt.Sprintf("client: protocol error: %s", e.Detail)
	}
	return fmt.Sprintf("client: %s protocol error: %s", e.Operation, e.Detail)
}

func IsProtocolError(err error) bool {
	var target *ProtocolError
	return errors.As(err, &target)
}

func (e *ProtocolError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindProtocolViolation
}

// RegionRoutingError records stable region-cache and RegionError failures.
type RegionRoutingError struct {
	Operation string
	RegionID  uint64
	Key       []byte
	Detail    string
	Err       error
}

func (e *RegionRoutingError) Error() string {
	if e == nil {
		return "client: region routing error"
	}
	msg := "client: region routing error"
	if e.Operation != "" {
		msg += " during " + e.Operation
	}
	if e.RegionID != 0 {
		msg += fmt.Sprintf(" for region %d", e.RegionID)
	}
	if len(e.Key) > 0 {
		msg += fmt.Sprintf(" key %q", e.Key)
	}
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	if e.Err != nil {
		msg += ": " + e.Err.Error()
	}
	return msg
}

func (e *RegionRoutingError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsRegionRoutingError(err error) bool {
	var target *RegionRoutingError
	return errors.As(err, &target)
}

func (e *RegionRoutingError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindRegionRouting
}

func txnKeyError(errs ...*kvrpcpb.KeyError) error {
	return nokverrors.NewTxnKeyError(errs...)
}

// RouteUnavailableError indicates that the client could not resolve a route
// for the requested key because the external resolver was unavailable or the
// lookup timed out. Callers may retry once control-plane connectivity recovers.
type RouteUnavailableError struct {
	Key []byte
	Err error
}

func (e *RouteUnavailableError) Error() string {
	if e == nil {
		return "client: route unavailable"
	}
	return fmt.Sprintf("client: route unavailable for key %q: %v", e.Key, e.Err)
}

func (e *RouteUnavailableError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func IsRouteUnavailable(err error) bool {
	var target *RouteUnavailableError
	return errors.As(err, &target)
}

func (e *RouteUnavailableError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindRouteUnavailable
}

// AsRouteUnavailable extracts a RouteUnavailableError from err.
func AsRouteUnavailable(err error) (*RouteUnavailableError, bool) {
	var target *RouteUnavailableError
	if !errors.As(err, &target) {
		return nil, false
	}
	return target, true
}

// RegionNotFoundError indicates that no region metadata currently covers the
// requested key.
type RegionNotFoundError struct {
	Key []byte
}

func (e *RegionNotFoundError) Error() string {
	if e == nil {
		return "client: region not found"
	}
	return fmt.Sprintf("client: region not found for key %q", e.Key)
}

func IsRegionNotFound(err error) bool {
	var target *RegionNotFoundError
	return errors.As(err, &target)
}

func (e *RegionNotFoundError) ErrorKind() nokverrors.Kind {
	return nokverrors.KindNotFound
}

// AsRegionNotFound extracts a RegionNotFoundError from err.
func AsRegionNotFound(err error) (*RegionNotFoundError, bool) {
	var target *RegionNotFoundError
	if !errors.As(err, &target) {
		return nil, false
	}
	return target, true
}
