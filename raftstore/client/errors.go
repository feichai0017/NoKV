package client

import (
	"errors"
	"fmt"

	kvrpcpb "github.com/feichai0017/NoKV/pb/kv"
)

var (
	// errMissingRegionResolver indicates that the distributed client cannot route requests without a resolver.
	errMissingRegionResolver = errors.New("client: region resolver required")
	// errMissingStoreResolver indicates that the distributed client cannot dial stores without a resolver.
	errMissingStoreResolver = errors.New("client: store resolver required")
	// errStoreIDNotSet indicates that a request tried to use an empty store id.
	errStoreIDNotSet = errors.New("client: store id not set")
	// errStoreUnavailable indicates that Coordinator knows the store but marks its heartbeat stale.
	errStoreUnavailable = errors.New("client: store unavailable")
	// errResolvedRegionIDMissing indicates that routing returned a descriptor without a region id.
	errResolvedRegionIDMissing = errors.New("client: resolved region id missing")
	// errRegionMetaMissing indicates that a routed region snapshot had no metadata.
	errRegionMetaMissing = errors.New("client: region meta missing")
	// errLeaderUnknown indicates that a region has no known leader store.
	errLeaderUnknown = errors.New("client: leader unknown")
	// errInvalidScanLimit indicates that scan was called with limit == 0.
	errInvalidScanLimit = errors.New("client: scan limit must be > 0")
)

func IsMissingRegionResolver(err error) bool { return errors.Is(err, errMissingRegionResolver) }

func IsMissingStoreResolver(err error) bool { return errors.Is(err, errMissingStoreResolver) }

func IsStoreIDNotSet(err error) bool { return errors.Is(err, errStoreIDNotSet) }

func IsStoreUnavailable(err error) bool { return errors.Is(err, errStoreUnavailable) }

func IsResolvedRegionIDMissing(err error) bool { return errors.Is(err, errResolvedRegionIDMissing) }

func IsRegionMetaMissing(err error) bool { return errors.Is(err, errRegionMetaMissing) }

func IsLeaderUnknown(err error) bool { return errors.Is(err, errLeaderUnknown) }

func IsInvalidScanLimit(err error) bool { return errors.Is(err, errInvalidScanLimit) }

// KeyConflictError represents prewrite-time key conflicts surfaced by the raft
// service. Callers can inspect the KeyErrors to resolve locks before retrying.
type KeyConflictError struct {
	Errors []*kvrpcpb.KeyError
}

func (e *KeyConflictError) Error() string {
	return fmt.Sprintf("client: prewrite key errors: %+v", e.Errors)
}

// KeyErrors exposes the raw per-key conflicts without forcing callers to
// depend on this package's concrete error type.
func (e *KeyConflictError) KeyErrors() []*kvrpcpb.KeyError {
	if e == nil {
		return nil
	}
	return e.Errors
}

// AsKeyConflict extracts a KeyConflictError from err.
func AsKeyConflict(err error) (*KeyConflictError, bool) {
	var target *KeyConflictError
	if !errors.As(err, &target) {
		return nil, false
	}
	return target, true
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

// AsRegionNotFound extracts a RegionNotFoundError from err.
func AsRegionNotFound(err error) (*RegionNotFoundError, bool) {
	var target *RegionNotFoundError
	if !errors.As(err, &target) {
		return nil, false
	}
	return target, true
}
