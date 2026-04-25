package catalog

import (
	"errors"

	pdview "github.com/feichai0017/NoKV/coordinator/view"
)

var (
	// ErrInvalidStoreID indicates the heartbeat uses an invalid store id.
	ErrInvalidStoreID = pdview.ErrInvalidStoreID
	// ErrInvalidRegionID indicates the heartbeat uses an invalid region id.
	ErrInvalidRegionID = pdview.ErrInvalidRegionID
	// ErrRegionHeartbeatStale indicates a region heartbeat regressed epoch.
	ErrRegionHeartbeatStale = pdview.ErrRegionHeartbeatStale
	// ErrRegionRangeOverlap indicates the incoming region overlaps another region.
	ErrRegionRangeOverlap = pdview.ErrRegionRangeOverlap
	// ErrStoreNotJoined indicates a store heartbeat arrived before rooted membership joined it.
	ErrStoreNotJoined = errors.New("coordinator/catalog: store is not joined in rooted membership")
	// ErrStoreRetired indicates a store heartbeat arrived after rooted membership retired it.
	ErrStoreRetired = errors.New("coordinator/catalog: store is retired in rooted membership")
	// ErrInvalidMountID indicates a rooted mount event uses an invalid mount id.
	ErrInvalidMountID = errors.New("coordinator/catalog: invalid mount id")
	// ErrMountNotFound indicates a mount-scoped operation referenced an unknown rooted mount.
	ErrMountNotFound = errors.New("coordinator/catalog: mount is not registered")
	// ErrMountRetired indicates a mount-scoped operation referenced a retired rooted mount.
	ErrMountRetired = errors.New("coordinator/catalog: mount is retired")
	// ErrMountConflict indicates a mount registration conflicts with existing rooted truth.
	ErrMountConflict = errors.New("coordinator/catalog: mount registration conflicts with rooted truth")
	// ErrSubtreeAuthorityNotFound indicates a handoff references an undeclared subtree authority.
	ErrSubtreeAuthorityNotFound = errors.New("coordinator/catalog: subtree authority is not declared")
	// ErrSubtreeAuthorityConflict indicates a subtree authority event conflicts with rooted truth.
	ErrSubtreeAuthorityConflict = errors.New("coordinator/catalog: subtree authority conflicts with rooted truth")
	// ErrSubtreeAuthorityHandoff indicates a subtree handoff violates Eunomia coverage or finality.
	ErrSubtreeAuthorityHandoff = errors.New("coordinator/catalog: invalid subtree authority handoff")
	// ErrQuotaFenceNotFound indicates a quota-scoped operation referenced an unknown fence.
	ErrQuotaFenceNotFound = errors.New("coordinator/catalog: quota fence is not published")
	// ErrQuotaFenceConflict indicates a quota update regressed or conflicted with rooted truth.
	ErrQuotaFenceConflict = errors.New("coordinator/catalog: quota fence conflicts with rooted truth")
)
