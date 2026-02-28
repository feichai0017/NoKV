package core

import "errors"

var (
	// ErrInvalidStoreID indicates the heartbeat uses an invalid store id.
	ErrInvalidStoreID = errors.New("pd/core: invalid store id")
	// ErrInvalidRegionID indicates the heartbeat uses an invalid region id.
	ErrInvalidRegionID = errors.New("pd/core: invalid region id")
	// ErrRegionHeartbeatStale indicates a region heartbeat regressed epoch.
	ErrRegionHeartbeatStale = errors.New("pd/core: stale region heartbeat epoch")
	// ErrRegionRangeOverlap indicates the incoming region overlaps another region.
	ErrRegionRangeOverlap = errors.New("pd/core: region range overlap")
	// ErrInvalidBatch indicates a requested allocation batch is invalid.
	ErrInvalidBatch = errors.New("pd/core: invalid batch")
)
