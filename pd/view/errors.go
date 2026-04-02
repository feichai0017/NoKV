package view

import "errors"

var (
	ErrInvalidStoreID       = errors.New("pd/view: invalid store id")
	ErrInvalidRegionID      = errors.New("pd/view: invalid region id")
	ErrRegionHeartbeatStale = errors.New("pd/view: stale region heartbeat epoch")
	ErrRegionRangeOverlap   = errors.New("pd/view: region range overlap")
)
