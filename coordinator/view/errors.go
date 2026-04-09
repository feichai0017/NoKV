package view

import "errors"

var (
	ErrInvalidStoreID       = errors.New("coordinator/view: invalid store id")
	ErrInvalidRegionID      = errors.New("coordinator/view: invalid region id")
	ErrRegionHeartbeatStale = errors.New("coordinator/view: stale region heartbeat epoch")
	ErrRegionRangeOverlap   = errors.New("coordinator/view: region range overlap")
)
