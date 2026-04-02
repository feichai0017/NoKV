package core

import (
	"errors"
	pdview "github.com/feichai0017/NoKV/pd/view"
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
	// ErrInvalidBatch indicates a requested allocation batch is invalid.
	ErrInvalidBatch = errors.New("pd/core: invalid batch")
)
