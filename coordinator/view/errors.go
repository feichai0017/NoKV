package view

import nokverrors "github.com/feichai0017/NoKV/errors"

var (
	ErrInvalidStoreID       = nokverrors.New(nokverrors.KindInvalidArgument, "coordinator/view: invalid store id")
	ErrInvalidRegionID      = nokverrors.New(nokverrors.KindInvalidArgument, "coordinator/view: invalid region id")
	ErrRegionHeartbeatStale = nokverrors.New(nokverrors.KindStaleEpoch, "coordinator/view: stale region heartbeat epoch")
	ErrRegionRangeOverlap   = nokverrors.New(nokverrors.KindConflict, "coordinator/view: region range overlap")
)
