// Package region owns the store-local region catalog: the in-memory
// metaByID + range-by-start-key map, the per-region peer registry, the
// persistence handoff to localmeta, and the per-region traffic counters
// the scheduler reports as RegionStats.
package region

import (
	"errors"
	"fmt"
)

// Sentinel errors returned by Manager.
var (
	ErrNil    = errors.New("raftstore/region: manager is nil")
	ErrZeroID = errors.New("raftstore/region: region id is zero")
)

// ErrNotFound formats a not-found error for the given region id.
func ErrNotFound(id uint64) error {
	return fmt.Errorf("raftstore/region: region %d not found", id)
}
