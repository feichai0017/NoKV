package capsule

import "math"

// ReplayVersionRange is the Raft-replicated MVCC version segment assigned to
// one sealed Capsule. Each operation consumes one version in replay order.
type ReplayVersionRange struct {
	First uint64
	Count uint64
}

// Empty reports whether a seal has not been assigned replay versions yet.
func (r ReplayVersionRange) Empty() bool {
	return r.First == 0 && r.Count == 0
}

// Validate rejects empty, sentinel, and overflowing version ranges.
func (r ReplayVersionRange) Validate() error {
	if r.First == 0 || r.Count == 0 || r.First == math.MaxUint64 {
		return ErrReplayVersionRequired
	}
	if r.Count-1 >= math.MaxUint64-r.First {
		return ErrReplayVersionRequired
	}
	return nil
}

// ValidateForOperationCount requires the reserved range to exactly cover the
// operations in a replay plan.
func (r ReplayVersionRange) ValidateForOperationCount(count uint64) error {
	if err := r.Validate(); err != nil {
		return err
	}
	if r.Count != count {
		return ErrReplayVersionRequired
	}
	return nil
}

// VersionAt returns the version for the operation at offset in replay order.
func (r ReplayVersionRange) VersionAt(offset uint64) (uint64, error) {
	if err := r.Validate(); err != nil {
		return 0, err
	}
	if offset >= r.Count {
		return 0, ErrReplayVersionRequired
	}
	return r.First + offset, nil
}
